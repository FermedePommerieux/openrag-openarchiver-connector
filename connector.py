"""Connecteur d'inventaire et d'ingestion OpenArchiver vers OpenRAG.

Le connecteur orchestre quatre responsabilités, sans modifier OpenRAG :

1. inventorier les sources, dossiers, mails et pièces jointes d'OpenArchiver ;
2. conserver les sélections et l'état de reprise dans une base SQLite locale ;
3. déléguer l'identité, les rôles et les permissions de l'interface à OpenRAG ;
4. soumettre les fichiers originaux à l'API d'ingestion OpenRAG ;
5. valider et réconcilier le résultat avec les chunks durables d'OpenRAG.

Le chemin normal en production est l'upload multipart (``OPENRAG_INGEST_MODE=api``).
Chaque worker télécharge un fichier dans le volume partagé, calcule son SHA-256,
le soumet, attend la fin de la tâche OpenRAG, puis vérifie que ``/v2/files`` expose
au moins un chunk portant l'identifiant dérivé de ce SHA-256. Le statut local ne
passe à ``validated`` qu'après cette dernière preuve.

Ce module reste volontairement autonome et est publié dans sa propre image.
Le guide d'architecture, les états et les procédures d'exploitation sont dans
le fichier ``README.md`` situé à côté de ce module.
"""

from __future__ import annotations

import base64
import hashlib
import html
import http.client
import http.cookies
import json
import logging
import mimetypes
import os
import re
import signal
import sqlite3
import tempfile
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import dataclass
from email import policy
from email.parser import BytesParser
from email.utils import getaddresses
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from html.parser import HTMLParser
from pathlib import Path
from typing import Callable, Iterable, Iterator, Mapping, Sequence


LOG = logging.getLogger("openarchiver-openrag-connector")
CHUNK_SIZE = 1024 * 1024
ACTIVE_STATUSES = ("downloading", "ingesting")
QUEUE_STATUSES = ("queued", "failed", "lost")
ALL_STATUSES = (
    "discovered",
    "queued",
    "downloading",
    "ingesting",
    "validated",
    "failed",
    "lost",
    "non_indexable",
    "missing",
    "unavailable",
)
DEFAULT_EXTENSIONS = ".asc,.asciidoc,.adoc,.csv,.docx,.htm,.html,.md,.pdf,.txt,.xlsx"
SAFE_EXTENSION = re.compile(r"^\.[a-z0-9]{1,10}$")
STOP = threading.Event()
WAKE = threading.Event()
RECONCILE_WAKE = threading.Event()
POOL_RECONFIGURE = threading.Event()
SCHEMA_VERSION = 5
SCHEMA_LOCK = threading.Lock()
RECONCILE_BATCH_SIZE = 100
MAIL_RATE_POLL_SECONDS = 1
OPENRAG_TASK_POLL_SECONDS = 0.25
API_KEY_DISPLAY_PREFIX_LENGTH = 12
RUNTIME_OPENRAG_URL_KEY = "runtime_openrag_base_url"
RUNTIME_CONNECTOR_URL_KEY = "runtime_connector_public_url"
RUNTIME_POOL_SIZE_KEY = "runtime_ingestion_pool_size"
MIN_INGESTION_POOL_SIZE = 1
MAX_INGESTION_POOL_SIZE = 6
CONFIG_LOCK = threading.RLock()


# ---------------------------------------------------------------------------
# Configuration et types du domaine
# ---------------------------------------------------------------------------


class ConnectorError(RuntimeError):
    """Erreur contrôlée dont le message ne contient ni secret ni contenu."""


class HTTPStatusError(ConnectorError):
    def __init__(self, status: int, operation: str) -> None:
        self.status = status
        super().__init__(f"{operation}: HTTP {status}")


class LostTaskError(ConnectorError):
    """La tâche soumise n'existe plus dans le registre OpenRAG."""


class IncompleteScanError(ConnectorError):
    pass


class FileTooLargeError(ConnectorError):
    pass


@dataclass(frozen=True)
class ReconciliationResult:
    checked: int
    restored: int
    lost: int


@dataclass(frozen=True)
class ConnectorPrincipal:
    """Identité et autorisations relues depuis OpenRAG pour une requête UI."""

    user_id: str
    email: str = ""
    name: str = ""
    picture: str = ""
    provider: str = ""
    roles: frozenset[str] = frozenset()
    permissions: frozenset[str] = frozenset()
    authenticated: bool = False
    no_auth_mode: bool = False
    rbac_enforced: bool = False

    def can(self, permission: str) -> bool:
        """Reproduit le coupe-circuit RBAC d'OpenRAG.

        En mode sans authentification ou lorsque le RBAC OpenRAG est désactivé,
        OpenRAG autorise toutes les actions. Sinon sa liste de permissions est
        la seule source de décision.
        """
        return (
            self.no_auth_mode
            or not self.rbac_enforced
            or permission in self.permissions
        )


@dataclass
class Config:
    """Configuration chargée au démarrage puis partiellement éditable.

    Les variables du Deployment amorcent le service. Les deux URL utilisées
    par l'interface peuvent ensuite être remplacées dans SQLite ; seuls ces
    deux attributs sont modifiés à chaud, sous ``CONFIG_LOCK``.
    """

    openarchiver_base_url: str
    openarchiver_api_key_file: Path
    openrag_base_url: str
    openrag_ingest_path: str
    openrag_task_path: str
    openrag_api_key_file: Path
    state_db: Path
    connector_public_url: str = ""
    openrag_auth_mode: str = "disabled"
    openrag_auth_cookie_name: str = "auth_token"
    openrag_ingest_directory: Path = Path("/shared/openrag-documents/openarchiver")
    openrag_ingest_mode: str = "auto"
    openrag_upload_path: str = "/v1/documents/ingest"
    scan_interval_seconds: int = 3600
    task_timeout_seconds: int = 3600
    max_file_bytes: int = 104_857_600
    max_auto_retries: int = 3
    retry_base_seconds: int = 300
    retry_max_seconds: int = 3600
    supported_extensions: frozenset[str] = frozenset(DEFAULT_EXTENSIONS.split(","))
    openarchiver_requests_per_minute: int = 90
    ingestion_concurrency: int = 3
    ingestion_concurrency_max: int = 6
    page_limit: int = 250
    openarchiver_link_template: str = ""
    openarchiver_source_url_template: str = ""
    request_timeout_seconds: int = 30
    cycle_retry_seconds: int = 60
    http_host: str = "0.0.0.0"
    http_port: int = 8080

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "Config":
        values = os.environ if env is None else env
        extensions = frozenset(
            _normalise_extension(item)
            for item in values.get("SUPPORTED_EXTENSIONS", DEFAULT_EXTENSIONS).split(
                ","
            )
            if item.strip()
        )
        concurrency_max = min(
            MAX_INGESTION_POOL_SIZE,
            max(
                MIN_INGESTION_POOL_SIZE,
                int(values.get("INGESTION_CONCURRENCY_MAX", "6")),
            ),
        )
        concurrency_value = values.get("INGESTION_CONCURRENCY", "3").strip().lower()
        # ``auto`` était l'ancienne valeur de déploiement. Elle migre vers le
        # nouveau défaut manuel afin qu'une mise à jour ne bloque pas le service.
        ingestion_concurrency = (
            3 if concurrency_value == "auto" else int(concurrency_value)
        )
        ingestion_concurrency = min(
            concurrency_max,
            max(MIN_INGESTION_POOL_SIZE, ingestion_concurrency),
        )
        config = cls(
            openarchiver_base_url=values.get(
                "OPENARCHIVER_BASE_URL",
                "http://openarchiver-api.openarchiver.svc.cluster.local:4000/v1",
            ).rstrip("/"),
            openarchiver_api_key_file=Path(
                values.get(
                    "OPENARCHIVER_API_KEY_FILE",
                    "/var/run/secrets/openarchiver/api-key",
                )
            ),
            openrag_base_url=values.get(
                "OPENRAG_BASE_URL", "http://openrag-backend:8000"
            ).rstrip("/"),
            openrag_ingest_path=values.get(
                "OPENRAG_INGEST_PATH", "/v1/documents/ingest-path"
            ),
            openrag_ingest_directory=Path(
                values.get(
                    "OPENRAG_INGEST_DIRECTORY",
                    "/shared/openrag-documents/openarchiver",
                )
            ),
            openrag_ingest_mode=values.get("OPENRAG_INGEST_MODE", "auto")
            .strip()
            .lower(),
            openrag_upload_path=values.get(
                "OPENRAG_UPLOAD_PATH", "/v1/documents/ingest"
            ),
            openrag_task_path=values.get(
                "OPENRAG_TASK_PATH", "/v1/tasks/{task_id}/enhanced"
            ),
            openrag_api_key_file=Path(
                values.get("OPENRAG_API_KEY_FILE", "/var/run/secrets/openrag/api-key")
            ),
            state_db=Path(values.get("STATE_DB", "/state/connector.sqlite3")),
            connector_public_url=values.get("CONNECTOR_PUBLIC_URL", "").rstrip("/"),
            openrag_auth_mode=values.get("OPENRAG_AUTH_MODE", "disabled")
            .strip()
            .lower(),
            openrag_auth_cookie_name=values.get(
                "OPENRAG_AUTH_COOKIE_NAME", "auth_token"
            ).strip(),
            scan_interval_seconds=max(
                60, int(values.get("SCAN_INTERVAL_SECONDS", "3600"))
            ),
            task_timeout_seconds=max(
                1, int(values.get("TASK_TIMEOUT_SECONDS", "3600"))
            ),
            max_file_bytes=max(1, int(values.get("MAX_FILE_BYTES", "104857600"))),
            max_auto_retries=max(1, int(values.get("MAX_AUTO_RETRIES", "3"))),
            retry_base_seconds=max(1, int(values.get("RETRY_BASE_SECONDS", "300"))),
            retry_max_seconds=max(1, int(values.get("RETRY_MAX_SECONDS", "3600"))),
            supported_extensions=extensions,
            openarchiver_requests_per_minute=max(
                1, int(values.get("OPENARCHIVER_REQUESTS_PER_MINUTE", "90"))
            ),
            ingestion_concurrency=ingestion_concurrency,
            ingestion_concurrency_max=concurrency_max,
            page_limit=max(1, int(values.get("OPENARCHIVER_PAGE_LIMIT", "250"))),
            openarchiver_link_template=values.get("OPENARCHIVER_LINK_TEMPLATE", ""),
            openarchiver_source_url_template=values.get(
                "OPENARCHIVER_SOURCE_URL_TEMPLATE",
                values.get("OPENARCHIVER_LINK_TEMPLATE", ""),
            ),
            request_timeout_seconds=max(
                1, int(values.get("REQUEST_TIMEOUT_SECONDS", "30"))
            ),
            cycle_retry_seconds=max(5, int(values.get("CYCLE_RETRY_SECONDS", "60"))),
            http_host=values.get("HTTP_HOST", "0.0.0.0"),
            http_port=min(65_535, max(1, int(values.get("HTTP_PORT", "8080")))),
        )
        _validate_internal_http_url(
            config.openarchiver_base_url, "OPENARCHIVER_BASE_URL"
        )
        _validate_internal_http_url(config.openrag_base_url, "OPENRAG_BASE_URL")
        if not config.openrag_ingest_directory.is_absolute():
            raise ValueError("OPENRAG_INGEST_DIRECTORY doit être un chemin absolu")
        if config.openrag_ingest_mode not in {"auto", "path", "api"}:
            raise ValueError("OPENRAG_INGEST_MODE doit être auto, path ou api")
        if config.openrag_auth_mode not in {"disabled", "auto", "required"}:
            raise ValueError("OPENRAG_AUTH_MODE doit être disabled, auto ou required")
        if config.openrag_auth_mode != "disabled":
            _validate_connector_public_url(config.connector_public_url)
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", config.openrag_auth_cookie_name):
            raise ValueError("OPENRAG_AUTH_COOKIE_NAME invalide")
        return config


@dataclass(frozen=True)
class ScanResult:
    sources: int
    emails: int
    complete: bool
    repeated: bool


@dataclass(frozen=True)
class WorkItem:
    kind: str
    object_id: str
    attempts: int


def _normalise_extension(value: str) -> str:
    extension = value.strip().lower()
    if extension and not extension.startswith("."):
        extension = "." + extension
    return extension


def _validate_internal_http_url(value: str, variable: str) -> None:
    parsed = urllib.parse.urlsplit(value)
    host = (parsed.hostname or "").lower()
    internal = (
        host in {"localhost", "127.0.0.1", "::1"}
        or host.endswith(".svc")
        or host.endswith(".svc.cluster.local")
        or (host and "." not in host)
    )
    if parsed.scheme != "http" or not internal or parsed.username or parsed.password:
        raise ValueError(f"{variable} doit être une URL HTTP interne sans identifiants")


def _validate_connector_public_url(value: str) -> None:
    parsed = urllib.parse.urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            "CONNECTOR_PUBLIC_URL doit être une origine HTTPS publique sans identifiants, chemin ni paramètres"
        )


def _validate_source_url(value: str) -> str:
    url = value.strip()
    parsed = urllib.parse.urlsplit(url)
    if (
        not url
        or len(url) > 2048
        or any(ord(character) < 32 or ord(character) == 127 for character in url)
        or parsed.scheme.lower() not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ConnectorError("URL de source distante invalide")
    return url


def render_remote_source_url(
    config: Config,
    *,
    kind: str,
    object_id: str,
    storage_path: str,
    email_id: str = "",
) -> str | None:
    """Render the optional user-facing OpenArchiver source URL.

    Every interpolated value is percent-encoded so metadata received from
    OpenArchiver cannot alter the configured URL structure. An empty template
    intentionally means that multipart ingestion proceeds without source_url.
    """
    template = config.openarchiver_source_url_template.strip()
    if not template:
        return None
    encoded_object_id = urllib.parse.quote(object_id, safe="")
    values = {
        "kind": kind,
        "object_id": encoded_object_id,
        "email_id": urllib.parse.quote(email_id, safe=""),
        "attachment_id": encoded_object_id if kind == "attachment" else "",
        "storage_path": urllib.parse.quote(storage_path, safe=""),
    }
    try:
        rendered = template.format_map(values)
    except (KeyError, ValueError):
        raise ConnectorError("OPENARCHIVER_SOURCE_URL_TEMPLATE invalide") from None
    return _validate_source_url(rendered)


def read_secret(path: Path, label: str) -> str:
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise ConnectorError(f"le fichier de clé {label} est vide")
    return value


def secret_is_configured(path: Path) -> bool:
    try:
        return bool(path.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, PermissionError, OSError, UnicodeDecodeError):
        return False


def secret_display_prefix(path: Path) -> str:
    """Retourne un préfixe affichable sans jamais révéler la clé complète."""
    try:
        value = path.read_text(encoding="utf-8").strip()
    except (FileNotFoundError, PermissionError, OSError, UnicodeDecodeError):
        return ""
    if not value:
        return ""
    visible_length = min(API_KEY_DISPLAY_PREFIX_LENGTH, max(1, len(value) - 4))
    return value[:visible_length]


def write_secret(path: Path, value: str, label: str) -> None:
    """Remplace atomiquement une clé sans la journaliser ni la conserver en SQLite."""
    secret = value.strip()
    if not secret:
        raise ConnectorError(f"la clé {label} ne peut pas être vide")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(secret + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
        os.chmod(path, 0o600)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


# ---------------------------------------------------------------------------
# Authentification déléguée à OpenRAG
# ---------------------------------------------------------------------------


class OpenRAGAuthClient:
    """Façade synchrone du login OpenRAG pour l'interface du connecteur.

    Le connecteur ne signe aucun token et ne conserve aucun mot de passe. Il
    demande à OpenRAG d'initialiser et de terminer Google OAuth, puis copie le
    JWT ``auth_token`` émis par OpenRAG dans un cookie de son propre domaine.
    Chaque requête protégée est revalidée par ``/auth/me`` ; rôles et
    permissions viennent de ``/users/me``.
    """

    MAX_RESPONSE_BYTES = 1_000_000

    def __init__(
        self,
        config: Config,
        *,
        opener: Callable[..., object] = urllib.request.urlopen,
    ) -> None:
        self.config = config
        self.opener = opener

    def _json_request(
        self,
        path: str,
        *,
        method: str = "GET",
        payload: Mapping[str, object] | None = None,
        token: str = "",
    ) -> tuple[int, dict[str, object], object]:
        data = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if token:
            if len(token) > 16_384 or not re.fullmatch(r"[A-Za-z0-9._~-]+", token):
                raise ConnectorError("cookie de session OpenRAG invalide")
            headers["Cookie"] = f"{self.config.openrag_auth_cookie_name}={token}"
        request = urllib.request.Request(
            self.config.openrag_base_url + path,
            data=data,
            method=method,
            headers=headers,
        )
        try:
            response = self.opener(
                request, timeout=self.config.request_timeout_seconds
            )
            status = int(getattr(response, "status", 200))
            response_headers = getattr(response, "headers", {})
            with response:
                raw = response.read(self.MAX_RESPONSE_BYTES + 1)
        except urllib.error.HTTPError as error:
            status = error.code
            response_headers = error.headers
            raw = error.read(self.MAX_RESPONSE_BYTES + 1)
        except (TimeoutError, urllib.error.URLError, OSError):
            raise ConnectorError("authentification OpenRAG indisponible") from None
        if len(raw) > self.MAX_RESPONSE_BYTES:
            raise ConnectorError("réponse d'authentification OpenRAG trop volumineuse")
        try:
            decoded = json.loads(raw.decode("utf-8")) if raw else {}
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ConnectorError("réponse d'authentification OpenRAG invalide") from None
        result = _require_object(decoded, "authentification OpenRAG")
        return status, result, response_headers

    def resolve(self, token: str = "") -> ConnectorPrincipal | None:
        """Retourne l'identité OpenRAG, ``None`` si une connexion est requise."""
        if self.config.openrag_auth_mode == "disabled":
            return ConnectorPrincipal(user_id="anonymous", no_auth_mode=True)

        status, auth, _headers = self._json_request("/auth/me", token=token)
        if status >= 500:
            raise ConnectorError("service d'authentification OpenRAG indisponible")
        if bool(auth.get("no_auth_mode")):
            if self.config.openrag_auth_mode == "required":
                raise ConnectorError(
                    "OpenRAG est sans authentification alors que le login est obligatoire"
                )
            return ConnectorPrincipal(user_id="anonymous", no_auth_mode=True)
        if status == 401 or not bool(auth.get("authenticated")):
            return None

        raw_user = _require_object(auth.get("user"), "utilisateur OpenRAG")
        user_id = raw_user.get("user_id")
        if not isinstance(user_id, str) or not user_id:
            raise ConnectorError("utilisateur OpenRAG sans identifiant")

        permissions: frozenset[str] = frozenset()
        roles: frozenset[str] = frozenset()
        rbac_enforced = False
        permission_status, permission_data, _headers = self._json_request(
            "/users/me", token=token
        )
        if permission_status == 401:
            return None
        if permission_status >= 400:
            raise ConnectorError(
                f"lecture des permissions OpenRAG: HTTP {permission_status}"
            )
        raw_permissions = permission_data.get("permissions", [])
        raw_roles = permission_data.get("roles", [])
        if not isinstance(raw_permissions, list) or not all(
            isinstance(item, str) for item in raw_permissions
        ):
            raise ConnectorError("permissions OpenRAG invalides")
        if not isinstance(raw_roles, list) or not all(
            isinstance(item, str) for item in raw_roles
        ):
            raise ConnectorError("rôles OpenRAG invalides")
        permissions = frozenset(raw_permissions)
        roles = frozenset(raw_roles)
        rbac_enforced = bool(permission_data.get("rbac_enforced"))

        return ConnectorPrincipal(
            user_id=user_id,
            email=str(raw_user.get("email") or ""),
            name=str(raw_user.get("name") or ""),
            picture=str(raw_user.get("picture") or ""),
            provider=str(raw_user.get("provider") or "unknown"),
            roles=roles,
            permissions=permissions,
            authenticated=True,
            rbac_enforced=rbac_enforced,
        )

    def begin_login(self) -> tuple[str, str]:
        """Initialise OAuth dans OpenRAG et retourne ``(URL, state)``."""
        if self.config.openrag_auth_mode == "disabled":
            raise ConnectorError("authentification OpenRAG désactivée")
        status, current, _headers = self._json_request("/auth/me")
        if status >= 500:
            raise ConnectorError("service d'authentification OpenRAG indisponible")
        if bool(current.get("no_auth_mode")):
            raise ConnectorError("OpenRAG est actuellement en mode sans authentification")

        callback_url = self.config.connector_public_url + "/auth/callback"
        status, result, _headers = self._json_request(
            "/auth/init",
            method="POST",
            payload={
                "connector_type": "google_drive",
                "purpose": "app_auth",
                "name": "OpenArchiver connector authentication",
                "redirect_uri": callback_url,
            },
        )
        if status >= 400:
            detail = str(result.get("error") or "initialisation OAuth refusée")
            raise ConnectorError(detail[:1000])
        connection_id = result.get("connection_id")
        oauth = _require_object(result.get("oauth_config"), "configuration OAuth")
        if not isinstance(connection_id, str) or not connection_id:
            raise ConnectorError("initialisation OAuth sans identifiant")
        endpoint = str(oauth.get("authorization_endpoint") or "")
        parsed_endpoint = urllib.parse.urlsplit(endpoint)
        if parsed_endpoint.scheme != "https" or not parsed_endpoint.hostname:
            raise ConnectorError("URL d'autorisation OAuth invalide")
        client_id = str(oauth.get("client_id") or "")
        redirect_uri = str(oauth.get("redirect_uri") or "")
        scopes = oauth.get("scopes")
        if (
            not client_id
            or redirect_uri != callback_url
            or not isinstance(scopes, list)
            or not scopes
            or not all(isinstance(scope, str) and scope for scope in scopes)
        ):
            raise ConnectorError("configuration OAuth OpenRAG incomplète")
        query = urllib.parse.urlencode(
            {
                "client_id": client_id,
                "response_type": "code",
                "scope": " ".join(scopes),
                "redirect_uri": redirect_uri,
                "access_type": "offline",
                "prompt": str(oauth.get("prompt") or "consent"),
                "state": connection_id,
            }
        )
        return endpoint + "?" + query, connection_id

    def complete_login(self, connection_id: str, code: str) -> str:
        """Termine OAuth dans OpenRAG et extrait son JWT du ``Set-Cookie``."""
        if (
            not re.fullmatch(r"[A-Za-z0-9_-]{1,200}", connection_id)
            or not code
            or len(code) > 4096
        ):
            raise ConnectorError("retour OAuth invalide")
        status, result, headers = self._json_request(
            "/auth/callback",
            method="POST",
            payload={
                "connection_id": connection_id,
                "authorization_code": code,
                "state": connection_id,
            },
        )
        if status >= 400:
            detail = str(result.get("error") or "callback OAuth refusé")
            raise ConnectorError(detail[:1000])
        raw_cookies = (
            headers.get_all("Set-Cookie")
            if hasattr(headers, "get_all")
            else [headers.get("Set-Cookie", "")]
        )
        for raw_cookie in raw_cookies:
            cookie = http.cookies.SimpleCookie()
            cookie.load(raw_cookie or "")
            morsel = cookie.get(self.config.openrag_auth_cookie_name)
            if morsel is not None and morsel.value:
                token = morsel.value
                if len(token) <= 16_384 and re.fullmatch(
                    r"[A-Za-z0-9._~-]+", token
                ):
                    return token
        raise ConnectorError("OpenRAG n'a pas émis de cookie de session")

    def logout(self, token: str) -> None:
        """Informe OpenRAG de la déconnexion, sans empêcher l'effacement local."""
        if not token or self.config.openrag_auth_mode == "disabled":
            return
        try:
            self._json_request("/auth/logout", method="POST", token=token)
        except ConnectorError:
            LOG.warning("déconnexion distante OpenRAG indisponible")


# ---------------------------------------------------------------------------
# Persistance SQLite, sélection et reprise
# ---------------------------------------------------------------------------


def connect_db(config: Config) -> sqlite3.Connection:
    """Ouvre SQLite et applique les migrations manquantes une seule fois.

    ``PRAGMA user_version`` est la version du schéma. Une migration doit être
    idempotente car plusieurs threads peuvent ouvrir la base au démarrage ;
    ``SCHEMA_LOCK`` sérialise cette phase dans le processus.

    La migration v3 change l'identité visible des connaissances. Elle efface
    donc le SHA et le ``task_id`` précédents, puis remet chaque objet indexable
    en file afin qu'OpenRAG reçoive le même contenu sous son nouveau nom.
    La v4 ne touche pas aux objets d'ingestion : elle ajoute seulement les
    tables d'identité OpenRAG et de journal d'audit.
    """
    config.state_db.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(config.state_db, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=30000")
    # Le conteneur utilise un système de fichiers racine en lecture seule.
    # Les tris volumineux ne doivent donc pas déborder dans /tmp.
    db.execute("PRAGMA temp_store=MEMORY")
    with SCHEMA_LOCK:
        version = int(db.execute("PRAGMA user_version").fetchone()[0])
        if version >= SCHEMA_VERSION:
            return db
        journal_mode = str(db.execute("PRAGMA journal_mode").fetchone()[0])
        if journal_mode.lower() != "delete":
            selected_mode = str(db.execute("PRAGMA journal_mode=DELETE").fetchone()[0])
            if selected_mode.lower() != "delete":
                raise sqlite3.OperationalError(
                    f"journal SQLite inattendu: {selected_mode}"
                )
        db.executescript(
            """
        CREATE TABLE IF NOT EXISTS sources (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL DEFAULT '',
            provider TEXT NOT NULL DEFAULT '',
            selected INTEGER NOT NULL DEFAULT 0,
            merged_into_id TEXT NOT NULL DEFAULT '',
            first_seen_at INTEGER NOT NULL,
            last_seen_at INTEGER NOT NULL,
            last_error TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS emails (
            id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            mailbox_path TEXT NOT NULL DEFAULT '',
            thread_id TEXT NOT NULL DEFAULT '',
            sent_at TEXT NOT NULL DEFAULT '',
            subject TEXT NOT NULL DEFAULT '',
            sender_name TEXT NOT NULL DEFAULT '',
            sender_email TEXT NOT NULL DEFAULT '',
            recipients_json TEXT NOT NULL DEFAULT '[]',
            cc_json TEXT NOT NULL DEFAULT '[]',
            message_id TEXT NOT NULL DEFAULT '',
            storage_path TEXT NOT NULL,
            storage_hash TEXT NOT NULL DEFAULT '',
            sha256 TEXT NOT NULL DEFAULT '',
            size_bytes INTEGER NOT NULL DEFAULT 0,
            has_attachments INTEGER NOT NULL DEFAULT 0,
            fingerprint TEXT NOT NULL,
            openrag_filename TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'discovered',
            attempts INTEGER NOT NULL DEFAULT 0,
            next_retry_at INTEGER NOT NULL DEFAULT 0,
            last_error TEXT NOT NULL DEFAULT '',
            task_id TEXT NOT NULL DEFAULT '',
            first_seen_at INTEGER NOT NULL,
            last_seen_at INTEGER NOT NULL,
            last_success_at INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS attachments (
            id TEXT PRIMARY KEY,
            filename TEXT NOT NULL DEFAULT '',
            mime_type TEXT NOT NULL DEFAULT '',
            storage_path TEXT NOT NULL,
            size_bytes INTEGER NOT NULL DEFAULT 0,
            sha256 TEXT NOT NULL DEFAULT '',
            metadata_fingerprint TEXT NOT NULL,
            openrag_filename TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'discovered',
            attempts INTEGER NOT NULL DEFAULT 0,
            next_retry_at INTEGER NOT NULL DEFAULT 0,
            last_error TEXT NOT NULL DEFAULT '',
            task_id TEXT NOT NULL DEFAULT '',
            first_seen_at INTEGER NOT NULL,
            last_seen_at INTEGER NOT NULL,
            last_success_at INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS email_attachments (
            email_id TEXT NOT NULL,
            attachment_id TEXT NOT NULL,
            PRIMARY KEY (email_id, attachment_id),
            FOREIGN KEY (email_id) REFERENCES emails(id),
            FOREIGN KEY (attachment_id) REFERENCES attachments(id)
        );
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            provider TEXT NOT NULL DEFAULT '',
            roles_json TEXT NOT NULL DEFAULT '[]',
            permissions_json TEXT NOT NULL DEFAULT '[]',
            rbac_enforced INTEGER NOT NULL DEFAULT 0,
            first_seen_at INTEGER NOT NULL,
            last_seen_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor_user_id TEXT NOT NULL,
            action TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            FOREIGN KEY (actor_user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS mailboxes (
            source_id TEXT NOT NULL,
            path TEXT NOT NULL,
            selected INTEGER NOT NULL DEFAULT 0,
            message_count INTEGER NOT NULL DEFAULT 0,
            first_seen_at INTEGER NOT NULL,
            last_seen_at INTEGER NOT NULL,
            PRIMARY KEY (source_id, path),
            FOREIGN KEY (source_id) REFERENCES sources(id)
        );
        CREATE INDEX IF NOT EXISTS emails_queue
            ON emails(status, next_retry_at);
        CREATE INDEX IF NOT EXISTS attachments_queue
            ON attachments(status, next_retry_at);
        CREATE INDEX IF NOT EXISTS email_attachments_by_attachment
            ON email_attachments(attachment_id, email_id);
            """
        )
        email_columns = {
            str(row["name"]) for row in db.execute("PRAGMA table_info(emails)")
        }
        if "sha256" not in email_columns:
            db.execute("ALTER TABLE emails ADD COLUMN sha256 TEXT NOT NULL DEFAULT ''")
        if "mailbox_path" not in email_columns:
            db.execute(
                "ALTER TABLE emails ADD COLUMN mailbox_path TEXT NOT NULL DEFAULT ''"
            )
        if version < 3:
            for row in db.execute(
                "SELECT id, subject, openrag_filename FROM emails"
            ).fetchall():
                filename = mail_openrag_filename(str(row["id"]), str(row["subject"]))
                if filename == str(row["openrag_filename"]):
                    continue
                db.execute(
                    """
                    UPDATE emails
                    SET openrag_filename=?, status='queued', sha256='', attempts=0,
                        next_retry_at=0, last_error='', task_id=''
                    WHERE id=?
                    """,
                    (filename, row["id"]),
                )
            attachment_parents: dict[str, tuple[str, str]] = {}
            for parent in db.execute(
                """
                SELECT ea.attachment_id, e.id AS email_id, e.subject
                FROM email_attachments ea
                JOIN emails e ON e.id=ea.email_id
                ORDER BY e.first_seen_at, e.id
                """
            ):
                attachment_parents.setdefault(
                    str(parent["attachment_id"]),
                    (str(parent["email_id"]), str(parent["subject"])),
                )
            for row in db.execute(
                "SELECT id, filename, openrag_filename, status FROM attachments"
            ).fetchall():
                email_id, mail_subject = attachment_parents.get(
                    str(row["id"]), ("", "")
                )
                filename = attachment_openrag_filename(
                    str(row["id"]),
                    str(row["filename"]),
                    mail_subject,
                    email_id,
                )
                if filename == str(row["openrag_filename"]):
                    continue
                db.execute(
                    """
                    UPDATE attachments
                    SET openrag_filename=?,
                        status=CASE WHEN status='non_indexable'
                                    THEN status ELSE 'queued' END,
                        sha256='', attempts=0, next_retry_at=0,
                        last_error=CASE WHEN status='non_indexable'
                                        THEN last_error ELSE '' END,
                        task_id=''
                    WHERE id=?
                    """,
                    (filename, row["id"]),
                )
        db.execute("INSERT OR IGNORE INTO settings(key,value) VALUES ('paused','0')")
        db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        db.commit()
    return db


@contextmanager
def database(config: Config) -> Iterator[sqlite3.Connection]:
    db = connect_db(config)
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def sync_connector_user(config: Config, principal: ConnectorPrincipal) -> None:
    """Mémorise l'identifiant OpenRAG et un instantané non-PII de ses droits."""
    if not principal.authenticated:
        return
    now = int(time.time())
    with database(config) as db:
        db.execute(
            """
            INSERT INTO users(
                id, provider, roles_json, permissions_json, rbac_enforced,
                first_seen_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                provider=excluded.provider,
                roles_json=excluded.roles_json,
                permissions_json=excluded.permissions_json,
                rbac_enforced=excluded.rbac_enforced,
                last_seen_at=excluded.last_seen_at
            WHERE users.provider<>excluded.provider
               OR users.roles_json<>excluded.roles_json
               OR users.permissions_json<>excluded.permissions_json
               OR users.rbac_enforced<>excluded.rbac_enforced
               OR users.last_seen_at<=excluded.last_seen_at-60
            """,
            (
                principal.user_id,
                principal.provider,
                json.dumps(sorted(principal.roles), separators=(",", ":")),
                json.dumps(sorted(principal.permissions), separators=(",", ":")),
                int(principal.rbac_enforced),
                now,
                now,
            ),
        )


def record_audit(
    config: Config, principal: ConnectorPrincipal, action: str
) -> None:
    """Attribue une mutation globale du connecteur à l'identité OpenRAG."""
    actor = principal.user_id if principal.authenticated else "anonymous"
    with database(config) as db:
        if not principal.authenticated:
            now = int(time.time())
            db.execute(
                """
                INSERT OR IGNORE INTO users(
                    id, provider, first_seen_at, last_seen_at
                ) VALUES ('anonymous', 'none', ?, ?)
                """,
                (now, now),
            )
        db.execute(
            "INSERT INTO audit_log(actor_user_id,action,created_at) VALUES (?,?,?)",
            (actor, action[:100], int(time.time())),
        )
        db.execute(
            """DELETE FROM audit_log
               WHERE id <= COALESCE((SELECT MAX(id) - 10000 FROM audit_log), 0)"""
        )


def set_source_selected(config: Config, source_id: str, selected: bool) -> None:
    now = int(time.time())
    with database(config) as db:
        db.execute(
            """
            INSERT INTO sources(id, selected, first_seen_at, last_seen_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET selected=excluded.selected
            """,
            (source_id, int(selected), now, now),
        )


def selected_source_ids(config: Config) -> list[str]:
    with database(config) as db:
        return [
            str(row[0])
            for row in db.execute("SELECT id FROM sources WHERE selected=1 ORDER BY id")
        ]


def source_rows(config: Config) -> list[sqlite3.Row]:
    with database(config) as db:
        return list(
            db.execute(
                """SELECT id, name, provider, selected, merged_into_id,
                          last_seen_at, last_error
                   FROM sources ORDER BY name, id"""
            )
        )


def is_paused(config: Config) -> bool:
    with database(config) as db:
        row = db.execute("SELECT value FROM settings WHERE key='paused'").fetchone()
        return bool(row and str(row[0]) == "1")


def set_paused(config: Config, paused: bool) -> None:
    with database(config) as db:
        db.execute(
            "INSERT OR REPLACE INTO settings(key,value) VALUES ('paused',?)",
            ("1" if paused else "0",),
        )


def normalize_runtime_pool_size(value: object) -> int:
    """Valide la taille manuelle du pool exposée dans l'interface."""

    try:
        size = int(str(value).strip())
    except (TypeError, ValueError):
        raise ConnectorError("la taille du pool doit être un entier") from None
    if not MIN_INGESTION_POOL_SIZE <= size <= MAX_INGESTION_POOL_SIZE:
        raise ConnectorError(
            f"la taille du pool doit être comprise entre "
            f"{MIN_INGESTION_POOL_SIZE} et {MAX_INGESTION_POOL_SIZE}"
        )
    return size


def apply_runtime_pool_size(config: Config, value: object) -> int:
    """Active une taille manuelle validée et demande un nouveau pool."""

    size = normalize_runtime_pool_size(value)
    with CONFIG_LOCK:
        config.ingestion_concurrency = size
    POOL_RECONFIGURE.set()
    return size


def persist_runtime_pool_size(config: Config, value: object) -> int:
    """Conserve la taille du pool sur le PVC puis l'active sans redémarrage."""

    size = normalize_runtime_pool_size(value)
    with database(config) as db:
        db.execute(
            "INSERT OR REPLACE INTO settings(key,value) VALUES (?,?)",
            (RUNTIME_POOL_SIZE_KEY, str(size)),
        )
    return apply_runtime_pool_size(config, size)


def restore_runtime_pool_size(config: Config) -> bool:
    """Restaure le réglage manuel ou conserve le défaut du Deployment."""

    with database(config) as db:
        row = db.execute(
            "SELECT value FROM settings WHERE key=?", (RUNTIME_POOL_SIZE_KEY,)
        ).fetchone()
    if row is None:
        return False
    with CONFIG_LOCK:
        config.ingestion_concurrency = normalize_runtime_pool_size(row[0])
    return True


def runtime_pool_size_is_persisted(config: Config) -> bool:
    with database(config) as db:
        row = db.execute(
            "SELECT 1 FROM settings WHERE key=?", (RUNTIME_POOL_SIZE_KEY,)
        ).fetchone()
    return row is not None


def normalize_runtime_urls(
    *, openrag_base_url: str, connector_public_url: str
) -> tuple[str, str]:
    """Normalise et valide les deux URL éditables."""
    normalized_openrag_url = openrag_base_url.strip().rstrip("/")
    normalized_connector_url = connector_public_url.strip().rstrip("/")
    try:
        _validate_internal_http_url(normalized_openrag_url, "OPENRAG_BASE_URL")
        _validate_connector_public_url(normalized_connector_url)
    except ValueError as error:
        raise ConnectorError(str(error)) from None
    return normalized_openrag_url, normalized_connector_url


def apply_runtime_urls(
    config: Config, *, openrag_base_url: str, connector_public_url: str
) -> None:
    """Valide puis active les deux URL éditables.

    L'URL OpenRAG reste volontairement limitée au réseau HTTP interne afin
    qu'un compte d'exploitation ne transforme pas le connecteur en relais
    HTTP arbitraire. L'URL du connecteur est publique car elle sert de callback
    OAuth et doit donc utiliser HTTPS. Le verrou sérialise les modifications
    concurrentes issues de l'interface.
    """
    normalized_openrag_url, normalized_connector_url = normalize_runtime_urls(
        openrag_base_url=openrag_base_url,
        connector_public_url=connector_public_url,
    )
    with CONFIG_LOCK:
        config.openrag_base_url = normalized_openrag_url
        config.connector_public_url = normalized_connector_url


def persist_runtime_urls(
    config: Config, *, openrag_base_url: str, connector_public_url: str
) -> None:
    """Conserve les URL sur le PVC puis les rend actives sans redémarrage."""
    normalized_openrag_url, normalized_connector_url = normalize_runtime_urls(
        openrag_base_url=openrag_base_url,
        connector_public_url=connector_public_url,
    )
    # Valider avant toute écriture afin de ne jamais persister un état qui
    # empêcherait le prochain démarrage du connecteur.
    with database(config) as db:
        db.executemany(
            "INSERT OR REPLACE INTO settings(key,value) VALUES (?,?)",
            (
                (RUNTIME_OPENRAG_URL_KEY, normalized_openrag_url),
                (RUNTIME_CONNECTOR_URL_KEY, normalized_connector_url),
            ),
        )
    apply_runtime_urls(
        config,
        openrag_base_url=normalized_openrag_url,
        connector_public_url=normalized_connector_url,
    )


def restore_runtime_urls(config: Config) -> bool:
    """Charge les URL de l'interface, ou conserve l'amorçage du Deployment."""
    with database(config) as db:
        values = {
            str(row["key"]): str(row["value"])
            for row in db.execute(
                "SELECT key,value FROM settings WHERE key IN (?,?)",
                (RUNTIME_OPENRAG_URL_KEY, RUNTIME_CONNECTOR_URL_KEY),
            )
        }
    if not values:
        return False
    apply_runtime_urls(
        config,
        openrag_base_url=values.get(
            RUNTIME_OPENRAG_URL_KEY, config.openrag_base_url
        ),
        connector_public_url=values.get(
            RUNTIME_CONNECTOR_URL_KEY, config.connector_public_url
        ),
    )
    return True


def runtime_urls_are_persisted(config: Config) -> bool:
    """Indique si les deux valeurs de l'interface existent sur le PVC."""
    with database(config) as db:
        row = db.execute(
            "SELECT COUNT(*) FROM settings WHERE key IN (?,?)",
            (RUNTIME_OPENRAG_URL_KEY, RUNTIME_CONNECTOR_URL_KEY),
        ).fetchone()
    return bool(row and int(row[0]) == 2)


def mailbox_rows(config: Config) -> list[sqlite3.Row]:
    with database(config) as db:
        return list(
            db.execute(
                """
                SELECT m.source_id, m.path, m.selected, m.message_count,
                       m.last_seen_at, s.name AS source_name
                FROM mailboxes m
                JOIN sources s ON s.id=m.source_id
                ORDER BY s.name, m.source_id, m.path
                """
            )
        )


def replace_mailbox_selection(
    config: Config, selections: Iterable[tuple[str, str]]
) -> list[tuple[str, str]]:
    """Remplace atomiquement la sélection par des dossiers déjà découverts."""
    requested = sorted(set(selections))
    with database(config) as db:
        known = {
            (str(row[0]), str(row[1]))
            for row in db.execute("SELECT source_id, path FROM mailboxes")
        }
        if set(requested) - known:
            raise ConnectorError("sélection contenant un dossier inconnu")
        db.execute("UPDATE mailboxes SET selected=0")
        db.executemany(
            "UPDATE mailboxes SET selected=1 WHERE source_id=? AND path=?",
            requested,
        )
    return requested


def retryable_rows(
    config: Config, status: str, *, limit: int = 200
) -> list[sqlite3.Row]:
    """Liste bornée des objets réindexables appartenant à la sélection active."""
    if status not in {"failed", "lost"}:
        raise ConnectorError("statut de réindexation invalide")
    with database(config) as db:
        return list(
            db.execute(
                """
                SELECT 'email' AS kind, e.id, e.openrag_filename, e.attempts,
                       e.next_retry_at, e.last_error
                FROM emails e
                WHERE e.status=? AND EXISTS (
                    SELECT 1 FROM sources s
                    JOIN mailboxes m
                      ON m.source_id=e.source_id AND m.path=e.mailbox_path
                    WHERE s.id=e.source_id AND s.selected=1 AND m.selected=1
                )
                UNION ALL
                SELECT 'attachment' AS kind, a.id, a.openrag_filename, a.attempts,
                       a.next_retry_at, a.last_error
                FROM attachments a
                WHERE a.status=? AND EXISTS (
                    SELECT 1 FROM email_attachments ea
                    JOIN emails e ON e.id=ea.email_id
                    JOIN sources s ON s.id=e.source_id
                    JOIN mailboxes m
                      ON m.source_id=e.source_id AND m.path=e.mailbox_path
                    WHERE ea.attachment_id=a.id
                      AND s.selected=1 AND m.selected=1
                )
                ORDER BY next_retry_at, kind, id
                LIMIT ?
                """,
                (status, status, max(1, min(limit, 500))),
            )
        )


def requeue_objects(
    config: Config, objects: Iterable[tuple[str, str, str]]
) -> int:
    """Remet en file uniquement les objets failed/lost explicitement choisis."""
    requested = list(dict.fromkeys(objects))
    if not requested or len(requested) > 500:
        raise ConnectorError("sélection de réindexation invalide")
    updated = 0
    with database(config) as db:
        for kind, object_id, expected_status in requested:
            if kind not in {"email", "attachment"} or expected_status not in {
                "failed",
                "lost",
            }:
                raise ConnectorError("objet de réindexation invalide")
            table = "emails" if kind == "email" else "attachments"
            cursor = db.execute(
                f"""
                UPDATE {table}
                SET status='queued', attempts=0, next_retry_at=0,
                    last_error='', task_id=''
                WHERE id=? AND status=?
                """,
                (object_id, expected_status),
            )
            updated += cursor.rowcount
    return updated


def replace_source_selection(config: Config, source_ids: Iterable[str]) -> list[str]:
    """Remplace atomiquement la sélection par des sources déjà découvertes."""
    requested = sorted(set(source_ids))
    with database(config) as db:
        known = {str(row[0]) for row in db.execute("SELECT id FROM sources")}
        unknown = sorted(set(requested) - known)
        if unknown:
            raise ConnectorError("sélection contenant une source inconnue")
        db.execute("UPDATE sources SET selected=0")
        if requested:
            placeholders = ",".join("?" for _ in requested)
            db.execute(
                f"UPDATE sources SET selected=1 WHERE id IN ({placeholders})",
                requested,
            )
    return requested


def recover_interrupted(config: Config) -> int:
    """Replace les opérations interrompues en échec avec un retry borné."""
    now = int(time.time())
    recovered = 0
    with database(config) as db:
        for table in ("emails", "attachments"):
            rows = db.execute(
                f"SELECT id, attempts FROM {table} "
                "WHERE status IN ('downloading', 'ingesting')"
            ).fetchall()
            for row in rows:
                retry_at = (
                    now + config.retry_base_seconds
                    if int(row["attempts"]) < config.max_auto_retries
                    else 0
                )
                db.execute(
                    f"""
                    UPDATE {table}
                    SET status='failed', task_id='', next_retry_at=?,
                        last_error='opération interrompue par un redémarrage'
                    WHERE id=?
                    """,
                    (retry_at, row["id"]),
                )
                recovered += 1
    return recovered


class RateLimiter:
    def __init__(
        self,
        requests_per_minute: int,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.limit = max(1, requests_per_minute)
        self.clock = clock
        self.sleeper = sleeper
        self.timestamps: deque[float] = deque()
        self.lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self.lock:
                now = self.clock()
                while self.timestamps and now - self.timestamps[0] >= 60:
                    self.timestamps.popleft()
                if len(self.timestamps) < self.limit:
                    self.timestamps.append(now)
                    return
                delay = max(0.0, 60 - (now - self.timestamps[0]))
            self.sleeper(delay)


# ---------------------------------------------------------------------------
# Client OpenArchiver et inventaire
# ---------------------------------------------------------------------------


class OpenArchiverClient:
    """Accès HTTP authentifié à OpenArchiver avec débit et reprises bornés."""

    def __init__(
        self,
        config: Config,
        *,
        limiter: RateLimiter | None = None,
        opener: Callable[..., object] = urllib.request.urlopen,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.config = config
        self.limiter = limiter or RateLimiter(config.openarchiver_requests_per_minute)
        self.opener = opener
        self.sleeper = sleeper

    def _open(self, path: str):
        attempts = self.config.max_auto_retries
        for attempt in range(attempts):
            self.limiter.acquire()
            request = urllib.request.Request(
                self.config.openarchiver_base_url + path,
                headers={
                    "X-API-KEY": read_secret(
                        self.config.openarchiver_api_key_file, "OpenArchiver"
                    ),
                    "Accept": "application/json",
                },
            )
            try:
                return self.opener(request, timeout=self.config.request_timeout_seconds)
            except urllib.error.HTTPError as error:
                if error.code == 429 and attempt + 1 < attempts:
                    retry_after = error.headers.get("Retry-After", "1")
                    try:
                        delay = max(0.0, float(retry_after))
                    except ValueError:
                        delay = 1.0
                    self.sleeper(min(delay, self.config.retry_max_seconds))
                    continue
                if 500 <= error.code < 600 and attempt + 1 < attempts:
                    self.sleeper(self._backoff(attempt))
                    continue
                raise HTTPStatusError(error.code, "appel OpenArchiver") from None
            except (TimeoutError, urllib.error.URLError, OSError):
                if attempt + 1 < attempts:
                    self.sleeper(self._backoff(attempt))
                    continue
                raise ConnectorError(
                    "appel OpenArchiver temporairement indisponible"
                ) from None
        raise ConnectorError("appel OpenArchiver épuisé")

    def _backoff(self, attempt: int) -> float:
        return float(
            min(
                self.config.retry_max_seconds,
                self.config.retry_base_seconds * (2**attempt),
            )
        )

    def json(self, path: str) -> object:
        # Une réponse HTTP peut être acceptée puis interrompue pendant la
        # lecture du corps. Rejouer alors le GET complet, avec la même borne
        # que les autres erreurs transitoires OpenArchiver.
        for attempt in range(self.config.max_auto_retries):
            response = self._open(path)
            try:
                raw = response.read()
            except (
                http.client.IncompleteRead,
                http.client.RemoteDisconnected,
                TimeoutError,
                OSError,
            ):
                if attempt + 1 >= self.config.max_auto_retries:
                    raise ConnectorError("réponse OpenArchiver interrompue") from None
                self.sleeper(self._backoff(attempt))
                continue
            finally:
                response.close()
            break
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ConnectorError("réponse JSON OpenArchiver invalide") from None

    def list_sources(self) -> list[dict[str, object]]:
        payload = self.json("/ingestion-sources")
        if not isinstance(payload, list):
            raise ConnectorError("liste des sources OpenArchiver invalide")
        return [_require_object(item, "source") for item in payload]

    def list_emails(self, source_id: str, page: int, limit: int) -> dict[str, object]:
        path = "/archived-emails/ingestion-source/{}?{}".format(
            urllib.parse.quote(source_id, safe=""),
            urllib.parse.urlencode({"page": page, "limit": limit}),
        )
        payload = self.json(path)
        return _require_object(payload, "page de mails")

    def email_detail(self, email_id: str) -> dict[str, object]:
        payload = self.json("/archived-emails/" + urllib.parse.quote(email_id, safe=""))
        return _require_object(payload, "détail du mail")

    def download(self, storage_path: str, destination: Path) -> tuple[int, str]:
        path = "/storage/download?" + urllib.parse.urlencode({"path": storage_path})
        response = self._open(path)
        digest = hashlib.sha256()
        total = 0
        try:
            with destination.open("wb") as handle:
                while True:
                    chunk = response.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > self.config.max_file_bytes:
                        raise FileTooLargeError("fichier OpenArchiver trop volumineux")
                    digest.update(chunk)
                    handle.write(chunk)
        finally:
            response.close()
        return total, digest.hexdigest()


def _require_object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ConnectorError(f"{label} invalide")
    return value


def refresh_sources(
    config: Config, client: OpenArchiverClient
) -> list[dict[str, object]]:
    sources = client.list_sources()
    now = int(time.time())
    with database(config) as db:
        for source in sources:
            source_id = source.get("id")
            if not isinstance(source_id, str) or not source_id:
                raise ConnectorError("source OpenArchiver sans identifiant")
            db.execute(
                """
                INSERT INTO sources(
                    id, name, provider, merged_into_id, first_seen_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name=excluded.name, provider=excluded.provider,
                    merged_into_id=excluded.merged_into_id,
                    last_seen_at=excluded.last_seen_at, last_error=''
                """,
                (
                    source_id,
                    _optional_text(source.get("name")),
                    _optional_text(source.get("provider")),
                    _optional_text(source.get("mergedIntoId")),
                    now,
                    now,
                ),
            )
    return sources


def _scan_source_pass(
    config: Config,
    client: OpenArchiverClient,
    source_id: str,
    progress: Callable[[int, int], None] | None = None,
) -> tuple[dict[str, dict[str, object]], int]:
    found: dict[str, dict[str, object]] = {}
    page = 1
    latest_total = 0
    while True:
        payload = client.list_emails(source_id, page, config.page_limit)
        items = payload.get("items")
        total = payload.get("total")
        if not isinstance(items, list) or not isinstance(total, int) or total < 0:
            raise IncompleteScanError(f"pagination invalide pour la source {source_id}")
        response_limit = payload.get("limit", config.page_limit)
        if not isinstance(response_limit, int) or response_limit < 1:
            raise IncompleteScanError(
                f"limite de pagination invalide pour la source {source_id}"
            )
        latest_total = total
        for raw in items:
            email = _validate_email(_require_object(raw, "mail"), source_id)
            found[str(email["id"])] = email
        if progress is not None:
            progress(len(found), max(total, len(found)))
        if page * response_limit >= total:
            break
        if not items:
            break
        page += 1
    return found, latest_total


def _source_inventory(
    config: Config,
    client: OpenArchiverClient,
    source_id: str,
    progress: Callable[[int, int], None] | None = None,
) -> tuple[dict[str, dict[str, object]], bool]:
    """Retente uniquement une réponse invalide, sans exiger un total stable."""
    repeated = False
    for attempt in range(2):
        try:
            found, _latest_total = _scan_source_pass(
                config, client, source_id, progress=progress
            )
        except IncompleteScanError:
            repeated = True
        else:
            return found, repeated
        if attempt == 1:
            raise IncompleteScanError(
                f"pagination invalide après deux tentatives pour la source {source_id}"
            )
    raise AssertionError("boucle de stabilisation invalide")


def scan_selected_sources(
    config: Config,
    client: OpenArchiverClient,
    progress: Callable[[str, int, int], None] | None = None,
) -> ScanResult:
    source_ids = selected_source_ids(config)
    # Le marqueur nanoseconde évite de confondre deux scans lancés dans la même
    # seconde lors du classement conservatif des absences.
    scan_started = time.time_ns()
    global_emails: dict[str, dict[str, object]] = {}
    repeated = False
    try:
        for source_id in source_ids:
            found, source_repeated = _source_inventory(
                config,
                client,
                source_id,
                progress=(
                    lambda current, total, current_source=source_id: progress(
                        f"Inventaire de la source {current_source}", current, total
                    )
                    if progress is not None
                    else None
                ),
            )
            repeated = repeated or source_repeated
            global_emails.update(found)
    except IncompleteScanError as error:
        with database(config) as db:
            db.execute(
                "INSERT OR REPLACE INTO settings(key,value) VALUES ('last_scan_error',?)",
                (str(error),),
            )
        return ScanResult(len(source_ids), len(global_emails), False, True)

    # Un inventaire peut contenir plusieurs dizaines de milliers de mails.
    # Une transaction unique gardait alors le verrou d'écriture SQLite assez
    # longtemps pour empêcher les slots d'ingestion de réserver leur travail.
    # Les données sont écrites par lots courts ; seul le marqueur final rend le
    # nouvel inventaire visible comme un instantané complet.
    with database(config) as db:
        if source_ids:
            placeholders = ",".join("?" for _ in source_ids)
            db.execute(
                f"UPDATE mailboxes SET message_count=0 "
                f"WHERE source_id IN ({placeholders})",
                source_ids,
            )
        mailbox_counts: dict[tuple[str, str], int] = {}
        for email in global_emails.values():
            key = (str(email["source_id"]), str(email["mailbox_path"]))
            mailbox_counts[key] = mailbox_counts.get(key, 0) + 1
        for (source_id, path), count in mailbox_counts.items():
            db.execute(
                """
                INSERT INTO mailboxes(
                    source_id, path, message_count, first_seen_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(source_id,path) DO UPDATE SET
                    message_count=excluded.message_count,
                    last_seen_at=excluded.last_seen_at
                """,
                (source_id, path, count, scan_started, scan_started),
            )
    emails = list(global_emails.values())
    for offset in range(0, len(emails), 200):
        with database(config) as db:
            for email in emails[offset : offset + 200]:
                _upsert_email(db, email, scan_started)
    with database(config) as db:
        db.execute(
            "INSERT OR REPLACE INTO settings(key,value) VALUES ('last_scan_error','')"
        )
        db.executemany(
            "INSERT OR REPLACE INTO settings(key,value) VALUES (?,?)",
            (
                ("last_inventory_completed_at", str(int(time.time()))),
                ("last_inventory_sources", str(len(source_ids))),
                ("last_inventory_emails", str(len(global_emails))),
            ),
        )
    return ScanResult(len(source_ids), len(global_emails), True, repeated)


def cached_inventory(
    config: Config,
    *,
    now: int | None = None,
    allow_expired: bool = False,
) -> tuple[ScanResult, int] | None:
    # Paramètres conservés pour compatibilité avec les appels antérieurs : un
    # inventaire d'archives ne vieillit plus automatiquement.
    del now, allow_expired
    with database(config) as db:
        values = {
            str(row["key"]): str(row["value"])
            for row in db.execute(
                """SELECT key, value FROM settings
                   WHERE key IN (
                       'last_inventory_completed_at',
                       'last_inventory_sources',
                       'last_inventory_emails'
                )"""
            )
        }
        legacy = db.execute(
            """SELECT MAX(last_seen_at) AS completed_at,
                      COUNT(DISTINCT source_id) AS sources,
                      COUNT(*) AS emails
               FROM emails"""
        ).fetchone()
    completed_at = int(values.get("last_inventory_completed_at", "0") or 0)
    if not completed_at and legacy and int(legacy["completed_at"] or 0):
        raw_completed_at = int(legacy["completed_at"])
        completed_at = (
            raw_completed_at // 1_000_000_000
            if raw_completed_at > 10_000_000_000
            else raw_completed_at
        )
        values["last_inventory_sources"] = str(int(legacy["sources"] or 0))
        values["last_inventory_emails"] = str(int(legacy["emails"] or 0))
        with database(config) as db:
            db.executemany(
                "INSERT OR REPLACE INTO settings(key,value) VALUES (?,?)",
                (
                    ("last_inventory_completed_at", str(completed_at)),
                    ("last_inventory_sources", values["last_inventory_sources"]),
                    ("last_inventory_emails", values["last_inventory_emails"]),
                ),
            )
    if not completed_at:
        return None
    return (
        ScanResult(
            int(values.get("last_inventory_sources", "0") or 0),
            int(values.get("last_inventory_emails", "0") or 0),
            True,
            False,
        ),
        completed_at,
    )


def _validate_email(raw: dict[str, object], fallback_source: str) -> dict[str, object]:
    email_id = raw.get("id")
    storage_path = raw.get("storagePath")
    if not isinstance(email_id, str) or not email_id:
        raise IncompleteScanError("mail OpenArchiver sans identifiant")
    if not isinstance(storage_path, str) or not storage_path:
        raise IncompleteScanError(f"mail {email_id} sans storagePath")
    # Une source fusionnée peut retourner un mail dont ingestionSourceId pointe
    # vers une source enfant. L'identité globale reste l'UUID du mail, mais la
    # portée opérationnelle doit rester la source effectivement sélectionnée.
    source_id = fallback_source
    recipients, recipient_cc = _split_recipients(raw.get("recipients"))
    cc = sorted(set(recipient_cc + _normalise_recipients(raw.get("cc"))))
    return {
        "id": email_id,
        "source_id": source_id,
        "mailbox_path": _optional_text(raw.get("path")),
        "thread_id": _optional_text(raw.get("threadId")),
        "sent_at": _optional_text(raw.get("sentAt")),
        "subject": _optional_text(raw.get("subject")),
        "sender_name": _optional_text(raw.get("senderName")),
        "sender_email": _optional_text(raw.get("senderEmail")),
        "recipients": recipients,
        "cc": cc,
        "message_id": _optional_text(raw.get("messageIdHeader")),
        "storage_path": storage_path,
        "storage_hash": _optional_text(raw.get("storageHashSha256")),
        "size_bytes": _nonnegative_int(raw.get("sizeBytes")),
        "has_attachments": bool(raw.get("hasAttachments", False)),
    }


def _normalise_recipients(value: object) -> list[str]:
    if isinstance(value, dict):
        result: list[str] = []
        for nested in value.values():
            result.extend(_normalise_recipients(nested))
        return sorted(set(result))
    if not isinstance(value, list):
        return []
    result = []
    for item in value:
        if isinstance(item, str):
            result.append(item)
        elif isinstance(item, dict):
            name = _optional_text(item.get("name"))
            address = _optional_text(item.get("email"))
            result.append(
                f"{name} <{address}>" if name and address else address or name
            )
    return sorted(item for item in result if item)


def _split_recipients(value: object) -> tuple[list[str], list[str]]:
    if isinstance(value, dict):
        to = _normalise_recipients(value.get("to", []))
        cc = _normalise_recipients(value.get("cc", []))
        return to, cc
    if not isinstance(value, list):
        return [], []
    to: list[str] = []
    cc: list[str] = []
    for item in value:
        formatted = _normalise_recipients([item])
        if not formatted:
            continue
        kind = ""
        if isinstance(item, dict):
            kind = _optional_text(item.get("recipientType") or item.get("type")).lower()
        (cc if kind == "cc" else to).extend(formatted)
    return sorted(set(to)), sorted(set(cc))


def _optional_text(value: object) -> str:
    return value if isinstance(value, str) else ""


def _nonnegative_int(value: object) -> int:
    return value if isinstance(value, int) and value >= 0 else 0


def _fingerprint(values: Mapping[str, object], keys: Sequence[str]) -> str:
    canonical = json.dumps(
        {key: values.get(key) for key in keys},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _upsert_email(db: sqlite3.Connection, email: dict[str, object], now: int) -> None:
    """Insère un mail ou le remet en file uniquement si son identité a changé.

    Le fingerprint regroupe les métadonnées qui influencent la connaissance.
    Un inventaire identique conserve donc ``validated`` ; une modification du
    mail ou de son nom OpenRAG invalide la preuve SHA et relance l'ingestion.
    """

    fingerprint = _fingerprint(
        email,
        (
            "id",
            "source_id",
            "mailbox_path",
            "thread_id",
            "sent_at",
            "subject",
            "sender_name",
            "sender_email",
            "recipients",
            "cc",
            "message_id",
            "storage_path",
            "storage_hash",
            "size_bytes",
            "has_attachments",
        ),
    )
    db.execute(
        """
        INSERT INTO emails(
            id, source_id, mailbox_path, thread_id, sent_at, subject, sender_name,
            sender_email, recipients_json, cc_json, message_id, storage_path,
            storage_hash, size_bytes, has_attachments, fingerprint,
            openrag_filename, status, first_seen_at, last_seen_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            source_id=excluded.source_id, mailbox_path=excluded.mailbox_path,
            thread_id=excluded.thread_id,
            sent_at=excluded.sent_at, subject=excluded.subject,
            sender_name=excluded.sender_name, sender_email=excluded.sender_email,
            recipients_json=excluded.recipients_json, cc_json=excluded.cc_json,
            message_id=excluded.message_id, storage_path=excluded.storage_path,
            storage_hash=excluded.storage_hash, size_bytes=excluded.size_bytes,
            has_attachments=excluded.has_attachments,
            openrag_filename=excluded.openrag_filename,
            sha256=CASE
                WHEN emails.fingerprint <> excluded.fingerprint
                  OR emails.openrag_filename <> excluded.openrag_filename THEN ''
                ELSE emails.sha256 END,
            status=CASE
                WHEN emails.fingerprint <> excluded.fingerprint
                  OR emails.openrag_filename <> excluded.openrag_filename THEN 'queued'
                WHEN emails.status IN ('missing','unavailable') THEN 'queued'
                ELSE emails.status END,
            attempts=CASE
                WHEN emails.fingerprint <> excluded.fingerprint
                  OR emails.openrag_filename <> excluded.openrag_filename THEN 0
                ELSE emails.attempts END,
            next_retry_at=CASE
                WHEN emails.fingerprint <> excluded.fingerprint
                  OR emails.openrag_filename <> excluded.openrag_filename THEN 0
                ELSE emails.next_retry_at END,
            last_error=CASE
                WHEN emails.fingerprint <> excluded.fingerprint
                  OR emails.openrag_filename <> excluded.openrag_filename THEN ''
                ELSE emails.last_error END,
            task_id=CASE
                WHEN emails.fingerprint <> excluded.fingerprint
                  OR emails.openrag_filename <> excluded.openrag_filename THEN ''
                ELSE emails.task_id END,
            fingerprint=excluded.fingerprint, last_seen_at=excluded.last_seen_at
        """,
        (
            email["id"],
            email["source_id"],
            email["mailbox_path"],
            email["thread_id"],
            email["sent_at"],
            email["subject"],
            email["sender_name"],
            email["sender_email"],
            json.dumps(email["recipients"], ensure_ascii=False),
            json.dumps(email["cc"], ensure_ascii=False),
            email["message_id"],
            email["storage_path"],
            email["storage_hash"],
            email["size_bytes"],
            int(bool(email["has_attachments"])),
            fingerprint,
            mail_openrag_filename(str(email["id"]), str(email["subject"])),
            now,
            now,
        ),
    )


def _short_identifier(value: str) -> str:
    try:
        return uuid.UUID(value).hex[:12]
    except ValueError:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _readable_filename_stem(value: str, fallback: str, max_length: int) -> str:
    ascii_value = unicodedata.normalize("NFKD", value).encode(
        "ascii", "ignore"
    ).decode("ascii")
    stem = re.sub(r"[^A-Za-z0-9._-]+", "-", ascii_value)
    stem = re.sub(r"-{2,}", "-", stem).strip("._-")
    stem = (stem or fallback)[:max_length].rstrip("._-")
    return stem or fallback


def mail_openrag_filename(email_id: str, subject: str = "") -> str:
    """Construit ``<objet-lisible>--<id-court>.eml`` sans collision d'UUID."""

    suffix = f"--{_short_identifier(email_id)}.eml"
    stem = _readable_filename_stem(subject, "mail", 255 - len(suffix))
    return f"{stem}{suffix}"


def safe_attachment_extension(filename: str) -> str:
    suffix = Path(filename).suffix.lower()
    return suffix if SAFE_EXTENSION.fullmatch(suffix) else ""


def attachment_openrag_filename(
    attachment_id: str,
    filename: str,
    mail_subject: str = "",
    email_id: str = "",
) -> str:
    """Préfixe une pièce jointe par le titre et l'identifiant du mail d'origine."""

    extension = safe_attachment_extension(filename)
    source_stem = Path(filename).stem if extension else filename
    suffix = f"--{_short_identifier(attachment_id)}{extension}"
    mail_identifier = f"--{_short_identifier(email_id)}" if email_id else ""
    mail_stem = _readable_filename_stem(mail_subject, "mail", 128)
    attachment_stem = _readable_filename_stem(
        source_stem,
        "piece-jointe",
        255 - len(mail_stem) - len(mail_identifier) - len(suffix) - len("--"),
    )
    return f"{mail_stem}{mail_identifier}--{attachment_stem}{suffix}"


def inventory_attachments(
    config: Config,
    email_id: str,
    detail: Mapping[str, object],
    *,
    observed_at: int | None = None,
) -> list[str]:
    """Enregistre les pièces jointes découvertes dans le détail d'un mail.

    Une même pièce jointe peut être référencée par plusieurs mails. Le premier
    parent déjà enregistré reste alors l'origine stable utilisée dans son nom,
    ce qui évite de renommer la connaissance à chaque nouvel inventaire.
    """

    raw_attachments = detail.get("attachments", [])
    if not isinstance(raw_attachments, list):
        raise ConnectorError("inventaire des pièces jointes invalide")
    now = int(time.time()) if observed_at is None else observed_at
    identifiers: list[str] = []
    with database(config) as db:
        parent = db.execute(
            "SELECT subject FROM emails WHERE id=?", (email_id,)
        ).fetchone()
        mail_subject = str(parent[0]) if parent else ""
        for raw in raw_attachments:
            attachment = _require_object(raw, "pièce jointe")
            attachment_id = attachment.get("id")
            storage_path = attachment.get("storagePath")
            if not isinstance(attachment_id, str) or not attachment_id:
                raise ConnectorError("pièce jointe sans identifiant")
            if not isinstance(storage_path, str) or not storage_path:
                raise ConnectorError(f"pièce jointe {attachment_id} sans storagePath")
            filename = _optional_text(attachment.get("filename"))
            mime_type = _optional_text(attachment.get("mimeType"))
            size = _nonnegative_int(attachment.get("sizeBytes"))
            extension = safe_attachment_extension(filename)
            supported = extension in config.supported_extensions
            if not supported:
                status = "non_indexable"
                error = "extension non supportée"
            elif size > config.max_file_bytes:
                status = "non_indexable"
                error = "fichier trop volumineux"
            else:
                status = "queued"
                error = ""
            metadata = {
                "id": attachment_id,
                "filename": filename,
                "mime_type": mime_type,
                "storage_path": storage_path,
                "size_bytes": size,
                "classification": status,
            }
            fingerprint = _fingerprint(metadata, tuple(metadata))
            existing_parent = db.execute(
                """
                SELECT e.id, e.subject
                FROM email_attachments ea
                JOIN emails e ON e.id=ea.email_id
                WHERE ea.attachment_id=?
                ORDER BY e.id LIMIT 1
                """,
                (attachment_id,),
            ).fetchone()
            origin_email_id = (
                str(existing_parent["id"]) if existing_parent else email_id
            )
            origin_subject = (
                str(existing_parent["subject"])
                if existing_parent
                else mail_subject
            )
            db.execute(
                """
                INSERT INTO attachments(
                    id, filename, mime_type, storage_path, size_bytes,
                    metadata_fingerprint, openrag_filename, status, last_error,
                    first_seen_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    filename=excluded.filename, mime_type=excluded.mime_type,
                    storage_path=excluded.storage_path, size_bytes=excluded.size_bytes,
                    openrag_filename=excluded.openrag_filename,
                    status=CASE
                        WHEN attachments.metadata_fingerprint <> excluded.metadata_fingerprint
                          OR attachments.openrag_filename <> excluded.openrag_filename
                            THEN excluded.status
                        ELSE attachments.status END,
                    sha256=CASE
                        WHEN attachments.metadata_fingerprint <> excluded.metadata_fingerprint
                          OR attachments.openrag_filename <> excluded.openrag_filename
                            THEN '' ELSE attachments.sha256 END,
                    attempts=CASE
                        WHEN attachments.metadata_fingerprint <> excluded.metadata_fingerprint
                          OR attachments.openrag_filename <> excluded.openrag_filename
                            THEN 0 ELSE attachments.attempts END,
                    last_error=CASE
                        WHEN attachments.metadata_fingerprint <> excluded.metadata_fingerprint
                          OR attachments.openrag_filename <> excluded.openrag_filename
                            THEN excluded.last_error ELSE attachments.last_error END,
                    next_retry_at=CASE
                        WHEN attachments.metadata_fingerprint <> excluded.metadata_fingerprint
                          OR attachments.openrag_filename <> excluded.openrag_filename
                            THEN 0 ELSE attachments.next_retry_at END,
                    task_id=CASE
                        WHEN attachments.metadata_fingerprint <> excluded.metadata_fingerprint
                          OR attachments.openrag_filename <> excluded.openrag_filename
                            THEN '' ELSE attachments.task_id END,
                    metadata_fingerprint=excluded.metadata_fingerprint,
                    last_seen_at=excluded.last_seen_at
                """,
                (
                    attachment_id,
                    filename,
                    mime_type,
                    storage_path,
                    size,
                    fingerprint,
                    attachment_openrag_filename(
                        attachment_id,
                        filename,
                        origin_subject,
                        origin_email_id,
                    ),
                    status,
                    error,
                    now,
                    now,
                ),
            )
            db.execute(
                "INSERT OR IGNORE INTO email_attachments(email_id,attachment_id) VALUES (?,?)",
                (email_id, attachment_id),
            )
            identifiers.append(attachment_id)
    return sorted(identifiers)


# ---------------------------------------------------------------------------
# Transformation facultative du contenu mail
# ---------------------------------------------------------------------------


class _ReadableHTML(HTMLParser):
    BLOCKS = {"br", "p", "div", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.ignored = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"script", "style"}:
            self.ignored += 1
        elif not self.ignored and tag in self.BLOCKS:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style"} and self.ignored:
            self.ignored -= 1
        elif not self.ignored and tag in self.BLOCKS:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self.ignored:
            self.parts.append(data)

    def text(self) -> str:
        lines = [" ".join(line.split()) for line in "".join(self.parts).splitlines()]
        return "\n".join(line for line in lines if line).strip()


def html_to_text(value: str) -> str:
    parser = _ReadableHTML()
    parser.feed(value)
    parser.close()
    return parser.text()


def _decode_part(part) -> str:
    payload = part.get_payload(decode=True)
    if payload is None:
        value = part.get_payload()
        return value if isinstance(value, str) else ""
    charset = part.get_content_charset() or "utf-8"
    try:
        return payload.decode(charset, "replace")
    except LookupError:
        return payload.decode("utf-8", "replace")


def parse_eml(path: Path) -> tuple[str, dict[str, list[str] | str]]:
    with path.open("rb") as handle:
        message = BytesParser(policy=policy.default).parse(handle)
    plain: list[str] = []
    html_parts: list[str] = []
    parts = message.walk() if message.is_multipart() else (message,)
    for part in parts:
        if (
            part.is_multipart()
            or part.get_content_disposition() == "attachment"
            or part.get_filename()
        ):
            continue
        content_type = part.get_content_type()
        if content_type == "text/plain":
            plain.append(_decode_part(part))
        elif content_type == "text/html":
            html_parts.append(html_to_text(_decode_part(part)))
    body = "\n\n".join(item.strip() for item in (plain or html_parts) if item.strip())
    metadata: dict[str, list[str] | str] = {
        "subject": str(message.get("Subject", "")),
        "from": _format_addresses(message.get_all("From", [])),
        "to": _format_addresses(message.get_all("To", [])),
        "cc": _format_addresses(message.get_all("Cc", [])),
        "message_id": str(message.get("Message-ID", "")),
        "date": str(message.get("Date", "")),
    }
    return body, metadata


def _format_addresses(headers: Iterable[str]) -> list[str]:
    result = []
    for name, address in getaddresses(list(headers)):
        result.append(f"{name} <{address}>" if name and address else address or name)
    return result


def render_mail_markdown(
    email_row: Mapping[str, object],
    body: str,
    attachment_rows: Sequence[Mapping[str, object]],
    *,
    source_name: str = "",
    link_template: str = "",
) -> str:
    def value(row: Mapping[str, object], key: str) -> object:
        try:
            return row[key]
        except (KeyError, IndexError):
            return ""

    def field(label: str, value: object) -> str:
        text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
        return f"- **{label} :** {text}" if text else ""

    recipients = json.loads(str(value(email_row, "recipients_json") or "[]"))
    cc = json.loads(str(value(email_row, "cc_json") or "[]"))
    lines = [
        f"# {str(value(email_row, 'subject') or '(sans objet)').strip()}",
        "",
        field("Identifiant OpenArchiver", value(email_row, "id")),
        field("Source", value(email_row, "source_id")),
        field("Nom de la source", source_name),
        field("Dossier IMAP", value(email_row, "mailbox_path")),
        field("Thread", value(email_row, "thread_id")),
        field("Date", value(email_row, "sent_at")),
        field(
            "Expéditeur",
            " ".join(
                part
                for part in (
                    str(value(email_row, "sender_name") or ""),
                    str(value(email_row, "sender_email") or ""),
                )
                if part
            ),
        ),
        field("À", ", ".join(recipients)),
        field("Cc", ", ".join(cc)),
        field("Message-ID", value(email_row, "message_id")),
    ]
    if link_template:
        try:
            link = link_template.format(email_id=value(email_row, "id"))
        except (KeyError, ValueError):
            link = ""
        lines.append(field("OpenArchiver", link))
    lines = [line for line in lines if line]
    lines.extend(["", "## Pièces jointes", ""])
    if attachment_rows:
        for attachment in sorted(
            attachment_rows, key=lambda row: str(value(row, "id"))
        ):
            lines.append(
                "- `{}` — {}".format(
                    value(attachment, "id"), value(attachment, "filename") or "sans nom"
                )
            )
    else:
        lines.append("Aucune.")
    lines.extend(["", "## Corps", "", body.strip(), ""])
    return "\n".join(lines)


def deposit_source(
    config: Config,
    openarchiver: OpenArchiverClient,
    storage_path: str,
    filename: str,
) -> tuple[Path, int, str]:
    """Télécharge puis publie atomiquement une source dans l'inbox partagée."""
    safe_name = Path(filename).name
    if not safe_name or safe_name != filename:
        raise ConnectorError("nom de source OpenRAG invalide")

    directory = config.openrag_ingest_directory
    directory.mkdir(parents=True, exist_ok=True)
    directory = directory.resolve()
    destination = directory / safe_name
    temporary = directory / f".{safe_name}.{uuid.uuid4().hex}.part"
    try:
        size, sha256 = openarchiver.download(storage_path, temporary)
        if size > config.max_file_bytes:
            raise FileTooLargeError(
                f"fichier supérieur à {config.max_file_bytes} octets"
            )
        with temporary.open("rb+") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        try:
            directory_fd = os.open(
                directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            )
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            # Certains serveurs NFS ne permettent pas fsync sur un répertoire.
            pass
        return destination, size, sha256
    finally:
        temporary.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Soumission OpenRAG et preuve durable d'indexation
# ---------------------------------------------------------------------------


class OpenRAGClient:
    """Client minimal des API de tâche, d'ingestion et de connaissances OpenRAG."""

    def __init__(
        self, config: Config, *, sleeper: Callable[[float], None] = time.sleep
    ) -> None:
        self.config = config
        self.sleeper = sleeper
        self._auto_api_mode = False

    def upload(
        self, path: Path, remote_name: str, source_url: str | None = None
    ) -> str:
        """Stream a source through the authenticated multipart API."""
        if Path(remote_name).name != remote_name or not re.fullmatch(
            r"[A-Za-z0-9._-]{1,255}", remote_name
        ):
            raise ConnectorError("nom de source OpenRAG invalide")
        if source_url is not None:
            source_url = _validate_source_url(source_url)
        parsed = urllib.parse.urlsplit(self.config.openrag_base_url)
        boundary = f"openarchiver-{uuid.uuid4().hex}"
        content_type = (
            mimetypes.guess_type(remote_name)[0] or "application/octet-stream"
        )
        fields = [
            ("replace_duplicates", "true"),
            ("archive_source", "false"),
        ]
        if source_url is not None:
            fields.append(("source_url", source_url))
        prefix_parts = []
        for name, value in fields:
            prefix_parts.append(
                (
                    f"--{boundary}\r\n"
                    f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
                    f"{value}\r\n"
                ).encode("utf-8")
            )
        prefix_parts.append(
            (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="file"; filename="{remote_name}"\r\n'
                f"Content-Type: {content_type}\r\n\r\n"
            ).encode("utf-8")
        )
        prefix = b"".join(prefix_parts)
        suffix = f"\r\n--{boundary}--\r\n".encode("ascii")
        connection = http.client.HTTPConnection(
            parsed.hostname,
            parsed.port or 80,
            timeout=self.config.request_timeout_seconds,
        )
        target = (parsed.path.rstrip("/") + self.config.openrag_upload_path) or "/"
        try:
            connection.putrequest("POST", target)
            connection.putheader(
                "X-API-Key", read_secret(self.config.openrag_api_key_file, "OpenRAG")
            )
            connection.putheader("Accept", "application/json")
            connection.putheader(
                "Content-Type", f"multipart/form-data; boundary={boundary}"
            )
            connection.putheader(
                "Content-Length", str(len(prefix) + path.stat().st_size + len(suffix))
            )
            connection.endheaders()
            connection.send(prefix)
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(CHUNK_SIZE), b""):
                    connection.send(chunk)
            connection.send(suffix)
            response = connection.getresponse()
            status = response.status
            raw = response.read()
        finally:
            connection.close()
        if not 200 <= status < 300:
            raise HTTPStatusError(status, "ingestion OpenRAG")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ConnectorError("réponse JSON OpenRAG invalide") from None
        task_id = payload.get("task_id") if isinstance(payload, dict) else None
        if not isinstance(task_id, str) or not task_id:
            raise ConnectorError("réponse OpenRAG sans task_id")
        return task_id

    def ingest_path(self, path: Path) -> str:
        ingestion_directory = self.config.openrag_ingest_directory.resolve()
        resolved_path = path.resolve()
        try:
            resolved_path.relative_to(ingestion_directory)
        except ValueError:
            raise ConnectorError("source hors du dossier d'ingestion OpenRAG") from None

        payload = json.dumps(
            {
                "path": str(resolved_path),
                "replace_duplicates": True,
            }
        ).encode("utf-8")
        request = urllib.request.Request(
            self.config.openrag_base_url + self.config.openrag_ingest_path,
            data=payload,
            method="POST",
            headers={
                "X-API-Key": read_secret(self.config.openrag_api_key_file, "OpenRAG"),
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(
                request, timeout=self.config.request_timeout_seconds
            ) as response:
                raw = response.read()
        except urllib.error.HTTPError as error:
            raise HTTPStatusError(error.code, "ingestion OpenRAG") from None
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ConnectorError("réponse JSON OpenRAG invalide") from None
        task_id = payload.get("task_id") if isinstance(payload, dict) else None
        if not isinstance(task_id, str) or not task_id:
            raise ConnectorError("réponse OpenRAG sans task_id")
        return task_id

    def ingest_source(
        self, path: Path, source_url: str | None = None
    ) -> tuple[str, bool]:
        """Submit a source using the safe mode selected for this deployment.

        Multi-user OpenRAG rejects server-local paths. In ``auto`` mode the
        first such 403 switches this client instance to multipart API uploads.
        The boolean result tells the caller whether the shared file is only a
        working copy that can be removed after a successful task.
        """
        mode = self.config.openrag_ingest_mode
        if mode == "api" or (mode == "auto" and self._auto_api_mode):
            return self.upload(path, path.name, source_url), True
        try:
            return self.ingest_path(path), False
        except HTTPStatusError as error:
            if mode != "auto" or error.status != 403:
                raise
            self._auto_api_mode = True
            LOG.info(
                "ingestion locale refusée par OpenRAG; bascule vers l'API multipart"
            )
            return self.upload(path, path.name, source_url), True

    def task(self, task_id: str, *, path: str | None = None) -> dict[str, object]:
        encoded = urllib.parse.quote(task_id, safe="")
        request_path = path or self.config.openrag_task_path.format(task_id=encoded)
        request = urllib.request.Request(
            self.config.openrag_base_url + request_path,
            headers={
                "X-API-Key": read_secret(self.config.openrag_api_key_file, "OpenRAG"),
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(
                request, timeout=self.config.request_timeout_seconds
            ) as response:
                raw = response.read()
        except urllib.error.HTTPError as error:
            raise HTTPStatusError(error.code, "lecture tâche OpenRAG") from None
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ConnectorError("réponse de tâche OpenRAG invalide") from None
        return _require_object(payload, "tâche OpenRAG")

    def indexed_document(self, filename: str) -> dict[str, object] | None:
        """Retourne le document durable correspondant exactement au nom fourni."""
        return self.indexed_documents([filename]).get(filename)

    def indexed_documents(
        self, filenames: Sequence[str]
    ) -> dict[str, dict[str, object]]:
        """Retourne en une requête les documents durables demandés par leur nom."""
        requested = list(dict.fromkeys(filenames))
        if not requested or len(requested) > 500:
            raise ConnectorError("lot de vérification OpenRAG invalide")
        query = urllib.parse.urlencode(
            [
                *(("data_sources", filename) for filename in requested),
                ("page_size", str(len(requested))),
                ("sort_by", "filename"),
            ]
        )
        request = urllib.request.Request(
            f"{self.config.openrag_base_url}/v2/files?{query}",
            headers={
                "X-API-Key": read_secret(self.config.openrag_api_key_file, "OpenRAG"),
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(
                request, timeout=self.config.request_timeout_seconds
            ) as response:
                raw = response.read()
        except urllib.error.HTTPError as error:
            raise HTTPStatusError(error.code, "vérification document OpenRAG") from None
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ConnectorError("réponse de documents OpenRAG invalide") from None
        result = _require_object(payload, "documents OpenRAG")
        files = result.get("files")
        if not isinstance(files, list):
            raise ConnectorError("réponse de documents OpenRAG sans liste de fichiers")
        requested_set = set(requested)
        matches: dict[str, dict[str, object]] = {}
        for item in files:
            if not isinstance(item, dict):
                continue
            filename = str(item.get("filename", ""))
            if filename not in requested_set:
                continue
            if filename in matches:
                raise ConnectorError(f"plusieurs documents OpenRAG nommés {filename}")
            matches[filename] = item
        return matches

    def document_is_indexed(
        self, filename: str, sha256: str, *, attempts: int = 3
    ) -> bool:
        """Prouve que les octets soumis sont disponibles comme chunks OpenRAG."""
        tries = max(1, attempts)
        for attempt in range(tries):
            document = self.indexed_document(filename)
            if document is not None and _document_matches(document, sha256):
                return True
            if attempt + 1 < tries:
                self.sleeper(1)
        return False

    def wait(self, task_id: str) -> None:
        deadline = time.monotonic() + self.config.task_timeout_seconds
        encoded = urllib.parse.quote(task_id, safe="")
        path = self.config.openrag_task_path.format(task_id=encoded)
        fallback = f"/v1/tasks/{encoded}"
        while time.monotonic() < deadline:
            try:
                payload = self.task(task_id, path=path)
            except HTTPStatusError as error:
                if error.status == 404 and path != fallback:
                    path = fallback
                    continue
                if error.status == 404:
                    raise LostTaskError(f"tâche OpenRAG inconnue: {task_id}") from None
                raise
            status = str(payload.get("status", "")).lower()
            if status == "completed":
                if _failed_count(payload.get("failed_files")) == 0:
                    return
                raise ConnectorError("tâche OpenRAG terminée avec fichiers en échec")
            if status in {"failed", "cancelled", "canceled"}:
                raise ConnectorError(f"tâche OpenRAG {status}")
            self.sleeper(OPENRAG_TASK_POLL_SECONDS)
        raise TimeoutError("tâche OpenRAG non terminée avant expiration")


def _failed_count(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, (list, tuple, set, dict)):
        return len(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 1 if value else 0


def openrag_document_id(sha256: str) -> str:
    """Reproduit ``OpenRAG.hash_id`` depuis le SHA-256 hexadécimal local."""
    try:
        digest = bytes.fromhex(sha256)
    except ValueError:
        raise ConnectorError("empreinte SHA-256 locale invalide") from None
    if len(digest) != hashlib.sha256().digest_size:
        raise ConnectorError("empreinte SHA-256 locale invalide")
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")[:24]


def wait_for_indexed_document(
    openrag: OpenRAGClient, task_id: str, filename: str, sha256: str
) -> None:
    """Attend la tâche volatile puis exige une preuve durable dans OpenSearch."""
    try:
        openrag.wait(task_id)
    except LostTaskError:
        if not openrag.document_is_indexed(filename, sha256):
            raise
        LOG.info("tâche OpenRAG disparue mais document indexé: %s", filename)
        return
    if not openrag.document_is_indexed(filename, sha256):
        raise ConnectorError(
            "tâche OpenRAG terminée sans document indexé correspondant"
        )


def reconciliation_rows(config: Config) -> list[sqlite3.Row]:
    """Instantané des connaissances sélectionnées dont le contenu est vérifiable."""
    with database(config) as db:
        return list(
            db.execute(
                """
                SELECT 'email' AS kind, e.id, e.openrag_filename, e.sha256, e.status
                FROM emails e
                WHERE e.status IN ('validated','lost','failed') AND e.sha256<>''
                  AND EXISTS (
                    SELECT 1 FROM sources s
                    JOIN mailboxes m
                      ON m.source_id=e.source_id AND m.path=e.mailbox_path
                    WHERE s.id=e.source_id AND s.selected=1 AND m.selected=1
                  )
                UNION ALL
                SELECT 'attachment' AS kind, a.id, a.openrag_filename,
                       a.sha256, a.status
                FROM attachments a
                WHERE a.status IN ('validated','lost','failed') AND a.sha256<>''
                  AND EXISTS (
                    SELECT 1 FROM email_attachments ea
                    JOIN emails e ON e.id=ea.email_id
                    JOIN sources s ON s.id=e.source_id
                    JOIN mailboxes m
                      ON m.source_id=e.source_id AND m.path=e.mailbox_path
                    WHERE ea.attachment_id=a.id
                      AND s.selected=1 AND m.selected=1
                  )
                ORDER BY kind, id
                """
            )
        )


def _document_matches(document: Mapping[str, object] | None, sha256: str) -> bool:
    if document is None:
        return False
    try:
        chunk_count = int(document.get("chunk_count", 0))
    except (TypeError, ValueError):
        chunk_count = 0
    return (
        chunk_count > 0
        and str(document.get("document_id", "")) == openrag_document_id(sha256)
    )


def reconcile_openrag(
    config: Config,
    openrag: OpenRAGClient,
    progress: Callable[[int, int], None] | None = None,
) -> ReconciliationResult:
    """Réconcilie l'état SQLite avec les chunks réellement visibles dans OpenRAG."""
    rows = reconciliation_rows(config)
    total = len(rows)
    checked = restored = lost = 0
    report = progress or (lambda _current, _total: None)
    report(0, total)
    for offset in range(0, total, RECONCILE_BATCH_SIZE):
        batch = rows[offset : offset + RECONCILE_BATCH_SIZE]
        documents = openrag.indexed_documents(
            [str(row["openrag_filename"]) for row in batch]
        )
        now = int(time.time())
        with database(config) as db:
            for row in batch:
                filename = str(row["openrag_filename"])
                matches = _document_matches(documents.get(filename), str(row["sha256"]))
                table = "emails" if row["kind"] == "email" else "attachments"
                if matches and row["status"] in {"lost", "failed"}:
                    cursor = db.execute(
                        f"""
                        UPDATE {table}
                        SET status='validated', last_success_at=?, last_error='',
                            next_retry_at=0
                        WHERE id=? AND status=? AND sha256=?
                        """,
                        (now, row["id"], row["status"], row["sha256"]),
                    )
                    restored += cursor.rowcount
                elif not matches and row["status"] == "validated":
                    cursor = db.execute(
                        f"""
                        UPDATE {table}
                        SET status='lost', attempts=0, task_id='',
                            last_error='document absent ou obsolète dans OpenRAG',
                            next_retry_at=?
                        WHERE id=? AND status='validated' AND sha256=?
                        """,
                        (now, row["id"], row["sha256"]),
                    )
                    lost += cursor.rowcount
        checked += len(batch)
        report(checked, total)
    return ReconciliationResult(checked, restored, lost)


# ---------------------------------------------------------------------------
# File locale, transitions d'état et concurrence d'ingestion
# ---------------------------------------------------------------------------


def claim_next(config: Config, *, now: int | None = None) -> WorkItem | None:
    """Réserve atomiquement le prochain objet éligible de la sélection active.

    ``BEGIN IMMEDIATE`` empêche deux threads de réserver la même ligne. Les
    objets ``lost`` arrivés à échéance passent avant les nouveaux objets, puis
    viennent les échecs dont le délai exponentiel est écoulé.
    """

    timestamp = int(time.time()) if now is None else now
    db = connect_db(config)
    try:
        db.execute("BEGIN IMMEDIATE")
        paused = db.execute("SELECT value FROM settings WHERE key='paused'").fetchone()
        if paused is not None and str(paused[0]) == "1":
            db.commit()
            return None
        for kind, table in (("email", "emails"), ("attachment", "attachments")):
            selection_clause = (
                """EXISTS (
                       SELECT 1 FROM sources s
                       JOIN mailboxes m
                         ON m.source_id=emails.source_id
                        AND m.path=emails.mailbox_path
                       WHERE s.id=emails.source_id
                         AND s.selected=1 AND m.selected=1
                   )"""
                if kind == "email"
                else """EXISTS (
                       SELECT 1 FROM email_attachments ea
                       JOIN emails e ON e.id=ea.email_id
                       JOIN sources s ON s.id=e.source_id
                       JOIN mailboxes m
                         ON m.source_id=e.source_id AND m.path=e.mailbox_path
                       WHERE ea.attachment_id=attachments.id
                         AND s.selected=1 AND m.selected=1
                   )"""
            )
            row = db.execute(
                f"""
                SELECT id, attempts FROM {table}
                WHERE ({selection_clause}) AND (
                    status='queued' OR (
                        status IN ('failed','lost') AND attempts < ?
                        AND next_retry_at > 0 AND next_retry_at <= ?
                    )
                )
                ORDER BY CASE status
                           WHEN 'lost' THEN 0
                           WHEN 'queued' THEN 1
                           ELSE 2 END,
                         next_retry_at, id
                LIMIT 1
                """,
                (config.max_auto_retries, timestamp),
            ).fetchone()
            if row is None:
                continue
            attempts = int(row["attempts"]) + 1
            cursor = db.execute(
                f"""
                UPDATE {table}
                SET status='downloading', attempts=?, next_retry_at=0,
                    last_error='', task_id=''
                WHERE id=? AND attempts=? AND status IN ('queued','failed','lost')
                """,
                (attempts, row["id"], row["attempts"]),
            )
            if cursor.rowcount == 1:
                db.commit()
                return WorkItem(kind, str(row["id"]), attempts)
        db.commit()
        return None
    finally:
        db.close()


def _set_object_state(
    config: Config, kind: str, object_id: str, clause: str, values: Sequence[object]
) -> None:
    table = "emails" if kind == "email" else "attachments"
    with database(config) as db:
        db.execute(f"UPDATE {table} SET {clause} WHERE id=?", (*values, object_id))


def _rows_for_email(
    config: Config, email_id: str
) -> tuple[sqlite3.Row, list[sqlite3.Row], str]:
    with database(config) as db:
        email_row = db.execute(
            "SELECT * FROM emails WHERE id=?", (email_id,)
        ).fetchone()
        if email_row is None:
            raise ConnectorError("mail réservé introuvable")
        attachments = list(
            db.execute(
                """
                SELECT a.* FROM attachments a
                JOIN email_attachments ea ON ea.attachment_id=a.id
                WHERE ea.email_id=? ORDER BY a.id
                """,
                (email_id,),
            )
        )
        source = db.execute(
            "SELECT name FROM sources WHERE id=?", (email_row["source_id"],)
        ).fetchone()
        return email_row, attachments, str(source[0]) if source else ""


def process_work_item(
    config: Config,
    item: WorkItem,
    openarchiver: OpenArchiverClient,
    openrag: OpenRAGClient,
) -> None:
    """Exécute le cycle complet d'un mail ou d'une pièce jointe réservé.

    La transition nominale est ``downloading -> ingesting -> validated``.
    Toute exception est convertie en ``failed`` ou ``lost`` et reçoit, tant que
    la limite n'est pas atteinte, une date de nouvelle tentative.
    """

    try:
        if item.kind == "email":
            with database(config) as db:
                row = db.execute(
                    "SELECT * FROM emails WHERE id=?", (item.object_id,)
                ).fetchone()
            if row is None:
                raise ConnectorError("mail réservé introuvable")
            if int(row["has_attachments"]):
                detail = openarchiver.email_detail(item.object_id)
                inventory_attachments(config, item.object_id, detail)
            document, _size, sha256 = deposit_source(
                config,
                openarchiver,
                str(row["storage_path"]),
                str(row["openrag_filename"]),
            )
            _set_object_state(
                config,
                "email",
                item.object_id,
                "sha256=?, status='ingesting'",
                (sha256,),
            )
            source_url = render_remote_source_url(
                config,
                kind="email",
                object_id=item.object_id,
                storage_path=str(row["storage_path"]),
                email_id=item.object_id,
            )
            task_id, api_upload = openrag.ingest_source(document, source_url)
            _set_object_state(config, "email", item.object_id, "task_id=?", (task_id,))
            wait_for_indexed_document(
                openrag, task_id, str(row["openrag_filename"]), sha256
            )
            if api_upload:
                try:
                    document.unlink(missing_ok=True)
                except OSError as error:
                    LOG.warning(
                        "copie de travail non supprimée après ingestion: %s", error
                    )
        else:
            with database(config) as db:
                row = db.execute(
                    """
                    SELECT a.*,
                           COALESCE((
                               SELECT MIN(ea.email_id)
                               FROM email_attachments ea
                               WHERE ea.attachment_id=a.id
                           ), '') AS email_id
                    FROM attachments a WHERE a.id=?
                    """,
                    (item.object_id,),
                ).fetchone()
            if row is None:
                raise ConnectorError("pièce jointe réservée introuvable")
            document, _size, sha256 = deposit_source(
                config,
                openarchiver,
                str(row["storage_path"]),
                str(row["openrag_filename"]),
            )
            _set_object_state(
                config,
                "attachment",
                item.object_id,
                "sha256=?, status='ingesting'",
                (sha256,),
            )
            source_url = render_remote_source_url(
                config,
                kind="attachment",
                object_id=item.object_id,
                storage_path=str(row["storage_path"]),
                email_id=str(row["email_id"]),
            )
            task_id, api_upload = openrag.ingest_source(document, source_url)
            _set_object_state(
                config, "attachment", item.object_id, "task_id=?", (task_id,)
            )
            wait_for_indexed_document(
                openrag, task_id, str(row["openrag_filename"]), sha256
            )
            if api_upload:
                try:
                    document.unlink(missing_ok=True)
                except OSError as error:
                    LOG.warning(
                        "copie de travail non supprimée après ingestion: %s", error
                    )
        _set_object_state(
            config,
            item.kind,
            item.object_id,
            "status='validated', last_success_at=?, last_error='', next_retry_at=0",
            (int(time.time()),),
        )
    except LostTaskError as error:
        _record_lost(config, item, error)
    except FileTooLargeError as error:
        if item.kind == "attachment":
            _set_object_state(
                config,
                item.kind,
                item.object_id,
                "status='non_indexable', last_error=?, next_retry_at=0",
                (_safe_error(error),),
            )
            return
        _record_failure(config, item, error)
    except Exception as error:
        _record_failure(config, item, error)


def _record_failure(config: Config, item: WorkItem, error: Exception) -> None:
    if item.attempts < config.max_auto_retries:
        delay = min(
            config.retry_max_seconds,
            config.retry_base_seconds * (2 ** (item.attempts - 1)),
        )
        retry_at = int(time.time()) + delay
    else:
        retry_at = 0
    message = _safe_error(error)
    _set_object_state(
        config,
        item.kind,
        item.object_id,
        "status='failed', last_error=?, next_retry_at=?",
        (message, retry_at),
    )


def _record_lost(config: Config, item: WorkItem, error: LostTaskError) -> None:
    if item.attempts < config.max_auto_retries:
        delay = min(
            config.retry_max_seconds,
            config.retry_base_seconds * (2 ** (item.attempts - 1)),
        )
        retry_at = int(time.time()) + delay
    else:
        retry_at = 0
    _set_object_state(
        config,
        item.kind,
        item.object_id,
        "status='lost', last_error=?, next_retry_at=?",
        (_safe_error(error), retry_at),
    )


def _safe_error(error: Exception) -> str:
    if isinstance(error, (ConnectorError, TimeoutError, ValueError)):
        return str(error)[:1000]
    if isinstance(error, sqlite3.Error):
        detail = " ".join(str(error).split())
        return f"{error.__class__.__name__}: {detail}"[:1000]
    return error.__class__.__name__


def effective_ingestion_concurrency(
    config: Config, state: RuntimeState | None = None
) -> int:
    """Retourne la taille manuelle du pool, toujours comprise entre 1 et 6."""

    with CONFIG_LOCK:
        effective = min(
            MAX_INGESTION_POOL_SIZE,
            config.ingestion_concurrency_max,
            max(MIN_INGESTION_POOL_SIZE, int(config.ingestion_concurrency)),
        )
    if state is not None:
        state.ingestion_concurrency_updated(effective)
    return effective


def process_queue(
    config: Config,
    openarchiver: OpenArchiverClient,
    openrag: OpenRAGClient,
    progress: Callable[[int, int], None] | None = None,
    state: RuntimeState | None = None,
) -> int:
    """Vide la file locale avec un superviseur redimensionnable à chaud.

    Une Future ne traite qu'un objet. Le superviseur maintient ensuite autant
    de Futures actives que la taille manuelle courante, comprise entre 1 et 6.
    Une conversion Docling longue n'empêche donc ni les autres slots d'avancer,
    ni une augmentation de capacité depuis l'interface.
    """

    effective_ingestion_concurrency(config, state)
    POOL_RECONFIGURE.clear()
    progress_lock = threading.Lock()
    processed_total = 0
    initial_total = selected_queue_pending_count(config)
    if progress is not None:
        progress(0, initial_total)

    def process_one(slot: int) -> bool:
        """Réserve et traite un objet ; ``False`` signifie file épuisée."""
        nonlocal processed_total
        consecutive_errors = 0
        while True:
            try:
                item = claim_next(config)
            except Exception:
                consecutive_errors += 1
                delay = min(30.0, float(2 ** min(consecutive_errors - 1, 5)))
                LOG.exception(
                    "slot d'ingestion %d: réservation impossible; reprise dans %.0fs",
                    slot,
                    delay,
                )
                time.sleep(delay)
                continue
            if item is None:
                return False
            try:
                process_work_item(config, item, openarchiver, openrag)
            except Exception:
                # ``process_work_item`` convertit normalement toute erreur en
                # état failed/lost. Cette garde protège le pool si cette
                # persistance échoue elle-même (SQLite, disque, etc.). L'objet
                # reste alors downloading/ingesting et sera récupéré au
                # prochain redémarrage, mais le slot continue immédiatement.
                consecutive_errors += 1
                delay = min(30.0, float(2 ** min(consecutive_errors - 1, 5)))
                LOG.exception(
                    "slot d'ingestion %d: traitement interrompu pour %s/%s; "
                    "reprise dans %.0fs",
                    slot,
                    item.kind,
                    item.object_id,
                    delay,
                )
                time.sleep(delay)
                # L'objet a bien occupé ce slot. Le superviseur doit en ouvrir
                # un nouveau même si la persistance de son échec a elle-même
                # échoué.
                return True
            with progress_lock:
                processed_total += 1
                current = processed_total
            if progress is not None:
                try:
                    progress(current, max(initial_total, current))
                except Exception:
                    # La télémétrie ne doit jamais arrêter le travail métier.
                    LOG.exception(
                        "slot d'ingestion %d: mise à jour de progression ignorée",
                        slot,
                    )
            return True

    active: dict[Future[bool], int] = {}
    next_slot = 1
    queue_exhausted = False
    with ThreadPoolExecutor(
        max_workers=MAX_INGESTION_POOL_SIZE,
        thread_name_prefix="openarchiver-ingest",
    ) as pool:
        while True:
            current_workers = effective_ingestion_concurrency(config, state)
            if POOL_RECONFIGURE.is_set():
                POOL_RECONFIGURE.clear()
                queue_exhausted = False

            while not queue_exhausted and len(active) < current_workers:
                future = pool.submit(process_one, next_slot)
                active[future] = next_slot
                next_slot = next_slot % MAX_INGESTION_POOL_SIZE + 1

            if not active:
                return processed_total

            completed, _ = wait(
                active, timeout=0.25, return_when=FIRST_COMPLETED
            )
            if not completed:
                continue

            # Une réservation vide signifie qu'il ne reste momentanément plus
            # d'objet réservable. On attend alors la fin des travaux actifs :
            # ils peuvent découvrir de nouvelles pièces jointes à leur tour.
            saw_claimed = False
            saw_empty = False
            for future in completed:
                slot = active.pop(future)
                try:
                    claimed = future.result()
                except Exception:
                    LOG.exception(
                        "slot d'ingestion %d: erreur non interceptée", slot
                    )
                    claimed = True
                saw_claimed = saw_claimed or claimed
                saw_empty = saw_empty or not claimed
            queue_exhausted = saw_empty and not saw_claimed
            if queue_exhausted and not active:
                return processed_total


def selected_queue_pending_count(config: Config) -> int:
    counts = _status_counts(config, selected_only=True)
    active_statuses = {"queued", "downloading", "ingesting"}
    return sum(
        count
        for values in counts.values()
        for status, count in values.items()
        if status in active_statuses
    )


def selected_mails_validated_last_minute(
    config: Config, *, now: int | None = None
) -> int:
    """Débit glissant des mails sélectionnés réellement validés."""
    cutoff = (int(time.time()) if now is None else now) - 60
    with database(config) as db:
        row = db.execute(
            """
            SELECT COUNT(*) FROM emails e
            WHERE e.status='validated' AND e.last_success_at>=?
              AND EXISTS (
                SELECT 1 FROM sources s
                JOIN mailboxes m
                  ON m.source_id=e.source_id AND m.path=e.mailbox_path
                WHERE s.id=e.source_id AND s.selected=1 AND m.selected=1
              )
            """,
            (cutoff,),
        ).fetchone()
    return int(row[0]) if row else 0


def active_ingestion_tasks(config: Config) -> list[dict[str, object]]:
    """Retourne les documents actuellement détenus par les slots du connecteur."""
    with database(config) as db:
        rows = db.execute(
            """
            SELECT kind, id, openrag_filename, size_bytes, status FROM (
                SELECT 'email' AS kind, id, openrag_filename, size_bytes, status
                FROM emails WHERE status IN ('downloading','ingesting')
                UNION ALL
                SELECT 'attachment' AS kind, id, openrag_filename, size_bytes, status
                FROM attachments WHERE status IN ('downloading','ingesting')
            )
            ORDER BY CASE status WHEN 'ingesting' THEN 0 ELSE 1 END,
                     openrag_filename, id
            """
        ).fetchall()
    return [
        {
            "kind": str(row["kind"]),
            "id": str(row["id"]),
            "name": str(row["openrag_filename"]),
            "size_bytes": max(0, int(row["size_bytes"])),
            "status": str(row["status"]),
        }
        for row in rows
    ]


# ---------------------------------------------------------------------------
# État mémoire, boucles de fond et cycle métier
# ---------------------------------------------------------------------------


class RuntimeState:
    """Petit état mémoire pour les probes et l'interface d'exploitation."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.changed = threading.Condition(self.lock)
        self._revision = 0
        self.csrf_token = uuid.uuid4().hex
        self.started_at = int(time.time())
        self.last_cycle_started_at = 0
        self.last_cycle_completed_at = 0
        self.cycle_requested_at = 0
        self.force_inventory_requested = False
        self.cycle_in_progress = False
        self.cycle_phase = ""
        self.progress_current = 0
        self.progress_total = 0
        self.last_error = ""
        self.last_scan: ScanResult | None = None
        self.last_processed = 0
        self.reconciliation_requested_at = 0
        self.reconciliation_started_at = 0
        self.reconciliation_completed_at = 0
        self.reconciliation_in_progress = False
        self.reconciliation_current = 0
        self.reconciliation_total = 0
        self.reconciliation_restored = 0
        self.reconciliation_lost = 0
        self.reconciliation_error = ""
        self.mails_per_minute = 0
        self.active_ingestions: list[dict[str, object]] = []
        self.ingestion_concurrency_effective = 0
        self.ready = False
        self.running = False

    def set_running(self, value: bool) -> None:
        with self.changed:
            self.running = value
            self._notify_changed()

    def _notify_changed(self) -> None:
        """Signale une mutation aux clients de suivi, verrou déjà acquis."""
        self._revision += 1
        self.changed.notify_all()

    def revision(self) -> int:
        with self.lock:
            return self._revision

    def wait_for_change(self, revision: int, timeout: float = 15) -> int:
        with self.changed:
            self.changed.wait_for(lambda: self._revision != revision, timeout)
            return self._revision

    def ingestion_concurrency_updated(self, effective: int) -> None:
        """Publie la taille effective du pool manuel dans l'état live."""

        with self.changed:
            self.ingestion_concurrency_effective = effective
            self._notify_changed()

    def restore_cycle(
        self,
        completed_at: int,
        scan: ScanResult | None,
        processed: int,
        error: str,
    ) -> None:
        with self.changed:
            self.last_cycle_completed_at = completed_at
            self.last_scan = scan
            self.last_processed = processed
            self.last_error = error
            self.ready = bool(completed_at and not error)
            self._notify_changed()

    def cycle_started(self) -> bool:
        with self.changed:
            force_inventory = self.force_inventory_requested
            self.last_cycle_started_at = int(time.time())
            self.cycle_requested_at = 0
            self.force_inventory_requested = False
            self.cycle_in_progress = True
            self.cycle_phase = "Démarrage de l’inventaire"
            self.progress_current = 0
            self.progress_total = 0
            self._notify_changed()
            return force_inventory

    def cycle_progress(
        self, phase: str, current: int | None = None, total: int | None = None
    ) -> None:
        with self.changed:
            if self.cycle_in_progress:
                self.cycle_phase = phase
                if current is not None:
                    self.progress_current = max(0, current)
                if total is not None:
                    self.progress_total = max(0, total)
                self._notify_changed()

    def cycle_requested(self, *, force_inventory: bool = False) -> None:
        with self.changed:
            self.cycle_requested_at = int(time.time())
            self.force_inventory_requested = (
                self.force_inventory_requested or force_inventory
            )
            self._notify_changed()

    def cycle_pending(self) -> bool:
        with self.lock:
            return bool(self.cycle_requested_at)

    def reconciliation_requested(self) -> bool:
        with self.changed:
            if self.reconciliation_requested_at or self.reconciliation_in_progress:
                return False
            self.reconciliation_requested_at = int(time.time())
            self.reconciliation_error = ""
            self._notify_changed()
            return True

    def reconciliation_pending(self) -> bool:
        with self.lock:
            return bool(self.reconciliation_requested_at)

    def reconciliation_started(self) -> None:
        with self.changed:
            self.reconciliation_requested_at = 0
            self.reconciliation_started_at = int(time.time())
            self.reconciliation_in_progress = True
            self.reconciliation_current = 0
            self.reconciliation_total = 0
            self.reconciliation_restored = 0
            self.reconciliation_lost = 0
            self.reconciliation_error = ""
            self._notify_changed()

    def reconciliation_progress(self, current: int, total: int) -> None:
        with self.changed:
            if self.reconciliation_in_progress:
                self.reconciliation_current = max(0, current)
                self.reconciliation_total = max(0, total)
                self._notify_changed()

    def reconciliation_succeeded(self, result: ReconciliationResult) -> None:
        with self.changed:
            self.reconciliation_completed_at = int(time.time())
            self.reconciliation_in_progress = False
            self.reconciliation_current = result.checked
            self.reconciliation_total = result.checked
            self.reconciliation_restored = result.restored
            self.reconciliation_lost = result.lost
            self.reconciliation_error = ""
            self._notify_changed()

    def reconciliation_failed(self, error: Exception) -> None:
        with self.changed:
            self.reconciliation_completed_at = int(time.time())
            self.reconciliation_in_progress = False
            self.reconciliation_error = _safe_error(error)
            self._notify_changed()

    def mail_rate_updated(self, mails_per_minute: int) -> None:
        """Publie le débit récent calculé dans la base locale."""
        with self.changed:
            value = max(0, mails_per_minute)
            if value != self.mails_per_minute:
                self.mails_per_minute = value
                self._notify_changed()

    def active_ingestions_updated(
        self, active_ingestions: Sequence[Mapping[str, object]]
    ) -> None:
        """Publie uniquement les changements de la liste des travaux actifs."""
        normalized = [dict(task) for task in active_ingestions]
        with self.changed:
            if normalized != self.active_ingestions:
                self.active_ingestions = normalized
                self._notify_changed()

    def cycle_succeeded(self, scan: ScanResult, processed: int) -> None:
        with self.changed:
            self.last_cycle_completed_at = int(time.time())
            self.last_error = ""
            self.last_scan = scan
            self.last_processed = processed
            self.ready = True
            self.cycle_in_progress = False
            self.cycle_phase = ""
            self._notify_changed()

    def cycle_failed(self, error: Exception) -> None:
        with self.changed:
            self.last_cycle_completed_at = int(time.time())
            self.last_error = _safe_error(error)
            self.ready = False
            self.cycle_in_progress = False
            self.cycle_phase = ""
            self._notify_changed()

    def snapshot(self) -> dict[str, object]:
        with self.lock:
            return {
                "csrf_token": self.csrf_token,
                "started_at": self.started_at,
                "last_cycle_started_at": self.last_cycle_started_at,
                "last_cycle_completed_at": self.last_cycle_completed_at,
                "cycle_requested_at": self.cycle_requested_at,
                "force_inventory_requested": self.force_inventory_requested,
                "cycle_in_progress": self.cycle_in_progress,
                "cycle_phase": self.cycle_phase,
                "progress_current": self.progress_current,
                "progress_total": self.progress_total,
                "last_error": self.last_error,
                "last_scan": self.last_scan,
                "last_processed": self.last_processed,
                "reconciliation_requested_at": self.reconciliation_requested_at,
                "reconciliation_started_at": self.reconciliation_started_at,
                "reconciliation_completed_at": self.reconciliation_completed_at,
                "reconciliation_in_progress": self.reconciliation_in_progress,
                "reconciliation_current": self.reconciliation_current,
                "reconciliation_total": self.reconciliation_total,
                "reconciliation_restored": self.reconciliation_restored,
                "reconciliation_lost": self.reconciliation_lost,
                "reconciliation_error": self.reconciliation_error,
                "mails_per_minute": self.mails_per_minute,
                "active_ingestions": [dict(task) for task in self.active_ingestions],
                "ingestion_concurrency_effective": self.ingestion_concurrency_effective,
                "ready": self.ready,
                "running": self.running,
            }


def run_cycle(
    config: Config,
    openarchiver: OpenArchiverClient,
    openrag: OpenRAGClient,
    progress: Callable[[str, int | None, int | None], None] | None = None,
    force_inventory: bool = True,
    state: RuntimeState | None = None,
) -> tuple[ScanResult, int]:
    """Construit/réutilise un inventaire valide puis traite la file locale.

    Un scan manuel force l'appel à OpenArchiver. Les cycles suivants peuvent
    réutiliser le dernier instantané complet : une pagination instable ne doit
    jamais faire disparaître des objets ni bloquer une file déjà connue.
    """

    report = progress or (lambda _phase, _current=None, _total=None: None)
    cached = cached_inventory(config)
    if force_inventory or cached is None:
        report("Actualisation des sources OpenArchiver", 0, 0)
        LOG.info("inventaire: actualisation des sources OpenArchiver")
        refresh_sources(config, openarchiver)
        report("Lecture des messages des sources sélectionnées", 0, 0)
        LOG.info("inventaire: lecture des sources sélectionnées")
        scan = scan_selected_sources(config, openarchiver, progress=report)
        if not scan.complete:
            if cached is None:
                with database(config) as db:
                    row = db.execute(
                        "SELECT value FROM settings WHERE key='last_scan_error'"
                    ).fetchone()
                detail = str(row[0]) if row else "inventaire incomplet"
                raise IncompleteScanError(f"{detail}; ingestion différée")
            scan, cached_at = cached
            report(
                "Inventaire instable — utilisation de l’instantané valide", 0, 0
            )
            LOG.warning(
                "inventaire instable; instantané valide du %s réutilisé",
                time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(cached_at)),
            )
    else:
        scan, cached_at = cached
        report("Utilisation de l’inventaire conservé", 0, 0)
        LOG.info(
            "inventaire valide du %s réutilisé",
            time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(cached_at)),
        )
    queue_total = selected_queue_pending_count(config)
    report("Traitement de l’ingestion OpenRAG", 0, queue_total)
    LOG.info(
        "inventaire terminé: sources=%d mails=%d; traitement de la file",
        scan.sources,
        scan.emails,
    )
    processed = process_queue(
        config,
        openarchiver,
        openrag,
        progress=lambda current, total: report(
            "Traitement de l’ingestion OpenRAG", current, total
        ),
        state=state,
    )
    return scan, processed


def persist_cycle_outcome(
    config: Config,
    *,
    completed_at: int,
    scan: ScanResult | None,
    processed: int,
    error: str,
) -> None:
    values = {
        "last_cycle_completed_at": str(completed_at),
        "last_cycle_sources": str(scan.sources if scan else 0),
        "last_cycle_emails": str(scan.emails if scan else 0),
        "last_cycle_processed": str(processed),
        "last_cycle_error": error,
    }
    with database(config) as db:
        db.executemany(
            "INSERT OR REPLACE INTO settings(key,value) VALUES (?,?)",
            values.items(),
        )


def restore_cycle_outcome(config: Config, state: RuntimeState) -> None:
    with database(config) as db:
        values = {
            str(row["key"]): str(row["value"])
            for row in db.execute(
                """SELECT key, value FROM settings
                   WHERE key IN (
                       'last_cycle_completed_at', 'last_cycle_sources',
                       'last_cycle_emails', 'last_cycle_processed',
                       'last_cycle_error'
                   )"""
            )
        }
    completed_at = int(values.get("last_cycle_completed_at", "0") or 0)
    if not completed_at:
        return
    error = values.get("last_cycle_error", "")
    scan = None
    if not error:
        scan = ScanResult(
            int(values.get("last_cycle_sources", "0") or 0),
            int(values.get("last_cycle_emails", "0") or 0),
            True,
            False,
        )
    state.restore_cycle(
        completed_at,
        scan,
        int(values.get("last_cycle_processed", "0") or 0),
        error,
    )


def runtime_loop(
    config: Config,
    state: RuntimeState,
    *,
    openarchiver: OpenArchiverClient | None = None,
    openrag: OpenRAGClient | None = None,
    stop: threading.Event = STOP,
    wake: threading.Event = WAKE,
) -> None:
    """Boucle principale : récupération, inventaire, ingestion puis attente."""

    archive_client = openarchiver or OpenArchiverClient(config)
    rag_client = openrag or OpenRAGClient(config)
    state.set_running(True)
    try:
        recovered = recover_interrupted(config)
        if recovered:
            LOG.warning("%d opération(s) interrompue(s) récupérée(s)", recovered)
        while not stop.is_set():
            paused = is_paused(config)
            if paused and not state.cycle_pending():
                if cached_inventory(config) is not None:
                    LOG.info("indexation en pause; inventaire IMAP conservé")
                    wake.wait(config.scan_interval_seconds)
                    wake.clear()
                    continue
                LOG.info("indexation en pause; inventaire IMAP initial")
            if paused:
                LOG.info("indexation en pause; inventaire IMAP manuel")
            force_inventory = state.cycle_started()
            try:
                scan, processed = run_cycle(
                    config,
                    archive_client,
                    rag_client,
                    progress=state.cycle_progress,
                    force_inventory=force_inventory,
                    state=state,
                )
                state.cycle_succeeded(scan, processed)
                completed_at = int(state.snapshot()["last_cycle_completed_at"])
                try:
                    persist_cycle_outcome(
                        config,
                        completed_at=completed_at,
                        scan=scan,
                        processed=processed,
                        error="",
                    )
                except sqlite3.Error as persistence_error:
                    LOG.error(
                        "persistance de l’état du cycle en échec: %s",
                        _safe_error(persistence_error),
                    )
                LOG.info(
                    "cycle terminé: sources=%d mails=%d traités=%d",
                    scan.sources,
                    scan.emails,
                    processed,
                )
                delay = config.scan_interval_seconds
            except Exception as error:
                state.cycle_failed(error)
                completed_at = int(state.snapshot()["last_cycle_completed_at"])
                try:
                    persist_cycle_outcome(
                        config,
                        completed_at=completed_at,
                        scan=None,
                        processed=0,
                        error=_safe_error(error),
                    )
                except sqlite3.Error as persistence_error:
                    LOG.error(
                        "persistance de l’échec du cycle impossible: %s",
                        _safe_error(persistence_error),
                    )
                LOG.error("cycle en échec: %s", _safe_error(error))
                delay = (
                    config.scan_interval_seconds
                    if isinstance(error, IncompleteScanError)
                    else config.cycle_retry_seconds
                )
            wake.wait(delay)
            wake.clear()
    finally:
        state.set_running(False)


def reconciliation_loop(
    config: Config,
    state: RuntimeState,
    *,
    openrag: OpenRAGClient | None = None,
    stop: threading.Event = STOP,
    wake: threading.Event = RECONCILE_WAKE,
    queue_wake: threading.Event = WAKE,
) -> None:
    """Exécute les audits OpenRAG demandés sans bloquer l'ingestion courante."""
    rag_client = openrag or OpenRAGClient(config)
    while not stop.is_set():
        wake.wait(1)
        wake.clear()
        if stop.is_set() or not state.reconciliation_pending():
            continue
        state.reconciliation_started()
        try:
            result = reconcile_openrag(
                config, rag_client, progress=state.reconciliation_progress
            )
            state.reconciliation_succeeded(result)
            if result.lost:
                state.cycle_requested()
                queue_wake.set()
            LOG.info(
                "réconciliation terminée: vérifiés=%d restaurés=%d perdus=%d",
                result.checked,
                result.restored,
                result.lost,
            )
        except Exception as error:
            state.reconciliation_failed(error)
            LOG.error("réconciliation OpenRAG en échec: %s", _safe_error(error))


def mail_rate_monitor_loop(
    config: Config,
    state: RuntimeState,
    *,
    stop: threading.Event = STOP,
    poll_seconds: float = MAIL_RATE_POLL_SECONDS,
) -> None:
    """Actualise le débit et les tâches actives depuis SQLite uniquement."""
    while not stop.is_set():
        try:
            state.mail_rate_updated(selected_mails_validated_last_minute(config))
            state.active_ingestions_updated(active_ingestion_tasks(config))
        except Exception as error:
            LOG.warning("actualisation de l'état d'ingestion impossible: %s", _safe_error(error))
        stop.wait(max(1.0, poll_seconds))


# ---------------------------------------------------------------------------
# Interface d'exploitation, événements SSE et métriques Prometheus
# ---------------------------------------------------------------------------


def _status_counts(
    config: Config, *, selected_only: bool = False
) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {"emails": {}, "attachments": {}}
    with database(config) as db:
        if selected_only:
            queries = {
                "emails": """SELECT e.status, COUNT(*) AS count
                    FROM sources s
                    JOIN mailboxes m ON m.source_id=s.id
                    JOIN emails e
                      ON e.source_id=m.source_id AND e.mailbox_path=m.path
                    WHERE s.selected=1 AND m.selected=1
                    GROUP BY e.status""",
                "attachments": """SELECT a.status,
                           COUNT(DISTINCT a.id) AS count
                    FROM sources s
                    JOIN mailboxes m ON m.source_id=s.id
                    JOIN emails e
                      ON e.source_id=m.source_id AND e.mailbox_path=m.path
                    JOIN email_attachments ea ON ea.email_id=e.id
                    JOIN attachments a ON a.id=ea.attachment_id
                    WHERE s.selected=1 AND m.selected=1
                    GROUP BY a.status""",
            }
        else:
            queries = {
                table: f"""SELECT status, COUNT(*) AS count FROM {table}
                             GROUP BY status"""
                for table in result
            }
        for table, query in queries.items():
            for row in db.execute(query):
                result[table][str(row["status"])] = int(row["count"])
    return result


def selected_mail_with_attachments(config: Config) -> int:
    with database(config) as db:
        row = db.execute(
            """SELECT COUNT(*)
               FROM sources s
               JOIN mailboxes m ON m.source_id=s.id
               JOIN emails e
                 ON e.source_id=m.source_id AND e.mailbox_path=m.path
               WHERE s.selected=1 AND m.selected=1 AND e.has_attachments=1"""
        ).fetchone()
    return int(row[0])


def cycle_stage(snapshot: Mapping[str, object]) -> str:
    if not bool(snapshot["cycle_in_progress"]):
        return "idle"
    phase = str(snapshot.get("cycle_phase") or "")
    if phase.startswith("Traitement de l’ingestion OpenRAG"):
        return "ingestion"
    return "inventory"


def inventory_status(snapshot: Mapping[str, object]) -> str:
    started_at = int(snapshot["last_cycle_started_at"])
    completed_at = int(snapshot["last_cycle_completed_at"])
    requested_at = int(snapshot["cycle_requested_at"])
    if bool(snapshot["cycle_in_progress"]):
        phase = str(snapshot.get("cycle_phase") or "Inventaire en cours")
        activity = (
            "Ingestion en cours"
            if cycle_stage(snapshot) == "ingestion"
            else "Inventaire en cours"
        )
        return (
            activity
            + " — "
            + phase
            + " — démarré le "
            + time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(started_at))
        )
    if requested_at:
        return "Inventaire demandé, en attente de démarrage"
    if completed_at:
        last_error = str(snapshot.get("last_error") or "")
        if last_error:
            return (
                "Inventaire interrompu le "
                + time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(completed_at))
                + " — "
                + last_error
            )
        last_scan = snapshot["last_scan"]
        detail = ""
        if isinstance(last_scan, ScanResult):
            detail = f" — {last_scan.sources} source(s), {last_scan.emails} mail(s)"
        return (
            "terminé le "
            + time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(completed_at))
            + detail
        )
    return "jamais exécuté depuis le démarrage"


OPENRAG_LOGO = """<svg class="brand-logo" viewBox="0 0 200 164" aria-hidden="true">
<path d="M136.91 6.07c.61 0 1.09.49 1.09 1.1v6.38c0 .6.49 1.09 1.09 1.09h16.32c.6 0 1.09.48 1.09 1.09v7.09c0 .61.48 1.09 1.09 1.09h7.07c.61 0 1.09.49 1.09 1.09v7.51c0 .6.49 1.09 1.09 1.09h7.89c.61 0 1.09.48 1.09 1.09v7.09c0 .61.49 1.09 1.09 1.09h7.08c.6 0 1.09.49 1.09 1.09v14.75c0 .61.48 1.09 1.09 1.09h6.68c.61 0 1.09.49 1.09 1.09V82.1c0 .6-.48 1.09-1.09 1.09h-6.68c-.61 0-1.09.48-1.09 1.09v7.5c0 .61-.49 1.09-1.09 1.09h-7.08c-.6 0-1.09.49-1.09 1.09v7.11c0 .61-.48 1.09-1.09 1.09h-5.08c-.6 0-1.09.49-1.09 1.09V118c0 .61-.48 1.09-1.08 1.09h-7.49c-.61 0-1.09-.48-1.09-1.09V40.69c0-.61-.48-1.09-1.09-1.09h-7.48c-.61 0-1.09.48-1.09 1.09v94.53c0 .6.48 1.09 1.09 1.09h3.46c.61 0 1.09.48 1.09 1.09v19.58c0 .61-.48 1.09-1.09 1.09H49.04c-.61 0-1.09-.48-1.09-1.09V137.4c0-.61.48-1.09 1.09-1.09h3.46c.61 0 1.09-.49 1.09-1.09V40.69c0-.61-.48-1.09-1.09-1.09h-7.48c-.61 0-1.09.48-1.09 1.09v77.22c0 .61-.49 1.09-1.09 1.09h-11.41c-.6 0-1.08-.48-1.08-1.09v-14.75c0-.6-.49-1.09-1.09-1.09h-5.07c-.6 0-1.09-.48-1.09-1.09v-7.11c0-.6-.48-1.09-1.09-1.09h-7.09c-.6 0-1.09-.48-1.09-1.09v-7.5c0-.61-.48-1.09-1.09-1.09H7.15c-.61 0-1.09-.49-1.09-1.1V60.8c0-.6.48-1.09 1.09-1.09h6.68c.61 0 1.09-.48 1.09-1.09V43.87c0-.6.49-1.09 1.09-1.09h7.09c.61 0 1.09-.48 1.09-1.09V34.6c0-.61.49-1.09 1.09-1.09h7.88c.6 0 1.09-.49 1.09-1.1v-7.5c0-.6.48-1.09 1.09-1.09h7.07c.61 0 1.09-.48 1.09-1.09v-7.09c0-.61.49-1.09 1.09-1.09h16.33c.61 0 1.09-.49 1.09-1.09V7.17c0-.61.49-1.1 1.09-1.1h73.81ZM72.34 51.47c-.6 0-1.09.48-1.09 1.09v9.11c0 .6.5 1.09 1.09 1.09h9.09c.6 0 1.09-.5 1.09-1.09v-9.11c0-.61-.49-1.09-1.09-1.09h-9.09Zm46.64 0c-.61 0-1.09.48-1.09 1.09v9.11c0 .6.48 1.09 1.09 1.09h9.09c.59 0 1.07-.5 1.07-1.09v-9.11c0-.61-.48-1.09-1.07-1.09h-9.09Zm-31.79 29.79c-.6 0-1.09.49-1.09 1.09v12.73c0 .61.49 1.1 1.09 1.1h8.17v15.03l-3.89 5.08H79.96c-.6 0-1.08.48-1.08 1.09v16.35c0 .61.48 1.09 1.08 1.09h3.24c-.18.2-.3.46-.3.75v2.25c0 .61.5 1.09 1.09 1.09h6.03c-.61 0-1.09.49-1.09 1.08v2.26c0 .61.48 1.09 1.09 1.09h20.34c.6 0 1.09-.48 1.09-1.09v-2.26c0-.59-.5-1.08-1.09-1.08h6.03c.61 0 1.09-.5 1.09-1.09v-2.25c0-.29-.12-.55-.3-.75h3.24c.6 0 1.08-.48 1.08-1.09v-16.35c0-.61-.48-1.09-1.08-1.09h-11.24l-4.16-5.22V96.19h8.17c.6 0 1.08-.48 1.08-1.09V82.37c0-.61-.48-1.09-1.08-1.09l-.02-.02H87.19Z"/>
</svg>"""


STATUS_PAGE_STYLE = """
:root{color-scheme:light dark;--background:#fff;--foreground:#09090b;--muted:#f4f4f5;--muted-foreground:#71717a;--border:#e4e4e7;--card:#fff;--sidebar:#fafafa;--primary:#09090b;--primary-foreground:#fff;--danger:#dc2626;--danger-soft:#fef2f2;--success:#059669;--success-soft:#ecfdf5;--warning:#b45309;--warning-soft:#fffbeb;--radius:8px;--shadow:0 1px 2px rgba(0,0,0,.04)}
*{box-sizing:border-box}html{background:var(--background)}body{margin:0;background:var(--background);color:var(--foreground);font:14px/1.5 Inter,ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}button,input{font:inherit}button,input[type=text],input[type=password]{min-height:40px}button{display:inline-flex;align-items:center;justify-content:center;gap:8px;border:1px solid var(--border);border-radius:var(--radius);background:var(--card);color:var(--foreground);padding:9px 14px;font-weight:600;cursor:pointer;transition:background .15s,border-color .15s,transform .05s}button:hover{background:var(--muted)}button:active{transform:translateY(1px)}button:focus-visible,input:focus-visible{outline:2px solid var(--foreground);outline-offset:2px}.primary{border-color:var(--primary);background:var(--primary);color:var(--primary-foreground)}.primary:hover{background:#27272a}
.app{min-height:100vh}.topbar{display:flex;align-items:center;justify-content:space-between;height:64px;border-bottom:1px solid var(--border);background:color-mix(in srgb,var(--background) 94%,transparent);padding:0 24px;position:sticky;top:0;z-index:2;backdrop-filter:blur(10px)}.brand{display:flex;align-items:center;gap:10px;font:600 18px/1 ui-monospace,SFMono-Regular,Menlo,monospace}.brand-logo{width:24px;height:22px;fill:currentColor}.connector-chip{border:1px solid var(--border);border-radius:999px;padding:5px 10px;color:var(--muted-foreground);font-size:12px}.user-menu{display:flex;align-items:center;gap:9px}.user-menu form{margin:0}.user-menu button{min-height:32px;padding:5px 10px;font-size:12px}.main{min-width:0;padding:38px 24px}.content{max-width:1120px;margin:0 auto}.page-heading{margin-bottom:24px}.eyebrow{margin:0 0 4px;color:var(--muted-foreground);font-size:12px;font-weight:600}.page-heading h1{margin:0;font-size:26px;line-height:1.25;letter-spacing:-.025em}.page-heading p{margin:7px 0 0;color:var(--muted-foreground)}.section-heading{display:flex;align-items:flex-start;justify-content:space-between;gap:20px;margin-bottom:18px}.section-heading h2{margin:0;font-size:18px;letter-spacing:-.015em}.section-heading p{margin:4px 0 0;color:var(--muted-foreground)}.toolbar{display:flex;align-items:center;flex-wrap:wrap;gap:8px}.toolbar form{margin:0}
.workspace-tabs{display:flex;gap:4px;width:max-content;max-width:100%;margin-bottom:28px;border:1px solid var(--border);border-radius:10px;background:var(--muted);padding:4px;overflow-x:auto}.workspace-tab{min-height:38px;border:0;background:transparent;padding:8px 15px;color:var(--muted-foreground);white-space:nowrap;box-shadow:none}.workspace-tab:hover{background:color-mix(in srgb,var(--card) 55%,transparent);color:var(--foreground)}.workspace-tab[aria-selected="true"]{background:var(--card);color:var(--foreground);box-shadow:var(--shadow)}.workspace-panel[hidden]{display:none}.workspace-panel{animation:panel-in .14s ease-out}@keyframes panel-in{from{opacity:.55;transform:translateY(2px)}to{opacity:1;transform:none}}
.status-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:20px}.stat-card,.card{border:1px solid var(--border);border-radius:var(--radius);background:var(--card);box-shadow:var(--shadow)}.stat-card{padding:16px}.stat-label{display:flex;align-items:center;gap:7px;color:var(--muted-foreground);font-size:12px;font-weight:600}.dot{width:8px;height:8px;border-radius:50%;background:var(--muted-foreground)}.dot.success{background:var(--success)}.dot.warning{background:#f59e0b}.dot.danger{background:var(--danger)}.stat-value{display:block;margin-top:8px;font-size:22px;font-weight:650;letter-spacing:-.03em}.stat-detail{display:block;margin-top:2px;color:var(--muted-foreground);font-size:12px}.card{margin-bottom:16px;overflow:hidden}.card-header{display:flex;align-items:flex-start;justify-content:space-between;gap:16px;padding:18px 20px;border-bottom:1px solid var(--border)}.card-title{margin:0;font-size:15px}.card-description{margin:3px 0 0;color:var(--muted-foreground);font-size:13px}.card-body{padding:20px}.card-footer{display:flex;justify-content:flex-end;padding:14px 20px;border-top:1px solid var(--border);background:var(--sidebar)}.badge{display:inline-flex;align-items:center;border-radius:999px;background:var(--muted);padding:3px 8px;color:var(--muted-foreground);font-size:11px;font-weight:600}.badge.success{background:var(--success-soft);color:var(--success)}.badge.warning{background:var(--warning-soft);color:var(--warning)}.badge.danger{background:var(--danger-soft);color:var(--danger)}
.active-task-list{display:grid}.active-task-row{display:flex;align-items:center;justify-content:space-between;gap:18px;padding:13px 0;border-bottom:1px solid var(--border)}.active-task-row:first-child{padding-top:0}.active-task-row:last-child{padding-bottom:0;border-bottom:0}.active-task-document{min-width:0;display:grid;gap:2px}.active-task-document strong{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.active-task-document span{color:var(--muted-foreground);font-size:12px}.active-task-row>.badge{flex:0 0 auto}.active-task-empty{margin:0}
.inventory-row{display:flex;align-items:center;gap:12px}.inventory-status{display:block;width:100%;min-height:24px;font-weight:600}.inventory-status.running{color:var(--success)}.progress-wrap{margin-top:12px}.progress-track{height:10px;overflow:hidden;border-radius:999px;background:var(--muted)}.progress-bar{height:100%;width:0;border-radius:inherit;background:var(--success);transition:width .25s ease}.progress-bar.indeterminate{width:35%;animation:progress-slide 1.2s ease-in-out infinite}.progress-label{display:block;margin-top:5px;color:var(--muted-foreground);font-size:12px;text-align:right}@keyframes progress-slide{0%{transform:translateX(-110%)}100%{transform:translateX(300%)}}.helper{margin:10px 0 0;color:var(--muted-foreground);font-size:12px}.error-alert{display:flex;gap:10px;margin-bottom:16px;border:1px solid #fecaca;border-radius:var(--radius);background:var(--danger-soft);padding:13px 15px;color:#991b1b}.error-alert strong{display:block}.selection-list{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;max-height:390px;overflow:auto}.selection-item{display:flex;align-items:flex-start;gap:11px;margin:0;border:1px solid var(--border);border-radius:var(--radius);padding:12px;cursor:pointer;transition:background .15s,border-color .15s}.selection-item:hover{background:var(--muted)}.selection-item:has(input:checked){border-color:#a1a1aa;background:var(--muted)}.selection-item input{width:16px;height:16px;margin:2px 0 0;accent-color:var(--primary);flex:0 0 auto}.selection-copy{min-width:0}.selection-title{display:block;font-weight:600;overflow-wrap:anywhere}.selection-meta{display:block;margin-top:2px;color:var(--muted-foreground);font-size:12px;overflow-wrap:anywhere}.empty{grid-column:1/-1;margin:0;border:1px dashed var(--border);border-radius:var(--radius);padding:22px;text-align:center;color:var(--muted-foreground)}.counts{display:flex;flex-wrap:wrap;gap:6px}.secret-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}.secret-field{display:grid;gap:6px;color:var(--muted-foreground);font-size:12px;font-weight:600}.secret-current{font-weight:400}.secret-current code{display:inline-block;border-radius:5px;background:var(--muted);padding:2px 6px;color:var(--foreground);font:12px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace}.secret-missing{font-weight:400}.secret-field input{width:100%;border:1px solid var(--border);border-radius:var(--radius);background:var(--background);color:var(--foreground);padding:8px 10px}.footer-note{padding:8px 0 24px;text-align:center;color:var(--muted-foreground);font-size:12px}
.reconciliation-row{display:flex;align-items:center;justify-content:space-between;gap:16px}.reconciliation-row form{flex:0 0 auto}.section-rule{margin:18px 0;border:0;border-top:1px solid var(--border)}.retry-tabs{position:relative}.tab-toggle{position:absolute;opacity:0;pointer-events:none}.tab-label{display:inline-flex;margin:0 6px 14px 0;border:1px solid var(--border);border-radius:999px;padding:7px 12px;color:var(--muted-foreground);cursor:pointer;font-weight:600}.tab-panel{display:none}.retry-actions{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-top:14px}.retry-select-all{display:flex;align-items:center;gap:8px;color:var(--muted-foreground);font-size:12px}.retry-select-all input{width:16px;height:16px;accent-color:var(--primary)}#retry-tab-lost:checked~label[for="retry-tab-lost"],#retry-tab-failed:checked~label[for="retry-tab-failed"]{border-color:var(--primary);background:var(--primary);color:var(--primary-foreground)}#retry-tab-lost:checked~.tab-panels>#retry-panel-lost,#retry-tab-failed:checked~.tab-panels>#retry-panel-failed{display:block}
.login-page{min-height:100vh;display:grid;place-items:center;padding:24px;background:var(--muted)}.login-card{width:min(440px,100%);border:1px solid var(--border);border-radius:14px;background:var(--card);padding:42px;box-shadow:var(--shadow);text-align:center}.login-card .brand-logo{width:50px;height:42px}.login-card h1{margin:24px 0 8px;font-size:24px}.login-card p{margin:0 0 28px;color:var(--muted-foreground)}.login-card form{margin:0}.login-card button{width:100%;min-height:46px}.identity-notice{margin-bottom:16px;border:1px solid var(--border);border-radius:var(--radius);background:var(--sidebar);padding:11px 14px;color:var(--muted-foreground);font-size:12px}
@media(prefers-color-scheme:dark){:root{--background:#18181b;--foreground:#fafafa;--muted:#27272a;--muted-foreground:#a1a1aa;--border:#3f3f46;--card:#18181b;--sidebar:#111113;--primary:#fafafa;--primary-foreground:#09090b;--danger:#f87171;--danger-soft:#2b1719;--success:#34d399;--success-soft:#10251e;--warning:#fbbf24;--warning-soft:#2b2414;--shadow:none}.primary:hover{background:#e4e4e7}.error-alert{border-color:#7f1d1d;color:#fecaca}.selection-item:has(input:checked){border-color:#71717a}}
@media(max-width:900px){.main{padding:28px 18px}.status-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.secret-grid{grid-template-columns:1fr}}
@media(max-width:620px){.topbar{height:58px;padding:0 16px}.connector-chip{display:none}.main{padding:24px 14px}.page-heading h1{font-size:23px}.section-heading{align-items:stretch;flex-direction:column}.toolbar button{width:100%}.toolbar form{display:flex;width:100%}.workspace-tabs{width:100%;margin-bottom:22px}.workspace-tab{flex:1;padding:8px 11px}.selection-list{grid-template-columns:1fr}.status-grid{grid-template-columns:1fr 1fr}.stat-card{padding:13px}.stat-value{font-size:19px}.card-header,.card-body{padding:16px}.card-footer{padding:12px 16px}.card-footer button{width:100%}.active-task-row{align-items:flex-start;flex-direction:column;gap:7px}.active-task-document{width:100%}.reconciliation-row{align-items:stretch;flex-direction:column}.reconciliation-row button{width:100%}}
"""


def render_inventory_status_page(state: RuntimeState) -> str:
    snapshot = state.snapshot()
    running = bool(snapshot["cycle_in_progress"])
    status = html.escape(inventory_status(snapshot))
    body_class = "running" if running else ""
    return f"""<!doctype html><html lang="fr"><head><meta charset="utf-8">
<meta http-equiv="refresh" content="3">
<meta name="color-scheme" content="light dark">
<style>:root{{color-scheme:light dark}}body{{font:600 13px/24px Inter,ui-sans-serif,system-ui,sans-serif;margin:0;color:#09090b;background:transparent;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}body.running{{color:#059669}}body.running::before{{content:"";display:inline-block;width:8px;height:8px;margin-right:8px;border-radius:50%;background:#10b981;animation:pulse 1.2s ease-in-out infinite}}@keyframes pulse{{50%{{opacity:.3;transform:scale(.75)}}}}@media(prefers-color-scheme:dark){{body{{color:#fafafa}}body.running{{color:#34d399}}}}</style>
</head><body class="{body_class}" aria-live="polite" aria-busy="{str(running).lower()}">{status}</body></html>"""


def render_live_status(state: RuntimeState, config: Config | None = None) -> str:
    snapshot = state.snapshot()
    active = (
        active_ingestion_tasks(config)
        if config is not None
        else list(snapshot["active_ingestions"])
    )
    return json.dumps(
        {
            "csrf_token": str(snapshot["csrf_token"]),
            "inventory_status": inventory_status(snapshot),
            "cycle_in_progress": bool(snapshot["cycle_in_progress"]),
            "cycle_stage": cycle_stage(snapshot),
            "cycle_requested": bool(snapshot["cycle_requested_at"]),
            "cycle_completed_at": int(snapshot["last_cycle_completed_at"]),
            "last_error": str(snapshot["last_error"] or ""),
            "ready": bool(snapshot["ready"]),
            "last_processed": int(snapshot["last_processed"]),
            "progress_current": int(snapshot["progress_current"]),
            "progress_total": int(snapshot["progress_total"]),
            "reconciliation_requested": bool(snapshot["reconciliation_requested_at"]),
            "reconciliation_in_progress": bool(snapshot["reconciliation_in_progress"]),
            "reconciliation_completed_at": int(
                snapshot["reconciliation_completed_at"]
            ),
            "reconciliation_current": int(snapshot["reconciliation_current"]),
            "reconciliation_total": int(snapshot["reconciliation_total"]),
            "reconciliation_restored": int(snapshot["reconciliation_restored"]),
            "reconciliation_lost": int(snapshot["reconciliation_lost"]),
            "reconciliation_error": str(snapshot["reconciliation_error"] or ""),
            "mails_per_minute": int(snapshot["mails_per_minute"]),
            "active_ingestions": active,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ) + "\n"


UI_SCRIPT = r"""(() => {
  const workspaceTabs = Array.from(document.querySelectorAll("[data-workspace-tab]"));
  const workspacePanels = Array.from(document.querySelectorAll("[data-workspace-panel]"));
  const availableTabs = workspaceTabs.map(tab => tab.dataset.workspaceTab);
  const selectWorkspaceTab = (name, focus = false) => {
    if (!availableTabs.includes(name)) name = "ingestion";
    workspaceTabs.forEach(tab => {
      const selected = tab.dataset.workspaceTab === name;
      tab.setAttribute("aria-selected", selected ? "true" : "false");
      tab.tabIndex = selected ? 0 : -1;
      if (selected && focus) tab.focus();
    });
    workspacePanels.forEach(panel => {
      panel.hidden = panel.dataset.workspacePanel !== name;
    });
    try {
      window.localStorage.setItem("openarchiver-connector-tab", name);
    } catch (_error) {
      // La navigation reste fonctionnelle si le stockage local est bloqué.
    }
    window.history.replaceState(
      null,
      "",
      window.location.pathname + window.location.search + "#" + name
    );
  };
  if (workspaceTabs.length && workspacePanels.length) {
    let initialTab = window.location.hash.slice(1);
    if (!availableTabs.includes(initialTab)) {
      try {
        initialTab = window.localStorage.getItem("openarchiver-connector-tab") || "";
      } catch (_error) {
        initialTab = "";
      }
    }
    selectWorkspaceTab(initialTab || "ingestion");
    workspaceTabs.forEach((tab, index) => {
      tab.addEventListener("click", () => selectWorkspaceTab(tab.dataset.workspaceTab));
      tab.addEventListener("keydown", event => {
        if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
        event.preventDefault();
        let next = index;
        if (event.key === "ArrowLeft") next = (index - 1 + workspaceTabs.length) % workspaceTabs.length;
        if (event.key === "ArrowRight") next = (index + 1) % workspaceTabs.length;
        if (event.key === "Home") next = 0;
        if (event.key === "End") next = workspaceTabs.length - 1;
        selectWorkspaceTab(workspaceTabs[next].dataset.workspaceTab, true);
      });
    });
  }
  const badge = document.getElementById("inventory-badge");
  const summary = document.getElementById("inventory-summary");
  const dot = document.getElementById("inventory-dot");
  const button = document.getElementById("inventory-button");
  const completion = document.getElementById("inventory-completion");
  const service = document.getElementById("service-status");
  const lastSync = document.getElementById("last-sync");
  const processed = document.getElementById("last-processed");
  const progressWrap = document.getElementById("cycle-progress");
  const progressBar = document.getElementById("cycle-progress-bar");
  const progressLabel = document.getElementById("cycle-progress-label");
  const inventoryProgressWrap = document.getElementById("inventory-cycle-progress");
  const inventoryProgressBar = document.getElementById("inventory-cycle-progress-bar");
  const inventoryProgressLabel = document.getElementById("inventory-cycle-progress-label");
  const mailRate = document.getElementById("mail-rate");
  const activeTaskCount = document.getElementById("active-task-count");
  const activeTaskList = document.getElementById("active-task-list");
  const reconciliationButton = document.getElementById("reconciliation-button");
  const reconciliationStatus = document.getElementById("reconciliation-status");
  const reconciliationDetail = document.getElementById("reconciliation-detail");
  const reconciliationProgress = document.getElementById("reconciliation-progress");
  const reconciliationBar = document.getElementById("reconciliation-progress-bar");
  const reconciliationLabel = document.getElementById("reconciliation-progress-label");
  if (!badge || !summary || !dot || !button || !completion) return;
  if (new URLSearchParams(window.location.search).get("form") === "expired") {
    const alert = document.createElement("div");
    alert.className = "error-alert";
    alert.setAttribute("role", "alert");
    alert.textContent = "L’interface a été renouvelée après un redémarrage. Recommencez l’action.";
    document.querySelector(".content")?.prepend(alert);
    window.history.replaceState(null, "", window.location.pathname + window.location.hash);
  }
  let observedActive = document.body.dataset.cycleActive === "true";
  let observedStage = document.body.dataset.cycleStage || "idle";
  let observedReconciliation = document.body.dataset.reconciliationActive === "true";
  const formatFileSize = raw => {
    let value = Math.max(0, Number(raw) || 0);
    const units = ["o", "Kio", "Mio", "Gio"];
    let unit = units[0];
    for (let index = 0; index < units.length; index += 1) {
      unit = units[index];
      if (value < 1024 || index === units.length - 1) break;
      value /= 1024;
    }
    const precision = unit === "o" || value >= 10 ? 0 : 1;
    return value.toFixed(precision) + " " + unit;
  };
  const renderActiveTasks = rawTasks => {
    if (!activeTaskList || !activeTaskCount) return;
    const tasks = Array.isArray(rawTasks) ? rawTasks : [];
    activeTaskCount.textContent = tasks.length + " active(s)";
    const content = document.createDocumentFragment();
    if (!tasks.length) {
      const empty = document.createElement("p");
      empty.className = "empty active-task-empty";
      empty.textContent = "Aucune tâche en cours.";
      content.append(empty);
    }
    tasks.forEach(task => {
      const row = document.createElement("div");
      row.className = "active-task-row";
      const documentBlock = document.createElement("div");
      documentBlock.className = "active-task-document";
      const name = document.createElement("strong");
      name.textContent = String(task.name || "Document sans nom");
      name.title = name.textContent;
      const detail = document.createElement("span");
      detail.textContent = (task.kind === "email" ? "Mail" : "Pièce jointe") +
        " · " + formatFileSize(task.size_bytes);
      const status = document.createElement("span");
      const ingesting = task.status === "ingesting";
      status.className = "badge " + (ingesting ? "success" : "warning");
      status.textContent = ingesting ? "Ingestion OpenRAG" : "Téléchargement";
      documentBlock.append(name, detail);
      row.append(documentBlock, status);
      content.append(row);
    });
    activeTaskList.replaceChildren(content);
  };
  const refreshInventoryDisplay = async () => {
    const response = await fetch("/?inventory-fragment=1", {cache: "no-store"});
    if (!response.ok) throw new Error("actualisation de l’inventaire impossible");
    const fresh = new DOMParser().parseFromString(await response.text(), "text/html");
    const currentMailboxList = document.getElementById("mailbox-selection-list");
    const freshMailboxList = fresh.getElementById("mailbox-selection-list");
    if (currentMailboxList && freshMailboxList) {
      const choices = new Map(Array.from(
        currentMailboxList.querySelectorAll('input[name="mailbox"]')
      ).map(input => [input.value, input.checked]));
      freshMailboxList.querySelectorAll('input[name="mailbox"]').forEach(input => {
        if (choices.has(input.value)) input.checked = choices.get(input.value);
      });
      currentMailboxList.innerHTML = freshMailboxList.innerHTML;
    }
    [
      "mailbox-selection-badge",
      "mail-count",
      "mailbox-selected-count",
      "inventory-cache-label",
      "email-status-counts",
      "history-summary",
      "attachment-status-counts"
    ].forEach(id => {
      const current = document.getElementById(id);
      const replacement = fresh.getElementById(id);
      if (current && replacement) current.innerHTML = replacement.innerHTML;
    });
  };
  const bindRetrySelectors = () => {
    document.querySelectorAll(".retry-select-all input").forEach(control => {
      if (control.dataset.bound === "true") return;
      control.dataset.bound = "true";
      control.addEventListener("change", () => {
        const status = control.dataset.retryStatus;
        document.querySelectorAll(`input[data-retry-object="${status}"]`).forEach(input => {
          input.checked = control.checked;
        });
      });
    });
  };
  const refreshRetryDisplay = async () => {
    const response = await fetch("/", {cache: "no-store"});
    if (!response.ok) throw new Error("actualisation de la réindexation impossible");
    const fresh = new DOMParser().parseFromString(await response.text(), "text/html");
    ["retry-count-badge", "retry-tabs"].forEach(id => {
      const current = document.getElementById(id);
      const replacement = fresh.getElementById(id);
      if (current && replacement) current.innerHTML = replacement.innerHTML;
    });
    bindRetrySelectors();
  };
  const applyStatus = async status => {
      if (typeof status.csrf_token === "string" && status.csrf_token) {
        document.querySelectorAll('input[name="csrf"]').forEach(input => {
          input.value = status.csrf_token;
        });
      }
      const active = Boolean(status.cycle_in_progress);
      const requested = Boolean(status.cycle_requested);
      const failed = Boolean(status.last_error);
      const ingestion = status.cycle_stage === "ingestion";
      observedActive = observedActive || active || requested;
      if (active) observedStage = status.cycle_stage;
      if (active || requested) completion.hidden = true;
      summary.textContent = active && ingestion ?
        "Inventaire terminé ; ingestion OpenRAG en cours." : status.inventory_status;
      summary.setAttribute("aria-busy", active && !ingestion ? "true" : "false");
      summary.classList.toggle("running", active && !ingestion);
      const stateClass = active && ingestion ? "success" :
        (active ? "success" : (failed ? "danger" : "warning"));
      badge.className = "badge " + stateClass;
      dot.className = "dot " + stateClass;
      badge.textContent = active ? (ingestion ? "Inventaire terminé" : "Inventaire en cours…") :
        (requested ? "Inventaire demandé" :
          (failed ? "Inventaire interrompu" : "En attente"));
      button.disabled = active || requested;
      button.type = active || requested ? "button" : "submit";
      button.textContent = active ? (ingestion ? "Cycle en cours…" : "Inventaire en cours…") :
        (requested ? "Inventaire demandé…" : "Relancer l’inventaire");
      if (service) service.textContent = status.ready ? "Prêt" : "Attention requise";
      if (lastSync) {
        lastSync.textContent = status.cycle_completed_at ?
          "Dernière synchro : " + new Date(status.cycle_completed_at * 1000).toLocaleString("fr-FR", {timeZone: "UTC"}) + " UTC" :
          "Dernière synchro : Jamais exécutée";
      }
      if (processed) processed.textContent = status.last_processed + " objet(s) au dernier cycle";
      if (mailRate) mailRate.textContent = String(Number(status.mails_per_minute || 0));
      renderActiveTasks(status.active_ingestions);
      const updateProgress = (wrap, bar, label, visible) => {
        if (!wrap || !bar || !label) return;
        const current = Number(status.progress_current || 0);
        const total = Number(status.progress_total || 0);
        wrap.hidden = !visible;
        bar.classList.toggle("indeterminate", visible && total <= 0);
        bar.style.width = total > 0 ? Math.min(100, current * 100 / total) + "%" : "";
        label.textContent = total > 0 ? current + " / " + total + " · " + Math.round(current * 100 / total) + " %" : "Préparation…";
        wrap.setAttribute("aria-valuemin", "0");
        if (total > 0) {
          wrap.setAttribute("aria-valuemax", String(total));
          wrap.setAttribute("aria-valuenow", String(Math.min(current, total)));
        } else {
          wrap.removeAttribute("aria-valuemax");
          wrap.removeAttribute("aria-valuenow");
        }
      };
      updateProgress(progressWrap, progressBar, progressLabel, active && ingestion);
      updateProgress(inventoryProgressWrap, inventoryProgressBar, inventoryProgressLabel, active && !ingestion);
      const reconciling = Boolean(status.reconciliation_in_progress);
      const reconciliationRequested = Boolean(status.reconciliation_requested);
      const reconciliationActive = reconciling || reconciliationRequested;
      const reconciliationCompleted = Boolean(status.reconciliation_completed_at);
      observedReconciliation = observedReconciliation || reconciliationActive;
      if (reconciliationButton) {
        reconciliationButton.disabled = reconciliationActive;
        reconciliationButton.type = reconciliationActive ? "button" : "submit";
        reconciliationButton.textContent = reconciling ? "Réconciliation en cours…" :
          (reconciliationRequested ? "Réconciliation demandée…" : "Réconcilier avec OpenRAG");
      }
      if (reconciliationStatus) {
        reconciliationStatus.textContent = reconciling ? "Réconciliation en cours…" :
          (reconciliationRequested ? "Réconciliation demandée…" :
            (status.reconciliation_error ? "Réconciliation interrompue" :
              (reconciliationCompleted ? "Réconciliation terminée" : "Aucune réconciliation lancée")));
      }
      if (reconciliationDetail) {
        reconciliationDetail.textContent = status.reconciliation_error ||
          (reconciliationActive ? "Comparaison des connaissances locales avec OpenRAG." :
            (reconciliationCompleted ?
              `${status.reconciliation_current} vérifié(s) · ${status.reconciliation_restored} restauré(s) · ${status.reconciliation_lost} perdu(s) détecté(s)` :
              "Le scan compare les documents validés, lost et failed avec les chunks OpenRAG."));
      }
      if (reconciliationProgress && reconciliationBar && reconciliationLabel) {
        const current = Number(status.reconciliation_current || 0);
        const total = Number(status.reconciliation_total || 0);
        reconciliationProgress.hidden = !reconciliationActive;
        reconciliationBar.classList.toggle("indeterminate", reconciliationActive && total <= 0);
        reconciliationBar.style.width = total > 0 ? Math.min(100, current * 100 / total) + "%" : "";
        reconciliationLabel.textContent = total > 0 ? current + " / " + total + " · " + Math.round(current * 100 / total) + " %" : "Préparation…";
      }
      if (observedReconciliation && !reconciliationActive) {
        await refreshRetryDisplay();
        observedReconciliation = false;
      }
      if (observedActive && !active && !requested) {
        await refreshInventoryDisplay();
        completion.hidden = false;
        completion.textContent = failed ?
          "Cycle interrompu. Le détail est affiché ci-dessus ; vos champs et sélections ont été conservés." :
          (observedStage === "ingestion" ?
            "Ingestion terminée. Les compteurs ont été actualisés ; vos champs et sélections ont été conservés." :
            "Inventaire terminé. Les dossiers et compteurs ont été actualisés ; vos champs et sélections ont été conservés.");
        observedActive = false;
        observedStage = "idle";
      }
  };
  const update = async () => {
    try {
      const response = await fetch("/status.json", {cache: "no-store"});
      if (!response.ok) return;
      await applyStatus(await response.json());
    } catch (_error) {
      // Le flux SSE ou la prochaine vérification reprendra automatiquement.
    }
  };
  bindRetrySelectors();
  if (window.EventSource) {
    const events = new EventSource("/events");
    events.addEventListener("status", event => {
      try {
        void applyStatus(JSON.parse(event.data)).catch(() => {});
      } catch (_error) {
        // Un événement suivant remplacera un message incomplet.
      }
    });
  } else {
    update();
  }
  window.setInterval(update, 30000);
})();
"""


def render_login_page(state: RuntimeState, error: str = "") -> str:
    """Page de connexion calquée sur le parcours Google OAuth d'OpenRAG."""
    csrf = html.escape(str(state.snapshot()["csrf_token"]), quote=True)
    error_alert = (
        f'<div class="error-alert" role="alert">{html.escape(error)}</div>'
        if error
        else ""
    )
    return f"""<!doctype html><html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light dark"><title>Connexion · OpenRAG</title>
<style>{STATUS_PAGE_STYLE}</style></head><body><main class="login-page">
<section class="login-card">{OPENRAG_LOGO}<h1>Connexion au connecteur</h1>
<p>Utilisez la même identité Google que dans OpenRAG.</p>{error_alert}
<form method="post" action="/auth/login"><input type="hidden" name="csrf" value="{csrf}">
<button class="primary" type="submit">Continuer avec Google</button></form>
</section></main></body></html>"""


def format_file_size(size_bytes: int) -> str:
    """Formate une taille de fichier de manière compacte pour l'exploitation."""
    value = float(max(0, size_bytes))
    units = ("o", "Kio", "Mio", "Gio")
    for unit in units:
        if value < 1024 or unit == units[-1]:
            precision = 0 if unit == "o" or value >= 10 else 1
            return f"{value:.{precision}f} {unit}"
        value /= 1024
    raise AssertionError("unité de taille inaccessible")


def render_active_ingestion_tasks(tasks: Sequence[Mapping[str, object]]) -> str:
    """Construit la liste initiale ; JavaScript reprend ensuite les mises à jour."""
    if not tasks:
        return '<p class="empty active-task-empty">Aucune tâche en cours.</p>'
    lines = []
    for task in tasks:
        kind = str(task.get("kind", ""))
        status = str(task.get("status", ""))
        kind_label = "Mail" if kind == "email" else "Pièce jointe"
        status_label = (
            "Ingestion OpenRAG" if status == "ingesting" else "Téléchargement"
        )
        status_class = "success" if status == "ingesting" else "warning"
        lines.append(
            '<div class="active-task-row">'
            '<div class="active-task-document">'
            f'<strong title="{html.escape(str(task.get("name", "")), quote=True)}">'
            f'{html.escape(str(task.get("name", "")))}</strong>'
            f'<span>{html.escape(kind_label)} · '
            f'{html.escape(format_file_size(int(task.get("size_bytes", 0))))}</span>'
            "</div>"
            f'<span class="badge {status_class}">{html.escape(status_label)}</span>'
            "</div>"
        )
    return "".join(lines)


def render_status_page(
    config: Config,
    state: RuntimeState,
    principal: ConnectorPrincipal | None = None,
) -> str:
    snapshot = state.snapshot()
    sources = source_rows(config)
    mailboxes = mailbox_rows(config)
    counts = _status_counts(config)
    selected_counts = _status_counts(config, selected_only=True)
    active_tasks = active_ingestion_tasks(config)
    inventory_cache = cached_inventory(config, allow_expired=True)
    paused = is_paused(config)
    csrf = html.escape(str(snapshot["csrf_token"]), quote=True)
    if principal is not None and principal.authenticated:
        user_label = html.escape(
            principal.name or principal.email or principal.user_id
        )
        identity_menu = (
            f'<div class="user-menu"><span class="connector-chip">{user_label}</span>'
            f'<form method="post" action="/auth/logout"><input type="hidden" '
            f'name="csrf" value="{csrf}"><button type="submit">Déconnexion</button>'
            "</form></div>"
        )
        roles_label = ", ".join(sorted(principal.roles)) or "droits hérités"
        identity_notice = (
            '<div class="identity-notice">Identité synchronisée avec OpenRAG : '
            f'<strong>{user_label}</strong> · {html.escape(roles_label)}. '
            "L’inventaire et la file actuels restent un espace d’exploitation partagé ; "
            "leur découpage par propriétaire sera la prochaine étape.</div>"
        )
    else:
        identity_menu = '<span class="connector-chip">OpenArchiver connector</span>'
        identity_notice = ""

    source_lines = []
    for row in sources:
        source_id = html.escape(str(row["id"]), quote=True)
        label = html.escape(str(row["name"] or row["id"]))
        provider = html.escape(str(row["provider"] or "—"))
        checked = " checked" if int(row["selected"]) else ""
        source_lines.append(
            f'<label class="selection-item"><input type="checkbox" '
            f'name="source_id" value="{source_id}"{checked}>'
            f'<span class="selection-copy"><span class="selection-title">{label}</span>'
            f'<span class="selection-meta">{provider} · {source_id}</span></span></label>'
        )
    if not source_lines:
        source_lines.append(
            '<p class="empty">Aucune source découverte. '
            "Vérifiez le Secret et la disponibilité d’OpenArchiver.</p>"
        )

    mailbox_lines = []
    for row in mailboxes:
        source_id = str(row["source_id"])
        path = str(row["path"])
        token = html.escape(
            json.dumps([source_id, path], ensure_ascii=False), quote=True
        )
        label = html.escape(path or "(sans dossier)")
        source = html.escape(str(row["source_name"] or source_id))
        checked = " checked" if int(row["selected"]) else ""
        mailbox_lines.append(
            f'<label class="selection-item"><input type="checkbox" '
            f'name="mailbox" value="{token}"{checked}>'
            f'<span class="selection-copy"><span class="selection-title">{label}</span>'
            f'<span class="selection-meta">{source} · '
            f'{int(row["message_count"])} mails</span></span></label>'
        )
    if not mailbox_lines:
        mailbox_lines.append(
            '<p class="empty">Aucun dossier découvert. Sélectionnez une source, '
            "enregistrer, puis attendre la fin du cycle d’inventaire.</p>"
        )

    def count_badges(values: Mapping[str, int], empty: str) -> str:
        if not values:
            return f'<span class="badge">{html.escape(empty)}</span>'
        return "".join(
            f'<span class="badge">{html.escape(status)} · {count}</span>'
            for status, count in sorted(values.items())
        )

    def retry_panel(status: str, label: str) -> str:
        rows = retryable_rows(config, status)
        total = sum(
            values.get(status, 0) for values in selected_counts.values()
        )
        items = []
        for row in rows:
            token = html.escape(
                json.dumps(
                    [str(row["kind"]), str(row["id"]), status],
                    ensure_ascii=False,
                ),
                quote=True,
            )
            filename = html.escape(str(row["openrag_filename"]))
            error_detail = html.escape(str(row["last_error"] or "raison inconnue"))
            kind_label = "Mail" if str(row["kind"]) == "email" else "Pièce jointe"
            retry_at = int(row["next_retry_at"] or 0)
            retry_label = (
                "reprise auto le "
                + time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(retry_at))
                if retry_at
                else "reprise automatique épuisée"
            )
            items.append(
                f'<label class="selection-item"><input type="checkbox" '
                f'name="object" value="{token}" data-retry-object="{status}">'
                f'<span class="selection-copy"><span class="selection-title">{filename}</span>'
                f'<span class="selection-meta">{kind_label} · tentative {int(row["attempts"])} · '
                f'{html.escape(retry_label)}<br>{error_detail}</span></span></label>'
            )
        if not items:
            items.append(f'<p class="empty">Aucun objet {html.escape(label.lower())} dans la sélection active.</p>')
        truncated = (
            f'<p class="helper">Affichage limité aux 200 premiers objets sur {total}.</p>'
            if total > len(rows)
            else ""
        )
        disabled = " disabled" if not rows else ""
        return (
            f'<div id="retry-panel-{status}" class="tab-panel"><form method="post" action="/retry">'
            f'<input type="hidden" name="csrf" value="{csrf}">'
            f'<div class="selection-list">{"".join(items)}</div>{truncated}'
            f'<div class="retry-actions"><label class="retry-select-all"><input type="checkbox" '
            f'data-retry-status="{status}"{disabled}>Tout sélectionner ({len(rows)} affiché(s))</label>'
            f'<button class="primary" type="submit"{disabled}>Réindexer la sélection</button></div>'
            f'</form></div>'
        )

    lost_panel = retry_panel("lost", "Lost")
    failed_panel = retry_panel("failed", "Failed")
    lost_total = sum(values.get("lost", 0) for values in selected_counts.values())
    failed_total = sum(values.get("failed", 0) for values in selected_counts.values())

    error = html.escape(str(snapshot["last_error"] or "aucune"))
    ready = "Prêt" if snapshot["ready"] else "Attention requise"
    pause_label = "Reprendre l’indexation" if paused else "Mettre en pause"
    pause_action = "resume" if paused else "pause"
    activity = "En pause" if paused else "Active"
    effective_concurrency = int(snapshot["ingestion_concurrency_effective"])
    inventory_running = bool(snapshot["cycle_in_progress"])
    inventory_requested = bool(snapshot["cycle_requested_at"])
    current_stage = cycle_stage(snapshot)
    if inventory_running:
        ingestion_running = current_stage == "ingestion"
        inventory_activity = (
            "Inventaire terminé" if ingestion_running else "Inventaire en cours…"
        )
        inventory_class = "success running"
        button_label = (
            "Cycle en cours…" if ingestion_running else "Inventaire en cours…"
        )
        scan_button = f'<button id="inventory-button" class="primary" type="button" disabled>{button_label}</button>'
    elif inventory_requested:
        inventory_activity = "Inventaire demandé"
        inventory_class = "warning"
        scan_button = '<button id="inventory-button" class="primary" type="button" disabled>Inventaire demandé…</button>'
    else:
        inventory_activity = "En attente"
        inventory_class = "warning" if paused else "success"
        scan_button = '<button id="inventory-button" class="primary" type="submit">Relancer l’inventaire</button>'
    selected_sources = sum(int(row["selected"]) for row in sources)
    selected_mailboxes = sum(int(row["selected"]) for row in mailboxes)
    email_total = sum(selected_counts["emails"].values())
    historical_email_total = sum(counts["emails"].values())
    historical_attachment_total = sum(counts["attachments"].values())
    if inventory_cache:
        _cached_scan, inventory_completed_at = inventory_cache
        inventory_cache_label = (
            "Inventaire conservé depuis le "
            + time.strftime(
                "%Y-%m-%d %H:%M:%S UTC", time.gmtime(inventory_completed_at)
            )
            + " ; il sera remplacé uniquement sur demande manuelle."
        )
    else:
        inventory_cache_label = "Aucun inventaire complet disponible."
    openarchiver_key_prefix = secret_display_prefix(
        config.openarchiver_api_key_file
    )
    openrag_key_prefix = secret_display_prefix(config.openrag_api_key_file)
    openarchiver_key_state = "Configurée" if openarchiver_key_prefix else "Absente"
    openrag_key_state = "Configurée" if openrag_key_prefix else "Absente"
    openarchiver_key_display = (
        f'<code>{html.escape(openarchiver_key_prefix)}...</code>'
        if openarchiver_key_prefix
        else '<span class="secret-missing">Aucune clé configurée</span>'
    )
    openrag_key_display = (
        f'<code>{html.escape(openrag_key_prefix)}...</code>'
        if openrag_key_prefix
        else '<span class="secret-missing">Aucune clé configurée</span>'
    )
    openrag_base_url = html.escape(config.openrag_base_url, quote=True)
    connector_public_url = html.escape(config.connector_public_url, quote=True)
    ingestion_pool_size = normalize_runtime_pool_size(config.ingestion_concurrency)
    url_configuration_state = (
        "Enregistrée sur le PVC"
        if runtime_urls_are_persisted(config)
        and runtime_pool_size_is_persisted(config)
        else "Amorçage Rancher/Fleet"
    )
    status_class = "success" if snapshot["ready"] else "danger"
    activity_class = "warning" if paused else "success"
    last_completed = int(snapshot["last_cycle_completed_at"])
    last_sync = (
        time.strftime("%d/%m/%Y à %H:%M UTC", time.gmtime(last_completed))
        if last_completed
        else "Jamais exécutée"
    )
    error_alert = ""
    if snapshot["last_error"]:
        error_alert = (
            '<div class="error-alert" role="alert"><span aria-hidden="true">⚠</span>'
            f'<div><strong>Dernière erreur</strong>{error}</div></div>'
        )
    inventory_summary = (
        "Inventaire terminé ; ingestion OpenRAG en cours."
        if inventory_running and current_stage == "ingestion"
        else inventory_status(snapshot)
    )
    reconciliation_active = bool(
        snapshot["reconciliation_requested_at"]
        or snapshot["reconciliation_in_progress"]
    )
    reconciliation_error = str(snapshot["reconciliation_error"] or "")
    if snapshot["reconciliation_in_progress"]:
        reconciliation_label = "Réconciliation en cours…"
        reconciliation_detail = "Comparaison des connaissances locales avec OpenRAG."
    elif snapshot["reconciliation_requested_at"]:
        reconciliation_label = "Réconciliation demandée…"
        reconciliation_detail = "Le scan va démarrer."
    elif reconciliation_error:
        reconciliation_label = "Réconciliation interrompue"
        reconciliation_detail = reconciliation_error
    elif snapshot["reconciliation_completed_at"]:
        reconciliation_label = "Réconciliation terminée"
        reconciliation_detail = (
            f'{int(snapshot["reconciliation_current"])} vérifié(s) · '
            f'{int(snapshot["reconciliation_restored"])} restauré(s) · '
            f'{int(snapshot["reconciliation_lost"])} perdu(s) détecté(s)'
        )
    else:
        reconciliation_label = "Aucune réconciliation lancée"
        reconciliation_detail = (
            "Le scan compare les documents validés, lost et failed avec les chunks OpenRAG."
        )
    reconciliation_button = (
        '<button id="reconciliation-button" class="primary" type="button" disabled>'
        f'{html.escape(reconciliation_label)}</button>'
        if reconciliation_active
        else '<button id="reconciliation-button" class="primary" type="submit">Réconcilier avec OpenRAG</button>'
    )
    return f"""<!doctype html>
<html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light dark">
<title>Connecteur OpenArchiver · OpenRAG</title>
<style>{STATUS_PAGE_STYLE}</style><script src="/ui.js" defer></script></head>
<body data-cycle-active="{str(inventory_running or inventory_requested).lower()}" data-cycle-stage="{current_stage}" data-cycle-completed="{last_completed}" data-reconciliation-active="{str(reconciliation_active).lower()}">
<div class="app">
<header class="topbar"><div class="brand">{OPENRAG_LOGO}<span>OpenRAG</span></div>
{identity_menu}</header>
<main class="main"><div class="content">
<div class="page-heading"><p class="eyebrow">Connecteurs / OpenArchiver</p>
<h1>OpenArchiver vers OpenRAG</h1><p>Supervisez l’ingestion et choisissez précisément son périmètre.</p></div>
<div class="workspace-tabs" role="tablist" aria-label="Sections du connecteur">
<button id="workspace-tab-ingestion" class="workspace-tab" type="button" role="tab" aria-selected="true" aria-controls="workspace-panel-ingestion" data-workspace-tab="ingestion">État de l’ingestion</button>
<button id="workspace-tab-sources" class="workspace-tab" type="button" role="tab" aria-selected="false" aria-controls="workspace-panel-sources" data-workspace-tab="sources" tabindex="-1">Sources</button>
<button id="workspace-tab-configuration" class="workspace-tab" type="button" role="tab" aria-selected="false" aria-controls="workspace-panel-configuration" data-workspace-tab="configuration" tabindex="-1">Configuration</button>
</div>
<section id="workspace-panel-ingestion" class="workspace-panel" role="tabpanel" aria-labelledby="workspace-tab-ingestion" data-workspace-panel="ingestion">
<div class="section-heading"><div><h2>État de l’ingestion</h2><p>Suivi en direct des traitements envoyés à OpenRAG.</p></div>
<div class="toolbar"><form method="post" action="/pause"><input type="hidden" name="csrf" value="{csrf}"><input type="hidden" name="action" value="{pause_action}"><button type="submit">{pause_label}</button></form></div></div>
{error_alert}
<section class="status-grid" aria-label="État du connecteur">
<div class="stat-card"><span class="stat-label"><span class="dot {status_class}"></span>Service</span><strong id="service-status" class="stat-value">{ready}</strong><span id="last-sync" class="stat-detail">Dernière synchro : {last_sync}</span></div>
<div class="stat-card"><span class="stat-label"><span class="dot {activity_class}"></span>Indexation</span><strong class="stat-value">{activity}</strong><span id="last-processed" class="stat-detail">{int(snapshot["last_processed"])} objet(s) au dernier cycle · pool manuel : {effective_concurrency}</span></div>
<div class="stat-card"><span class="stat-label">Mails dans la sélection</span><strong id="mail-count" class="stat-value">{email_total}</strong><span id="mailbox-selected-count" class="stat-detail">{selected_mailboxes} dossier(s) sélectionné(s)</span></div>
<div class="stat-card"><span class="stat-label">Débit récent</span><strong id="mail-rate" class="stat-value">{int(snapshot["mails_per_minute"])}</strong><span class="stat-detail">mail(s) validé(s)/min</span></div>
</section>
<section class="card" aria-labelledby="active-task-title"><div class="card-header"><div><h2 id="active-task-title" class="card-title">Tâches en cours</h2><p class="card-description">Documents actuellement détenus par les slots du connecteur.</p></div><span id="active-task-count" class="badge">{len(active_tasks)} active(s)</span></div>
<div class="card-body"><div id="active-task-list" class="active-task-list" aria-live="polite">{render_active_ingestion_tasks(active_tasks)}</div></div></section>
<section class="card"><div class="card-header"><div><h2 class="card-title">Ingestion OpenRAG</h2><p class="card-description">Progression de l’envoi des mails et pièces jointes sélectionnés.</p></div></div>
<div class="card-body"><div id="cycle-progress" class="progress-wrap" role="progressbar" aria-label="Progression de l’ingestion OpenRAG" hidden><div class="progress-track"><div id="cycle-progress-bar" class="progress-bar"></div></div><span id="cycle-progress-label" class="progress-label">Préparation…</span></div>
<p id="inventory-completion" class="helper" role="status" hidden></p>
<div id="email-status-counts" class="counts" aria-label="États des mails de la sélection"><span class="badge success">Sélection actuelle</span>{count_badges(selected_counts["emails"], "aucun mail")}</div>
<p id="history-summary" class="helper">Historique local conservé : {historical_email_total} mail(s) ; {historical_attachment_total} pièce(s) jointe(s) déjà détaillée(s). Les éléments hors sélection ne sont pas envoyés à OpenRAG.</p></div>
</section>
</section>
<section id="workspace-panel-sources" class="workspace-panel" role="tabpanel" aria-labelledby="workspace-tab-sources" data-workspace-panel="sources" hidden>
<div class="section-heading"><div><h2>Sources</h2><p>Sélectionnez les comptes et dossiers OpenArchiver à indexer.</p></div></div>
<section class="card"><div class="card-header"><div><h2 class="card-title">Inventaire IMAP</h2><p class="card-description">État de la découverte des sources, dossiers et messages OpenArchiver.</p></div><span id="inventory-badge" class="badge {inventory_class}">{inventory_activity}</span></div>
<div class="card-body"><div class="inventory-row"><span id="inventory-dot" class="dot {inventory_class}"></span><span id="inventory-summary" class="inventory-status" role="status" aria-live="polite" aria-busy="{str(inventory_running and current_stage == 'inventory').lower()}">{html.escape(inventory_summary)}</span></div>
<div id="inventory-cycle-progress" class="progress-wrap" role="progressbar" aria-label="Progression de l’inventaire IMAP" hidden><div class="progress-track"><div id="inventory-cycle-progress-bar" class="progress-bar"></div></div><span id="inventory-cycle-progress-label" class="progress-label">Préparation…</span></div>
<p id="inventory-cache-label" class="helper">{html.escape(inventory_cache_label)}</p>
<p class="helper">L’inventaire est conservé comme un instantané stable et n’est renouvelé que sur demande.</p>
<p class="helper">La pause bloque les envois vers OpenRAG, mais n’empêche pas l’inventaire IMAP.</p></div>
<div class="card-footer"><form method="post" action="/scan"><input type="hidden" name="csrf" value="{csrf}">{scan_button}</form></div></section>
<form method="post" action="/sources" class="card"><div class="card-header"><div><h2 class="card-title">Sources indexées</h2><p class="card-description">Choisissez les comptes OpenArchiver à rendre disponibles dans OpenRAG.</p></div><span class="badge">{selected_sources}/{len(sources)} sélectionnée(s)</span></div>
<div class="card-body"><input type="hidden" name="csrf" value="{csrf}"><div class="selection-list">{"".join(source_lines)}</div></div>
<div class="card-footer"><button class="primary" type="submit">Enregistrer et lancer l’inventaire</button></div></form>
<form method="post" action="/mailboxes" class="card"><div class="card-header"><div><h2 class="card-title">Dossiers IMAP indexés</h2><p class="card-description">Affinez l’indexation aux dossiers utiles de chaque source.</p></div><span id="mailbox-selection-badge" class="badge">{selected_mailboxes}/{len(mailboxes)} sélectionné(s)</span></div>
<div class="card-body"><input type="hidden" name="csrf" value="{csrf}"><div id="mailbox-selection-list" class="selection-list">{"".join(mailbox_lines)}</div><div id="attachment-status-counts" class="counts" aria-label="États des pièces jointes détaillées de la sélection" style="margin-top:14px"><span class="badge success">Pièces jointes détaillées</span>{count_badges(selected_counts["attachments"], "pas encore détaillées")}</div></div>
<div class="card-footer"><button class="primary" type="submit">Enregistrer les dossiers</button></div></form>
<section id="retry-card" class="card"><div class="card-header"><div><h2 class="card-title">Réindexation</h2><p class="card-description">Contrôlez OpenRAG puis resoumettez les tâches réellement perdues ou en échec.</p></div><span id="retry-count-badge" class="badge warning">Lost {lost_total} · Failed {failed_total}</span></div>
<div class="card-body"><div class="reconciliation-row"><div><strong id="reconciliation-status">{html.escape(reconciliation_label)}</strong><p id="reconciliation-detail" class="helper">{html.escape(reconciliation_detail)}</p></div><form method="post" action="/reconcile"><input type="hidden" name="csrf" value="{csrf}">{reconciliation_button}</form></div><div id="reconciliation-progress" class="progress-wrap" role="progressbar" aria-label="Progression de la réconciliation" {'hidden' if not reconciliation_active else ''}><div class="progress-track"><div id="reconciliation-progress-bar" class="progress-bar"></div></div><span id="reconciliation-progress-label" class="progress-label">Préparation…</span></div><hr class="section-rule"><div id="retry-tabs" class="retry-tabs"><input class="tab-toggle" type="radio" name="retry-tab" id="retry-tab-lost" checked><label class="tab-label" for="retry-tab-lost">Lost · {lost_total}</label><input class="tab-toggle" type="radio" name="retry-tab" id="retry-tab-failed"><label class="tab-label" for="retry-tab-failed">Failed · {failed_total}</label><div class="tab-panels">{lost_panel}{failed_panel}</div></div><p class="helper">La réindexation réinitialise les tentatives des objets choisis. Si le connecteur est en pause, utilisez ensuite « Reprendre l’indexation ».</p></div></section>
</section>
<section id="workspace-panel-configuration" class="workspace-panel" role="tabpanel" aria-labelledby="workspace-tab-configuration" data-workspace-panel="configuration" hidden>
<div class="section-heading"><div><h2>Configuration</h2><p>Accès techniques et identité héritée d’OpenRAG.</p></div></div>
{identity_notice}
<form method="post" action="/configuration" class="card" autocomplete="off"><div class="card-header"><div><h2 class="card-title">Services et ingestion</h2><p class="card-description">Configurez les adresses du connecteur et la concurrence d’ingestion.</p></div><span class="badge success">{url_configuration_state}</span></div>
<div class="card-body"><input type="hidden" name="csrf" value="{csrf}"><div class="secret-grid"><label class="secret-field">URL interne de l’API OpenRAG<span class="secret-current">Service HTTP joignable depuis le cluster</span><input type="url" name="openrag_base_url" value="{openrag_base_url}" required spellcheck="false" autocomplete="url"></label><label class="secret-field">URL publique du connecteur<span class="secret-current">Adresse HTTPS sans chemin ni paramètres</span><input type="url" name="connector_public_url" value="{connector_public_url}" required spellcheck="false" autocomplete="url"></label><label class="secret-field">Taille du pool d’ingestion<span class="secret-current">De 1 à 6 tâches simultanées · valeur par défaut : 3</span><input type="number" name="ingestion_pool_size" value="{ingestion_pool_size}" min="1" max="6" step="1" required inputmode="numeric"></label></div><p class="helper">Ces valeurs sont conservées dans SQLite sur le PVC et deviennent actives immédiatement. Les slots sont ajustés sans interrompre les tâches déjà en cours.</p></div>
<div class="card-footer"><button class="primary" type="submit">Enregistrer la configuration</button></div></form>
<form method="post" action="/secrets" class="card" autocomplete="off"><div class="card-header"><div><h2 class="card-title">Clés API</h2><p class="card-description">Renouvelez séparément les accès OpenArchiver et OpenRAG.</p></div><span class="badge">OpenArchiver : {openarchiver_key_state} · OpenRAG : {openrag_key_state}</span></div>
<div class="card-body"><input type="hidden" name="csrf" value="{csrf}"><div class="secret-grid"><label class="secret-field">Nouvelle clé OpenArchiver<span class="secret-current">Clé actuelle : {openarchiver_key_display}</span><input type="password" name="openarchiver_key" autocomplete="new-password"></label><label class="secret-field">Nouvelle clé OpenRAG<span class="secret-current">Clé actuelle : {openrag_key_display}</span><input type="password" name="openrag_key" autocomplete="new-password"></label></div><p class="helper">Laissez un champ vide pour conserver sa valeur actuelle. Seul le début des clés est affiché ; leur valeur complète n’est jamais placée dans la page ni enregistrée dans SQLite.</p></div>
<div class="card-footer"><button class="primary" type="submit">Enregistrer les clés renseignées</button></div></form>
</section>
<p class="footer-note">Aucune suppression OpenRAG n’est automatique · Interface d’exploitation autonome</p>
</div></main></div></body></html>"""


def render_metrics(config: Config, state: RuntimeState) -> str:
    snapshot = state.snapshot()
    counts = _status_counts(config)
    selected = len(selected_source_ids(config))
    selected_mailboxes = sum(int(row["selected"]) for row in mailbox_rows(config))
    paused = is_paused(config)
    lines = [
        "# TYPE openarchiver_connector_ready gauge",
        f"openarchiver_connector_ready {1 if snapshot['ready'] else 0}",
        "# TYPE openarchiver_connector_selected_sources gauge",
        f"openarchiver_connector_selected_sources {selected}",
        "# TYPE openarchiver_connector_selected_mailboxes gauge",
        f"openarchiver_connector_selected_mailboxes {selected_mailboxes}",
        "# TYPE openarchiver_connector_paused gauge",
        f"openarchiver_connector_paused {1 if paused else 0}",
        "# TYPE openarchiver_connector_ingestion_concurrency gauge",
        f"openarchiver_connector_ingestion_concurrency {snapshot['ingestion_concurrency_effective']}",
        "# TYPE openarchiver_connector_objects gauge",
    ]
    for kind, values in counts.items():
        for status, count in sorted(values.items()):
            lines.append(
                f'openarchiver_connector_objects{{kind="{kind}",status="{status}"}} {count}'
            )
    return "\n".join(lines) + "\n"


def make_http_handler(
    config: Config,
    state: RuntimeState,
    *,
    wake: threading.Event = WAKE,
    reconciliation_wake: threading.Event = RECONCILE_WAKE,
    auth_client: OpenRAGAuthClient | None = None,
) -> type[BaseHTTPRequestHandler]:
    """Construit le handler HTTP lié à la configuration et à l'état courant.

    Les probes et métriques restent publiques. Les autres routes suivent le
    mode d'authentification OpenRAG et chaque mutation POST exige aussi le jeton
    CSRF rendu dans la page. Les réponses SSE publient uniquement lors d'un
    changement d'état, avec un keepalive toutes les 15 secondes.
    """

    auth = auth_client or OpenRAGAuthClient(config)
    oauth_state_cookie = "openrag_oauth_state"

    class Handler(BaseHTTPRequestHandler):
        server_version = "OpenArchiverConnector/1"

        def _send(
            self,
            status: int,
            body: str,
            content_type: str,
            headers: Sequence[tuple[str, str]] = (),
        ) -> None:
            payload = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'none'; style-src 'unsafe-inline'; "
                "script-src 'self'; connect-src 'self'; frame-src 'self'; "
                "form-action 'self'",
            )
            for name, value in headers:
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(payload)

        def _redirect(
            self,
            location: str = "/",
            headers: Sequence[tuple[str, str]] = (),
        ) -> None:
            self.send_response(303)
            self.send_header("Location", location)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", "0")
            for name, value in headers:
                self.send_header(name, value)
            self.end_headers()

        def _cookie(self, name: str) -> str:
            raw = self.headers.get("Cookie", "")
            if len(raw) > 32_768:
                return ""
            try:
                cookies = http.cookies.SimpleCookie()
                cookies.load(raw)
                morsel = cookies.get(name)
                return morsel.value if morsel is not None else ""
            except http.cookies.CookieError:
                return ""

        @staticmethod
        def _set_cookie(name: str, value: str, max_age: int) -> tuple[str, str]:
            cookie = http.cookies.SimpleCookie()
            cookie[name] = value
            morsel = cookie[name]
            morsel["path"] = "/"
            morsel["max-age"] = str(max_age)
            morsel["httponly"] = True
            morsel["secure"] = True
            morsel["samesite"] = "Lax"
            return "Set-Cookie", morsel.OutputString()

        def _principal(self) -> ConnectorPrincipal | None:
            if hasattr(self, "_cached_principal"):
                return self._cached_principal
            token = self._cookie(config.openrag_auth_cookie_name)
            principal = auth.resolve(token)
            if principal is not None:
                sync_connector_user(config, principal)
            self._cached_principal = principal
            return principal

        def _require_principal(self, *, redirect: bool = False) -> ConnectorPrincipal | None:
            principal = self._principal()
            if principal is not None:
                return principal
            if redirect:
                self._redirect("/login")
            else:
                self._send(
                    401,
                    "authentication required\n",
                    "text/plain; charset=utf-8",
                )
            return None

        def _finish_action(
            self, principal: ConnectorPrincipal, action: str
        ) -> None:
            record_audit(config, principal, action)
            self._redirect()

        def _send_status_events(self) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("X-Accel-Buffering", "no")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            try:
                last_event_id = self.headers.get("Last-Event-ID", "")
                revision = int(last_event_id) if last_event_id.isdigit() else -1
                while True:
                    current_revision = state.revision()
                    if current_revision != revision:
                        payload = render_live_status(state, config).rstrip("\n")
                        event = (
                            f"id: {current_revision}\n"
                            f"event: status\n"
                            f"data: {payload}\n\n"
                        )
                        self.wfile.write(event.encode("utf-8"))
                        self.wfile.flush()
                        revision = current_revision
                    next_revision = state.wait_for_change(revision, timeout=15)
                    if next_revision == revision:
                        self.wfile.write(b": keepalive\n\n")
                        self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, TimeoutError):
                return
            except OSError as error:
                LOG.debug("flux SSE fermé: %s", _safe_error(error))

        def do_GET(self) -> None:
            parsed_request = urllib.parse.urlsplit(self.path)
            path = parsed_request.path
            try:
                principal: ConnectorPrincipal | None = None
                if path == "/auth/callback":
                    query = urllib.parse.parse_qs(
                        parsed_request.query, keep_blank_values=True
                    )
                    connection_id = query.get("state", [""])[0]
                    code = query.get("code", [""])[0]
                    provider_error = query.get("error", [""])[0]
                    if provider_error:
                        raise ConnectorError(
                            f"connexion Google refusée: {provider_error}"
                        )
                    if connection_id != self._cookie(oauth_state_cookie):
                        raise ConnectorError("état OAuth expiré ou invalide")
                    token = auth.complete_login(connection_id, code)
                    principal = auth.resolve(token)
                    if principal is None or not principal.authenticated:
                        raise ConnectorError("session OpenRAG non créée")
                    sync_connector_user(config, principal)
                    self._redirect(
                        "/",
                        headers=(
                            self._set_cookie(
                                config.openrag_auth_cookie_name,
                                token,
                                7 * 24 * 60 * 60,
                            ),
                            self._set_cookie(oauth_state_cookie, "", 0),
                        ),
                    )
                    return
                if path == "/login":
                    principal = self._principal()
                    if principal is not None:
                        self._redirect("/")
                    else:
                        form_error = urllib.parse.parse_qs(
                            parsed_request.query
                        ).get("error", [""])[0]
                        self._send(
                            200,
                            render_login_page(state, form_error[:500]),
                            "text/html; charset=utf-8",
                        )
                    return
                if path not in {"/healthz", "/readyz", "/metrics", "/ui.js"}:
                    principal = self._require_principal(redirect=path == "/")
                    if principal is None:
                        return

                if path == "/healthz":
                    running = bool(state.snapshot()["running"])
                    self._send(
                        200 if running else 503,
                        "ok\n" if running else "worker stopped\n",
                        "text/plain; charset=utf-8",
                    )
                elif path == "/readyz":
                    # La readiness Kubernetes décrit la capacité du serveur à
                    # recevoir du trafic. Une erreur de synchronisation reste
                    # visible dans l'interface et les métriques, mais ne doit
                    # pas retirer l'unique pod des endpoints du Service.
                    ready = bool(state.snapshot()["running"])
                    self._send(
                        200 if ready else 503,
                        "ready\n" if ready else "not ready\n",
                        "text/plain; charset=utf-8",
                    )
                elif path == "/metrics":
                    self._send(
                        200,
                        render_metrics(config, state),
                        "text/plain; version=0.0.4",
                    )
                elif path == "/inventory-status":
                    self._send(
                        200,
                        render_inventory_status_page(state),
                        "text/html; charset=utf-8",
                    )
                elif path == "/status.json":
                    self._send(
                        200,
                        render_live_status(state, config),
                        "application/json; charset=utf-8",
                    )
                elif path == "/events":
                    self._send_status_events()
                elif path == "/ui.js":
                    self._send(
                        200,
                        UI_SCRIPT,
                        "text/javascript; charset=utf-8",
                    )
                elif path == "/":
                    started_at = time.monotonic()
                    body = render_status_page(config, state, principal)
                    LOG.info(
                        "page principale générée en %.3fs",
                        time.monotonic() - started_at,
                    )
                    self._send(
                        200,
                        body,
                        "text/html; charset=utf-8",
                    )
                else:
                    self._send(404, "not found\n", "text/plain; charset=utf-8")
            except ConnectorError as error:
                if path == "/auth/callback":
                    self._send(
                        400,
                        render_login_page(state, _safe_error(error)),
                        "text/html; charset=utf-8",
                    )
                else:
                    self._send(
                        503,
                        _safe_error(error) + "\n",
                        "text/plain; charset=utf-8",
                    )
            except Exception:
                LOG.exception("échec de réponse HTTP")
                self._send(500, "internal error\n", "text/plain; charset=utf-8")

        def do_POST(self) -> None:
            path = urllib.parse.urlsplit(self.path).path
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length < 0 or length > 65_536:
                    self._send(413, "request too large\n", "text/plain; charset=utf-8")
                    return
                raw = self.rfile.read(length).decode("utf-8")
                form = urllib.parse.parse_qs(raw, keep_blank_values=True)
                csrf = form.get("csrf", [""])[0]
                if not isinstance(csrf, str) or csrf != state.snapshot()["csrf_token"]:
                    self._redirect("/?form=expired")
                    return
                if path == "/auth/login":
                    authorization_url, oauth_state = auth.begin_login()
                    self._redirect(
                        authorization_url,
                        headers=(
                            self._set_cookie(oauth_state_cookie, oauth_state, 600),
                        ),
                    )
                    return
                if path == "/auth/logout":
                    token = self._cookie(config.openrag_auth_cookie_name)
                    principal = self._principal()
                    auth.logout(token)
                    if principal is not None:
                        record_audit(config, principal, "auth.logout")
                    self._redirect(
                        "/login",
                        headers=(
                            self._set_cookie(
                                config.openrag_auth_cookie_name, "", 0
                            ),
                        ),
                    )
                    return

                principal = self._require_principal()
                if principal is None:
                    return
                permission_by_path = {
                    "/configuration": "config:write",
                    "/secrets": "config:write",
                    "/sources": "config:write",
                    "/mailboxes": "config:write",
                    "/pause": "config:write",
                    "/retry": "knowledge:upload",
                    "/reconcile": "knowledge:upload",
                    "/scan": "knowledge:upload",
                }
                required_permission = permission_by_path.get(path)
                if required_permission is None:
                    self._send(404, "not found\n", "text/plain; charset=utf-8")
                    return
                if not principal.can(required_permission):
                    self._send(
                        403,
                        "permission OpenRAG insuffisante pour cette action\n",
                        "text/plain; charset=utf-8",
                    )
                    return
                if path == "/configuration":
                    openrag_base_url = form.get("openrag_base_url", [""])[0]
                    connector_public_url = form.get(
                        "connector_public_url", [""]
                    )[0]
                    ingestion_pool_size = normalize_runtime_pool_size(
                        form.get("ingestion_pool_size", [""])[0]
                    )
                    persist_runtime_urls(
                        config,
                        openrag_base_url=openrag_base_url,
                        connector_public_url=connector_public_url,
                    )
                    persist_runtime_pool_size(config, ingestion_pool_size)
                    state.cycle_requested()
                    wake.set()
                    self._finish_action(principal, "configuration.update")
                elif path == "/secrets":
                    changed = []
                    openarchiver_key = form.get("openarchiver_key", [""])[0]
                    openrag_key = form.get("openrag_key", [""])[0]
                    if openarchiver_key.strip():
                        write_secret(
                            config.openarchiver_api_key_file,
                            openarchiver_key,
                            "OpenArchiver",
                        )
                        changed.append("OpenArchiver")
                    if openrag_key.strip():
                        write_secret(
                            config.openrag_api_key_file, openrag_key, "OpenRAG"
                        )
                        changed.append("OpenRAG")
                    if not changed:
                        raise ConnectorError("aucune nouvelle clé renseignée")
                    state.cycle_requested()
                    wake.set()
                    self._finish_action(principal, "secrets.update")
                elif path == "/sources":
                    replace_source_selection(config, form.get("source_id", []))
                    state.cycle_requested(force_inventory=True)
                    wake.set()
                    self._finish_action(principal, "sources.select")
                elif path == "/mailboxes":
                    selections = []
                    for value in form.get("mailbox", []):
                        decoded = json.loads(value)
                        if (
                            not isinstance(decoded, list)
                            or len(decoded) != 2
                            or not all(isinstance(item, str) for item in decoded)
                        ):
                            raise ConnectorError("sélection de dossier invalide")
                        selections.append((decoded[0], decoded[1]))
                    replace_mailbox_selection(config, selections)
                    self._finish_action(principal, "mailboxes.select")
                elif path == "/retry":
                    objects = []
                    for value in form.get("object", []):
                        decoded = json.loads(value)
                        if (
                            not isinstance(decoded, list)
                            or len(decoded) != 3
                            or not all(isinstance(item, str) for item in decoded)
                        ):
                            raise ConnectorError("sélection de réindexation invalide")
                        objects.append((decoded[0], decoded[1], decoded[2]))
                    if requeue_objects(config, objects) == 0:
                        raise ConnectorError("aucun objet éligible à réindexer")
                    state.cycle_requested()
                    wake.set()
                    self._finish_action(principal, "objects.retry")
                elif path == "/reconcile":
                    if not state.reconciliation_requested():
                        raise ConnectorError("réconciliation OpenRAG déjà en cours")
                    reconciliation_wake.set()
                    self._finish_action(principal, "openrag.reconcile")
                elif path == "/pause":
                    action = form.get("action", [""])[0]
                    if action not in {"pause", "resume"}:
                        raise ConnectorError("action de pause invalide")
                    set_paused(config, action == "pause")
                    if action == "resume":
                        state.cycle_requested()
                        wake.set()
                    self._finish_action(principal, f"ingestion.{action}")
                elif path == "/scan":
                    state.cycle_requested(force_inventory=True)
                    wake.set()
                    self._finish_action(principal, "inventory.scan")
                else:
                    self._send(404, "not found\n", "text/plain; charset=utf-8")
            except (UnicodeDecodeError, ValueError, ConnectorError) as error:
                self._send(400, _safe_error(error) + "\n", "text/plain; charset=utf-8")
            except Exception:
                LOG.exception("échec de requête HTTP")
                self._send(500, "internal error\n", "text/plain; charset=utf-8")

        def log_message(self, template: str, *args: object) -> None:
            LOG.info("http " + template, *args)

    return Handler


def main() -> None:
    """Migre la base, démarre les trois workers de fond et sert l'interface."""

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    config = Config.from_env()
    with database(config):
        pass
    if restore_runtime_urls(config):
        LOG.info("URL OpenRAG et URL publique restaurées depuis la configuration")
    if restore_runtime_pool_size(config):
        LOG.info(
            "taille du pool d'ingestion restaurée: %d",
            config.ingestion_concurrency,
        )

    STOP.clear()
    WAKE.clear()
    RECONCILE_WAKE.clear()
    POOL_RECONFIGURE.clear()
    state = RuntimeState()
    restore_cycle_outcome(config, state)

    def stop_service(_signum: int, _frame: object) -> None:
        STOP.set()
        WAKE.set()
        RECONCILE_WAKE.set()

    signal.signal(signal.SIGTERM, stop_service)
    signal.signal(signal.SIGINT, stop_service)
    worker = threading.Thread(
        target=runtime_loop,
        args=(config, state),
        name="openarchiver-cycle",
        daemon=True,
    )
    worker.start()
    reconciliation_worker = threading.Thread(
        target=reconciliation_loop,
        args=(config, state),
        name="openarchiver-reconciliation",
        daemon=True,
    )
    reconciliation_worker.start()
    mail_rate_worker = threading.Thread(
        target=mail_rate_monitor_loop,
        args=(config, state),
        name="mail-rate-monitor",
        daemon=True,
    )
    mail_rate_worker.start()
    server = ThreadingHTTPServer(
        (config.http_host, config.http_port), make_http_handler(config, state)
    )
    server.timeout = 1
    LOG.info("interface HTTP en écoute sur %s:%d", config.http_host, config.http_port)
    try:
        while not STOP.is_set():
            server.handle_request()
    finally:
        STOP.set()
        WAKE.set()
        RECONCILE_WAKE.set()
        server.server_close()
        worker.join(timeout=5)
        reconciliation_worker.join(timeout=5)
        mail_rate_worker.join(timeout=5)


if __name__ == "__main__":
    main()
