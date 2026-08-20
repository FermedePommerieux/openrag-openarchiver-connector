"""Cœur du connecteur OpenArchiver vers le dossier d'ingestion OpenRAG.

Le connecteur conserve son inventaire, ses sélections, ses reprises et son
rate limiting dans SQLite. Pour l'ingestion, il dépose désormais les messages
originaux ``.eml`` et les pièces jointes compatibles dans le volume partagé,
par renommage atomique, puis déclenche l'API ``ingest-path`` d'OpenRAG.

OpenRAG reste ainsi seul responsable du traitement et, lorsque l'archivage est
activé dans Settings > Archiving, du déplacement de la source vers son archive
authentifiée. En mode multi-utilisateur, le refus du chemin local déclenche un
repli vers l'upload multipart, avec une ``source_url`` distante facultative.
"""

from __future__ import annotations

import hashlib
import html
import http.client
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
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor
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
QUEUE_STATUSES = ("queued", "failed")
ALL_STATUSES = (
    "discovered",
    "queued",
    "downloading",
    "ingesting",
    "validated",
    "failed",
    "non_indexable",
    "missing",
    "unavailable",
)
DEFAULT_EXTENSIONS = ".asc,.asciidoc,.adoc,.csv,.docx,.htm,.html,.md,.pdf,.txt,.xlsx"
SAFE_EXTENSION = re.compile(r"^\.[a-z0-9]{1,10}$")
RQ_WORKER_METRIC = re.compile(
    r"^rq_workers\{(?P<labels>.*)\}\s+(?P<value>[0-9.eE+-]+)$"
)
PROMETHEUS_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"\\])*)"')
STOP = threading.Event()
WAKE = threading.Event()
SCHEMA_VERSION = 2
SCHEMA_LOCK = threading.Lock()


class ConnectorError(RuntimeError):
    """Erreur contrôlée dont le message ne contient ni secret ni contenu."""


class HTTPStatusError(ConnectorError):
    def __init__(self, status: int, operation: str) -> None:
        self.status = status
        super().__init__(f"{operation}: HTTP {status}")


class IncompleteScanError(ConnectorError):
    pass


class FileTooLargeError(ConnectorError):
    pass


@dataclass(frozen=True)
class Config:
    openarchiver_base_url: str
    openarchiver_api_key_file: Path
    openrag_base_url: str
    openrag_ingest_path: str
    openrag_task_path: str
    openrag_api_key_file: Path
    state_db: Path
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
    ingestion_concurrency: int | None = None
    ingestion_concurrency_fallback: int = 2
    ingestion_concurrency_max: int = 4
    docling_metrics_url: str = (
        "http://docling-serve.docling.svc.cluster.local:5001/metrics"
    )
    docling_metrics_timeout: int = 5
    docling_queue_name: str = "convert"
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
        concurrency_max = max(1, int(values.get("INGESTION_CONCURRENCY_MAX", "4")))
        concurrency_value = values.get("INGESTION_CONCURRENCY", "auto").strip().lower()
        ingestion_concurrency = (
            None
            if concurrency_value == "auto"
            else min(concurrency_max, max(1, int(concurrency_value)))
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
            ingestion_concurrency_fallback=min(
                concurrency_max,
                max(1, int(values.get("INGESTION_CONCURRENCY_FALLBACK", "2"))),
            ),
            ingestion_concurrency_max=concurrency_max,
            docling_metrics_url=values.get(
                "DOCLING_METRICS_URL",
                "http://docling-serve.docling.svc.cluster.local:5001/metrics",
            ),
            docling_metrics_timeout=max(
                1, int(values.get("DOCLING_METRICS_TIMEOUT_SECONDS", "5"))
            ),
            docling_queue_name=values.get("DOCLING_RQ_QUEUE_NAME", "convert"),
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
        _validate_internal_http_url(config.docling_metrics_url, "DOCLING_METRICS_URL")
        if not config.openrag_ingest_directory.is_absolute():
            raise ValueError("OPENRAG_INGEST_DIRECTORY doit être un chemin absolu")
        if config.openrag_ingest_mode not in {"auto", "path", "api"}:
            raise ValueError("OPENRAG_INGEST_MODE doit être auto, path ou api")
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


def connect_db(config: Config) -> sqlite3.Connection:
    """Crée et migre idempotemment l'état local."""
    config.state_db.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(config.state_db, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=30000")
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


def reset_state_database(config: Config) -> None:
    """Vide l'état local du connecteur sans toucher aux systèmes sources.

    Ce que l'on veut : repartir avec un inventaire OpenArchiver vierge.
    Pourquoi : permettre une reconstruction propre après un test ou un mauvais choix.
    Comment : vider les tables fonctionnelles dans une transaction et rester en pause.
    Compatibilité : aucun mail OpenArchiver ni document OpenRAG n'est supprimé.
    KISS : le schéma et le PVC restent en place ; seul leur contenu fonctionnel est effacé.
    """
    with database(config) as db:
        for table in (
            "email_attachments",
            "attachments",
            "emails",
            "mailboxes",
            "sources",
            "settings",
        ):
            db.execute(f"DELETE FROM {table}")
        db.execute("INSERT INTO settings(key,value) VALUES ('paused','1')")


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


class OpenArchiverClient:
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
        for email in global_emails.values():
            _upsert_email(db, email, scan_started)
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
            mail_openrag_filename(str(email["id"])),
            now,
            now,
        ),
    )


def mail_openrag_filename(email_id: str) -> str:
    return f"openarchiver-mail-{_safe_identifier(email_id)}.eml"


def safe_attachment_extension(filename: str) -> str:
    suffix = Path(filename).suffix.lower()
    return suffix if SAFE_EXTENSION.fullmatch(suffix) else ""


def attachment_openrag_filename(attachment_id: str, filename: str) -> str:
    return (
        f"openarchiver-attachment-{_safe_identifier(attachment_id)}"
        f"{safe_attachment_extension(filename)}"
    )


def _safe_identifier(value: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9-]{1,128}", value):
        return value
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def inventory_attachments(
    config: Config,
    email_id: str,
    detail: Mapping[str, object],
    *,
    observed_at: int | None = None,
) -> list[str]:
    raw_attachments = detail.get("attachments", [])
    if not isinstance(raw_attachments, list):
        raise ConnectorError("inventaire des pièces jointes invalide")
    now = int(time.time()) if observed_at is None else observed_at
    identifiers: list[str] = []
    with database(config) as db:
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
                            THEN excluded.status
                        ELSE attachments.status END,
                    sha256=CASE
                        WHEN attachments.metadata_fingerprint <> excluded.metadata_fingerprint
                            THEN '' ELSE attachments.sha256 END,
                    attempts=CASE
                        WHEN attachments.metadata_fingerprint <> excluded.metadata_fingerprint
                            THEN 0 ELSE attachments.attempts END,
                    last_error=CASE
                        WHEN attachments.metadata_fingerprint <> excluded.metadata_fingerprint
                            THEN excluded.last_error ELSE attachments.last_error END,
                    next_retry_at=CASE
                        WHEN attachments.metadata_fingerprint <> excluded.metadata_fingerprint
                            THEN 0 ELSE attachments.next_retry_at END,
                    task_id=CASE
                        WHEN attachments.metadata_fingerprint <> excluded.metadata_fingerprint
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
                    attachment_openrag_filename(attachment_id, filename),
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


class OpenRAGClient:
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
                raise
            status = str(payload.get("status", "")).lower()
            if status == "completed":
                if _failed_count(payload.get("failed_files")) == 0:
                    return
                raise ConnectorError("tâche OpenRAG terminée avec fichiers en échec")
            if status in {"failed", "cancelled", "canceled"}:
                raise ConnectorError(f"tâche OpenRAG {status}")
            self.sleeper(5)
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


def claim_next(config: Config, *, now: int | None = None) -> WorkItem | None:
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
                        status='failed' AND attempts < ?
                        AND next_retry_at > 0 AND next_retry_at <= ?
                    )
                )
                ORDER BY CASE status WHEN 'queued' THEN 0 ELSE 1 END,
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
                WHERE id=? AND attempts=? AND status IN ('queued','failed')
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
            openrag.wait(task_id)
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
            openrag.wait(task_id)
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


def _safe_error(error: Exception) -> str:
    if isinstance(error, (ConnectorError, TimeoutError, ValueError)):
        return str(error)[:1000]
    if isinstance(error, sqlite3.Error):
        detail = " ".join(str(error).split())
        return f"{error.__class__.__name__}: {detail}"[:1000]
    return error.__class__.__name__


def parse_docling_worker_metrics(metrics: str, queue_name: str) -> int:
    """Compte les workers RQ actifs qui consomment la file Docling attendue."""
    family_present = False
    workers = 0
    for line in metrics.splitlines():
        line = line.strip()
        if line.startswith("# HELP rq_workers ") or line == "# TYPE rq_workers gauge":
            family_present = True
            continue
        match = RQ_WORKER_METRIC.match(line)
        if not match:
            continue
        family_present = True
        labels = dict(PROMETHEUS_LABEL.findall(match.group("labels")))
        queues = {item.strip() for item in labels.get("queues", "").split(",")}
        if queue_name not in queues or labels.get("state") == "suspended":
            continue
        workers += max(0, int(float(match.group("value"))))
    if not family_present:
        raise RuntimeError("métrique rq_workers absente de la réponse Docling")
    return workers


def detect_docling_workers(config: Config) -> int:
    request = urllib.request.Request(
        config.docling_metrics_url,
        headers={"Accept": "text/plain"},
    )
    with urllib.request.urlopen(
        request, timeout=config.docling_metrics_timeout
    ) as response:
        payload = response.read(2_000_001)
    if len(payload) > 2_000_000:
        raise RuntimeError("réponse métriques Docling trop volumineuse")
    return parse_docling_worker_metrics(
        payload.decode("utf-8", errors="replace"),
        config.docling_queue_name,
    )


def effective_ingestion_concurrency(
    config: Config, state: RuntimeState | None = None
) -> int:
    """Résout la concurrence fixe ou automatique et publie son état runtime."""
    detected = -1
    detection_success = False
    if config.ingestion_concurrency is not None:
        effective = min(config.ingestion_concurrency, config.ingestion_concurrency_max)
    else:
        try:
            detected = detect_docling_workers(config)
            detection_success = True
            effective = min(detected, config.ingestion_concurrency_max)
            if effective == 0:
                LOG.warning(
                    "aucun worker Docling RQ détecté; aucune nouvelle ingestion"
                )
            else:
                LOG.info(
                    "%d worker(s) Docling détecté(s); concurrence effective=%d",
                    detected,
                    effective,
                )
        except Exception as error:
            effective = min(
                config.ingestion_concurrency_fallback,
                config.ingestion_concurrency_max,
            )
            LOG.warning(
                "détection des workers Docling impossible (%s); repli concurrence=%d",
                _safe_error(error),
                effective,
            )
    if state is not None:
        state.worker_detection_updated(
            detected=detected,
            effective=effective,
            success=detection_success,
        )
    return effective


def process_queue(
    config: Config,
    openarchiver: OpenArchiverClient,
    openrag: OpenRAGClient,
    progress: Callable[[int, int], None] | None = None,
    state: RuntimeState | None = None,
) -> int:
    workers = effective_ingestion_concurrency(config, state)
    if workers == 0:
        return 0
    progress_lock = threading.Lock()
    processed_total = 0
    initial_total = selected_queue_pending_count(config)
    if progress is not None:
        progress(0, initial_total)

    def worker() -> int:
        nonlocal processed_total
        processed = 0
        while True:
            item = claim_next(config)
            if item is None:
                return processed
            process_work_item(config, item, openarchiver, openrag)
            processed += 1
            if progress is not None:
                with progress_lock:
                    processed_total += 1
                    current = processed_total
                progress(current, max(initial_total, current))

    with ThreadPoolExecutor(
        max_workers=workers, thread_name_prefix="openarchiver-ingest"
    ) as pool:
        return sum(pool.map(lambda _index: worker(), range(workers)))


def selected_queue_pending_count(config: Config) -> int:
    counts = _status_counts(config, selected_only=True)
    active_statuses = {"queued", "downloading", "ingesting"}
    return sum(
        count
        for values in counts.values()
        for status, count in values.items()
        if status in active_statuses
    )


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
        self.reset_requested_at = 0
        self.last_reset_at = 0
        self.docling_workers_detected = -1
        self.ingestion_concurrency_effective = 0
        self.worker_detection_success = False
        self.worker_detection_at = 0
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

    def worker_detection_updated(
        self, *, detected: int, effective: int, success: bool
    ) -> None:
        with self.changed:
            self.docling_workers_detected = detected
            self.ingestion_concurrency_effective = effective
            self.worker_detection_success = success
            self.worker_detection_at = int(time.time())
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

    def reset_requested(self) -> None:
        with self.changed:
            self.reset_requested_at = int(time.time())
            self._notify_changed()

    def reset_pending(self) -> bool:
        with self.lock:
            return bool(self.reset_requested_at)

    def reset_succeeded(self) -> None:
        with self.changed:
            self.reset_requested_at = 0
            self.last_reset_at = int(time.time())
            self.last_cycle_started_at = 0
            self.last_cycle_completed_at = 0
            self.cycle_requested_at = 0
            self.force_inventory_requested = False
            self.last_error = ""
            self.last_scan = None
            self.last_processed = 0
            self.cycle_phase = ""
            self.progress_current = 0
            self.progress_total = 0
            self.ready = True
            self._notify_changed()

    def reset_failed(self, error: Exception) -> None:
        with self.changed:
            self.last_error = _safe_error(error)
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
                "reset_requested_at": self.reset_requested_at,
                "last_reset_at": self.last_reset_at,
                "docling_workers_detected": self.docling_workers_detected,
                "ingestion_concurrency_effective": self.ingestion_concurrency_effective,
                "worker_detection_success": self.worker_detection_success,
                "worker_detection_at": self.worker_detection_at,
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
    report("Traitement de la file vers OpenRAG", 0, queue_total)
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
            "Traitement de la file vers OpenRAG", current, total
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
    archive_client = openarchiver or OpenArchiverClient(config)
    rag_client = openrag or OpenRAGClient(config)
    state.set_running(True)
    try:
        recovered = recover_interrupted(config)
        if recovered:
            LOG.warning("%d opération(s) interrompue(s) récupérée(s)", recovered)
        while not stop.is_set():
            if state.reset_pending():
                try:
                    reset_state_database(config)
                    state.reset_succeeded()
                    LOG.info("base locale remise à zéro")
                    delay = config.scan_interval_seconds
                except Exception as error:
                    state.reset_failed(error)
                    LOG.error("remise à zéro en échec: %s", _safe_error(error))
                    delay = config.cycle_retry_seconds
                wake.wait(delay)
                wake.clear()
                continue
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


def inventory_status(snapshot: Mapping[str, object]) -> str:
    started_at = int(snapshot["last_cycle_started_at"])
    completed_at = int(snapshot["last_cycle_completed_at"])
    requested_at = int(snapshot["cycle_requested_at"])
    if bool(snapshot["cycle_in_progress"]):
        phase = str(snapshot.get("cycle_phase") or "Inventaire en cours")
        return (
            "Inventaire en cours — "
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
*{box-sizing:border-box}html{background:var(--background)}body{margin:0;background:var(--background);color:var(--foreground);font:14px/1.5 Inter,ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}button,input{font:inherit}button,input[type=text],input[type=password]{min-height:40px}button{display:inline-flex;align-items:center;justify-content:center;gap:8px;border:1px solid var(--border);border-radius:var(--radius);background:var(--card);color:var(--foreground);padding:9px 14px;font-weight:600;cursor:pointer;transition:background .15s,border-color .15s,transform .05s}button:hover{background:var(--muted)}button:active{transform:translateY(1px)}button:focus-visible,input:focus-visible{outline:2px solid var(--foreground);outline-offset:2px}.primary{border-color:var(--primary);background:var(--primary);color:var(--primary-foreground)}.primary:hover{background:#27272a}.danger-button{border-color:var(--danger);color:var(--danger)}.danger-button:hover{background:var(--danger-soft)}
.app{min-height:100vh;display:grid;grid-template-rows:64px 1fr;grid-template-columns:224px minmax(0,1fr)}.topbar{grid-column:1/-1;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid var(--border);background:var(--background);padding:0 20px;position:sticky;top:0;z-index:2}.brand{display:flex;align-items:center;gap:10px;font:600 18px/1 ui-monospace,SFMono-Regular,Menlo,monospace}.brand-logo{width:24px;height:22px;fill:currentColor}.connector-chip{border:1px solid var(--border);border-radius:999px;padding:5px 10px;color:var(--muted-foreground);font-size:12px}.sidebar{border-right:1px solid var(--border);background:var(--sidebar);padding:16px}.nav-label{display:block;margin:8px 12px 10px;color:var(--muted-foreground);font-size:11px;font-weight:600;letter-spacing:.08em;text-transform:uppercase}.nav-item{display:flex;align-items:center;gap:10px;border-radius:var(--radius);padding:11px 12px;background:var(--muted);font-size:13px;font-weight:600}.nav-icon{width:18px;height:18px}.main{min-width:0;padding:32px}.content{max-width:1120px;margin:0 auto}.page-heading{display:flex;align-items:flex-start;justify-content:space-between;gap:20px;margin-bottom:24px}.eyebrow{margin:0 0 4px;color:var(--muted-foreground);font-size:12px;font-weight:600}.page-heading h1{margin:0;font-size:24px;line-height:1.25;letter-spacing:-.02em}.page-heading p{margin:7px 0 0;color:var(--muted-foreground)}.toolbar{display:flex;align-items:center;flex-wrap:wrap;gap:8px}.toolbar form{margin:0}
.status-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:20px}.stat-card,.card{border:1px solid var(--border);border-radius:var(--radius);background:var(--card);box-shadow:var(--shadow)}.stat-card{padding:16px}.stat-label{display:flex;align-items:center;gap:7px;color:var(--muted-foreground);font-size:12px;font-weight:600}.dot{width:8px;height:8px;border-radius:50%;background:var(--muted-foreground)}.dot.success{background:var(--success)}.dot.warning{background:#f59e0b}.dot.danger{background:var(--danger)}.stat-value{display:block;margin-top:8px;font-size:22px;font-weight:650;letter-spacing:-.03em}.stat-detail{display:block;margin-top:2px;color:var(--muted-foreground);font-size:12px}.card{margin-bottom:16px;overflow:hidden}.card-header{display:flex;align-items:flex-start;justify-content:space-between;gap:16px;padding:18px 20px;border-bottom:1px solid var(--border)}.card-title{margin:0;font-size:15px}.card-description{margin:3px 0 0;color:var(--muted-foreground);font-size:13px}.card-body{padding:20px}.card-footer{display:flex;justify-content:flex-end;padding:14px 20px;border-top:1px solid var(--border);background:var(--sidebar)}.badge{display:inline-flex;align-items:center;border-radius:999px;background:var(--muted);padding:3px 8px;color:var(--muted-foreground);font-size:11px;font-weight:600}.badge.success{background:var(--success-soft);color:var(--success)}.badge.warning{background:var(--warning-soft);color:var(--warning)}.badge.danger{background:var(--danger-soft);color:var(--danger)}
.inventory-row{display:flex;align-items:center;gap:12px}.inventory-status{display:block;width:100%;min-height:24px;font-weight:600}.inventory-status.running{color:var(--success)}.progress-wrap{margin-top:12px}.progress-track{height:10px;overflow:hidden;border-radius:999px;background:var(--muted)}.progress-bar{height:100%;width:0;border-radius:inherit;background:var(--success);transition:width .25s ease}.progress-bar.indeterminate{width:35%;animation:progress-slide 1.2s ease-in-out infinite}.progress-label{display:block;margin-top:5px;color:var(--muted-foreground);font-size:12px;text-align:right}@keyframes progress-slide{0%{transform:translateX(-110%)}100%{transform:translateX(300%)}}.helper{margin:10px 0 0;color:var(--muted-foreground);font-size:12px}.error-alert{display:flex;gap:10px;margin-bottom:16px;border:1px solid #fecaca;border-radius:var(--radius);background:var(--danger-soft);padding:13px 15px;color:#991b1b}.error-alert strong{display:block}.selection-list{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;max-height:320px;overflow:auto}.selection-item{display:flex;align-items:flex-start;gap:11px;margin:0;border:1px solid var(--border);border-radius:var(--radius);padding:12px;cursor:pointer;transition:background .15s,border-color .15s}.selection-item:hover{background:var(--muted)}.selection-item:has(input:checked){border-color:#a1a1aa;background:var(--muted)}.selection-item input{width:16px;height:16px;margin:2px 0 0;accent-color:var(--primary);flex:0 0 auto}.selection-copy{min-width:0}.selection-title{display:block;font-weight:600;overflow-wrap:anywhere}.selection-meta{display:block;margin-top:2px;color:var(--muted-foreground);font-size:12px;overflow-wrap:anywhere}.empty{grid-column:1/-1;margin:0;border:1px dashed var(--border);border-radius:var(--radius);padding:22px;text-align:center;color:var(--muted-foreground)}.counts{display:flex;flex-wrap:wrap;gap:6px}.operation-grid,.secret-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}.secret-field{display:grid;gap:6px;color:var(--muted-foreground);font-size:12px;font-weight:600}.secret-field input{width:100%;border:1px solid var(--border);border-radius:var(--radius);background:var(--background);color:var(--foreground);padding:8px 10px}.danger-card{border-color:#fecaca}.danger-card .card-header{background:var(--danger-soft)}.confirm-row{display:flex;align-items:end;gap:10px}.confirm-row label{flex:1;margin:0;color:var(--muted-foreground);font-size:12px;font-weight:600}.confirm-row input{display:block;width:100%;margin-top:6px;border:1px solid var(--border);border-radius:var(--radius);background:var(--background);color:var(--foreground);padding:8px 10px}code{border-radius:4px;background:var(--muted);padding:2px 5px;font:12px ui-monospace,SFMono-Regular,Menlo,monospace}.footer-note{padding:8px 0 24px;text-align:center;color:var(--muted-foreground);font-size:12px}
@media(prefers-color-scheme:dark){:root{--background:#18181b;--foreground:#fafafa;--muted:#27272a;--muted-foreground:#a1a1aa;--border:#3f3f46;--card:#18181b;--sidebar:#111113;--primary:#fafafa;--primary-foreground:#09090b;--danger:#f87171;--danger-soft:#2b1719;--success:#34d399;--success-soft:#10251e;--warning:#fbbf24;--warning-soft:#2b2414;--shadow:none}.primary:hover{background:#e4e4e7}.danger-card,.error-alert{border-color:#7f1d1d}.danger-card .card-header{background:var(--danger-soft)}.error-alert{color:#fecaca}.selection-item:has(input:checked){border-color:#71717a}}
@media(max-width:900px){.app{grid-template-columns:1fr;grid-template-rows:64px auto 1fr}.sidebar{border-right:0;border-bottom:1px solid var(--border);padding:8px 16px}.nav-label{display:none}.nav-item{width:max-content;padding:8px 12px}.main{padding:24px 18px}.status-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.operation-grid,.secret-grid{grid-template-columns:1fr}}
@media(max-width:620px){.connector-chip{display:none}.page-heading{display:block}.toolbar{margin-top:16px}.toolbar button{flex:1}.toolbar form{display:flex;flex:1}.selection-list{grid-template-columns:1fr}.status-grid{grid-template-columns:1fr 1fr}.stat-card{padding:13px}.stat-value{font-size:19px}.card-header,.card-body{padding:16px}.card-footer{padding:12px 16px}.card-footer button{width:100%}.confirm-row{align-items:stretch;flex-direction:column}.confirm-row button{width:100%}}
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


def render_live_status(state: RuntimeState) -> str:
    snapshot = state.snapshot()
    return json.dumps(
        {
            "inventory_status": inventory_status(snapshot),
            "cycle_in_progress": bool(snapshot["cycle_in_progress"]),
            "cycle_requested": bool(snapshot["cycle_requested_at"]),
            "cycle_completed_at": int(snapshot["last_cycle_completed_at"]),
            "last_error": str(snapshot["last_error"] or ""),
            "ready": bool(snapshot["ready"]),
            "last_processed": int(snapshot["last_processed"]),
            "progress_current": int(snapshot["progress_current"]),
            "progress_total": int(snapshot["progress_total"]),
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ) + "\n"


UI_SCRIPT = r"""(() => {
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
  if (!badge || !summary || !dot || !button || !completion) return;
  let observedActive = document.body.dataset.cycleActive === "true";
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
      "selected-attachment-mail-count",
      "detailed-attachment-count",
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
  const applyStatus = async status => {
      const active = Boolean(status.cycle_in_progress);
      const requested = Boolean(status.cycle_requested);
      const failed = Boolean(status.last_error);
      observedActive = observedActive || active || requested;
      if (active || requested) completion.hidden = true;
      summary.textContent = status.inventory_status;
      summary.setAttribute("aria-busy", active ? "true" : "false");
      summary.classList.toggle("running", active);
      const stateClass = active ? "success" : (failed ? "danger" : "warning");
      badge.className = "badge " + stateClass;
      dot.className = "dot " + stateClass;
      badge.textContent = active ? "Inventaire en cours…" :
        (requested ? "Inventaire demandé" :
          (failed ? "Inventaire interrompu" : "En attente"));
      button.disabled = active || requested;
      button.type = active || requested ? "button" : "submit";
      button.textContent = active ? "Inventaire en cours…" :
        (requested ? "Inventaire demandé…" : "Relancer l’inventaire");
      if (service) service.textContent = status.ready ? "Prêt" : "Attention requise";
      if (lastSync) {
        lastSync.textContent = status.cycle_completed_at ?
          "Dernière synchro : " + new Date(status.cycle_completed_at * 1000).toLocaleString("fr-FR", {timeZone: "UTC"}) + " UTC" :
          "Dernière synchro : Jamais exécutée";
      }
      if (processed) processed.textContent = status.last_processed + " objet(s) au dernier cycle";
      if (progressWrap && progressBar && progressLabel) {
        const current = Number(status.progress_current || 0);
        const total = Number(status.progress_total || 0);
        progressWrap.hidden = !active;
        progressBar.classList.toggle("indeterminate", active && total <= 0);
        progressBar.style.width = total > 0 ? Math.min(100, current * 100 / total) + "%" : "";
        progressLabel.textContent = total > 0 ? current + " / " + total + " · " + Math.round(current * 100 / total) + " %" : "Préparation…";
        progressWrap.setAttribute("aria-valuemin", "0");
        if (total > 0) {
          progressWrap.setAttribute("aria-valuemax", String(total));
          progressWrap.setAttribute("aria-valuenow", String(Math.min(current, total)));
        } else {
          progressWrap.removeAttribute("aria-valuemax");
          progressWrap.removeAttribute("aria-valuenow");
        }
      }
      if (observedActive && !active && !requested) {
        await refreshInventoryDisplay();
        completion.hidden = false;
        completion.textContent = failed ?
          "Inventaire interrompu. Le détail est affiché ci-dessus ; vos champs et sélections ont été conservés." :
          "Inventaire terminé. Les dossiers et compteurs ont été actualisés ; vos champs et sélections ont été conservés.";
        observedActive = false;
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


def render_status_page(config: Config, state: RuntimeState) -> str:
    snapshot = state.snapshot()
    sources = source_rows(config)
    mailboxes = mailbox_rows(config)
    counts = _status_counts(config)
    selected_counts = _status_counts(config, selected_only=True)
    selected_attachment_mails = selected_mail_with_attachments(config)
    inventory_cache = cached_inventory(config, allow_expired=True)
    paused = is_paused(config)
    csrf = html.escape(str(snapshot["csrf_token"]), quote=True)

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

    error = html.escape(str(snapshot["last_error"] or "aucune"))
    ready = "Prêt" if snapshot["ready"] else "Attention requise"
    pause_label = "Reprendre l’indexation" if paused else "Mettre en pause"
    pause_action = "resume" if paused else "pause"
    activity = "En pause" if paused else "Active"
    concurrency_mode = "auto" if config.ingestion_concurrency is None else "fixe"
    effective_concurrency = int(snapshot["ingestion_concurrency_effective"])
    detected_workers = int(snapshot["docling_workers_detected"])
    worker_label = (
        str(detected_workers)
        if snapshot["worker_detection_success"]
        else "indisponible"
    )
    inventory_running = bool(snapshot["cycle_in_progress"])
    inventory_requested = bool(snapshot["cycle_requested_at"])
    if inventory_running:
        inventory_activity = "Inventaire en cours…"
        inventory_class = "success running"
        scan_button = '<button id="inventory-button" class="primary" type="button" disabled>Inventaire en cours…</button>'
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
    attachment_total = sum(selected_counts["attachments"].values())
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
    openarchiver_key_state = (
        "Configurée"
        if secret_is_configured(config.openarchiver_api_key_file)
        else "Absente"
    )
    openrag_key_state = (
        "Configurée" if secret_is_configured(config.openrag_api_key_file) else "Absente"
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
    if snapshot["reset_requested_at"]:
        reset_status = (
            "Remise à zéro demandée ; attente de la fin des opérations en cours."
        )
    elif snapshot["last_reset_at"]:
        reset_status = "Dernière remise à zéro : " + time.strftime(
            "%Y-%m-%d %H:%M:%S UTC", time.gmtime(int(snapshot["last_reset_at"]))
        )
    else:
        reset_status = "Aucune remise à zéro depuis le démarrage."
    return f"""<!doctype html>
<html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light dark">
<title>Connecteur OpenArchiver · OpenRAG</title>
<style>{STATUS_PAGE_STYLE}</style><script src="/ui.js" defer></script></head>
<body data-cycle-active="{str(inventory_running or inventory_requested).lower()}" data-cycle-completed="{last_completed}">
<div class="app">
<header class="topbar"><div class="brand">{OPENRAG_LOGO}<span>OpenRAG</span></div>
<span class="connector-chip">OpenArchiver connector</span></header>
<aside class="sidebar" aria-label="Navigation"><span class="nav-label">Intégrations</span>
<div class="nav-item"><svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><path d="M8 12h8M12 8v8"/><path d="M7 7.5 4.5 5M17 7.5 19.5 5M7 16.5 4.5 19M17 16.5 19.5 19"/><rect x="7" y="7" width="10" height="10" rx="2"/></svg>Connecteur</div></aside>
<main class="main"><div class="content">
<div class="page-heading"><div><p class="eyebrow">Connecteurs / OpenArchiver</p>
<h1>OpenArchiver vers OpenRAG</h1><p>Configurez et supervisez l’indexation de vos archives e-mail.</p></div>
<div class="toolbar"><form method="get" action="/"><button type="submit">↻&nbsp; Rafraîchir l’état</button></form>
<form method="post" action="/pause"><input type="hidden" name="csrf" value="{csrf}"><input type="hidden" name="action" value="{pause_action}"><button type="submit">{pause_label}</button></form></div></div>
{error_alert}
<section class="status-grid" aria-label="État du connecteur">
<div class="stat-card"><span class="stat-label"><span class="dot {status_class}"></span>Service</span><strong id="service-status" class="stat-value">{ready}</strong><span id="last-sync" class="stat-detail">Dernière synchro : {last_sync}</span></div>
<div class="stat-card"><span class="stat-label"><span class="dot {activity_class}"></span>Indexation</span><strong class="stat-value">{activity}</strong><span id="last-processed" class="stat-detail">{int(snapshot["last_processed"])} objet(s) au dernier cycle · concurrence {concurrency_mode} : {effective_concurrency} · workers Docling : {worker_label}</span></div>
<div class="stat-card"><span class="stat-label">Mails dans la sélection</span><strong id="mail-count" class="stat-value">{email_total}</strong><span id="mailbox-selected-count" class="stat-detail">{selected_mailboxes} dossier(s) sélectionné(s)</span></div>
<div class="stat-card"><span class="stat-label">Mails avec pièces jointes</span><strong id="selected-attachment-mail-count" class="stat-value">{selected_attachment_mails}</strong><span id="detailed-attachment-count" class="stat-detail">{attachment_total} pièce(s) déjà détaillée(s)</span></div>
</section>
<section class="card"><div class="card-header"><div><h2 class="card-title">Inventaire IMAP</h2><p class="card-description">État du cycle de découverte des sources et dossiers.</p></div><span id="inventory-badge" class="badge {inventory_class}">{inventory_activity}</span></div>
<div class="card-body"><div class="inventory-row"><span id="inventory-dot" class="dot {inventory_class}"></span><span id="inventory-summary" class="inventory-status" role="status" aria-live="polite" aria-busy="{str(inventory_running).lower()}">{html.escape(inventory_status(snapshot))}</span></div>
<div id="cycle-progress" class="progress-wrap" role="progressbar" aria-label="Progression du cycle" hidden><div class="progress-track"><div id="cycle-progress-bar" class="progress-bar"></div></div><span id="cycle-progress-label" class="progress-label">Préparation…</span></div>
<p id="inventory-completion" class="helper" role="status" hidden></p>
<p id="inventory-cache-label" class="helper">{html.escape(inventory_cache_label)}</p>
<p class="helper">Les archives sont traitées comme un instantané stable. Utilisez « Relancer l’inventaire » pour rechercher explicitement de nouveaux éléments.</p>
<p class="helper">La pause bloque les envois vers OpenRAG, mais autorise l’inventaire des sources et dossiers IMAP.</p>
<div id="email-status-counts" class="counts" aria-label="États des mails de la sélection"><span class="badge success">Sélection actuelle</span>{count_badges(selected_counts["emails"], "aucun mail")}</div>
<p id="history-summary" class="helper">Historique local conservé : {historical_email_total} mail(s) ; {historical_attachment_total} pièce(s) jointe(s) déjà détaillée(s). Les éléments hors sélection ne sont pas envoyés à OpenRAG.</p></div>
<div class="card-footer"><form method="post" action="/scan"><input type="hidden" name="csrf" value="{csrf}">{scan_button}</form></div></section>
<form method="post" action="/secrets" class="card" autocomplete="off"><div class="card-header"><div><h2 class="card-title">Clés API</h2><p class="card-description">Renouvelez séparément les accès OpenArchiver et OpenRAG.</p></div><span class="badge">OpenArchiver : {openarchiver_key_state} · OpenRAG : {openrag_key_state}</span></div>
<div class="card-body"><input type="hidden" name="csrf" value="{csrf}"><div class="secret-grid"><label class="secret-field">Nouvelle clé OpenArchiver<input type="password" name="openarchiver_key" autocomplete="new-password"></label><label class="secret-field">Nouvelle clé OpenRAG<input type="password" name="openrag_key" autocomplete="new-password"></label></div><p class="helper">Laissez un champ vide pour conserver sa valeur actuelle. Les clés ne sont jamais réaffichées ni enregistrées dans SQLite.</p></div>
<div class="card-footer"><button class="primary" type="submit">Enregistrer les clés renseignées</button></div></form>
<form method="post" action="/sources" class="card"><div class="card-header"><div><h2 class="card-title">Sources indexées</h2><p class="card-description">Choisissez les comptes OpenArchiver à rendre disponibles dans OpenRAG.</p></div><span class="badge">{selected_sources}/{len(sources)} sélectionnée(s)</span></div>
<div class="card-body"><input type="hidden" name="csrf" value="{csrf}"><div class="selection-list">{"".join(source_lines)}</div></div>
<div class="card-footer"><button class="primary" type="submit">Enregistrer et lancer l’inventaire</button></div></form>
<form method="post" action="/mailboxes" class="card"><div class="card-header"><div><h2 class="card-title">Dossiers IMAP indexés</h2><p class="card-description">Affinez l’indexation aux dossiers utiles de chaque source.</p></div><span id="mailbox-selection-badge" class="badge">{selected_mailboxes}/{len(mailboxes)} sélectionné(s)</span></div>
<div class="card-body"><input type="hidden" name="csrf" value="{csrf}"><div id="mailbox-selection-list" class="selection-list">{"".join(mailbox_lines)}</div><div id="attachment-status-counts" class="counts" aria-label="États des pièces jointes détaillées de la sélection" style="margin-top:14px"><span class="badge success">Pièces jointes détaillées</span>{count_badges(selected_counts["attachments"], "pas encore détaillées")}</div></div>
<div class="card-footer"><button class="primary" type="submit">Enregistrer les dossiers</button></div></form>
<section class="card danger-card"><div class="card-header"><div><h2 class="card-title">Remise à zéro</h2><p class="card-description">Efface l’inventaire, les sélections et l’historique local uniquement.</p></div><span class="badge danger">Zone sensible</span></div>
<div class="card-body"><p>Aucun mail OpenArchiver ni document OpenRAG n’est supprimé. Le connecteur reste en pause.</p><p class="helper">{html.escape(reset_status)}</p>
<form method="post" action="/reset" class="confirm-row"><input type="hidden" name="csrf" value="{csrf}"><label>Saisir <code>RESET</code> pour confirmer<input type="text" name="confirmation" required pattern="RESET" autocomplete="off"></label><button class="danger-button" type="submit">Remettre la base locale à zéro</button></form></div></section>
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
        "# TYPE openarchiver_connector_docling_workers_detected gauge",
        f"openarchiver_connector_docling_workers_detected {snapshot['docling_workers_detected']}",
        "# TYPE openarchiver_connector_worker_detection_success gauge",
        f"openarchiver_connector_worker_detection_success {1 if snapshot['worker_detection_success'] else 0}",
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
) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "OpenArchiverConnector/1"

        def _send(self, status: int, body: str, content_type: str) -> None:
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
            self.end_headers()
            self.wfile.write(payload)

        def _redirect(self) -> None:
            self.send_response(303)
            self.send_header("Location", "/")
            self.send_header("Content-Length", "0")
            self.end_headers()

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
                        payload = render_live_status(state).rstrip("\n")
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
            path = urllib.parse.urlsplit(self.path).path
            try:
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
                        render_live_status(state),
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
                    body = render_status_page(config, state)
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
                    self._send(403, "forbidden\n", "text/plain; charset=utf-8")
                    return
                if path == "/secrets":
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
                    self._redirect()
                elif path == "/sources":
                    replace_source_selection(config, form.get("source_id", []))
                    state.cycle_requested(force_inventory=True)
                    wake.set()
                    self._redirect()
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
                    self._redirect()
                elif path == "/pause":
                    action = form.get("action", [""])[0]
                    if action not in {"pause", "resume"}:
                        raise ConnectorError("action de pause invalide")
                    set_paused(config, action == "pause")
                    if action == "resume":
                        state.cycle_requested()
                        wake.set()
                    self._redirect()
                elif path == "/scan":
                    state.cycle_requested(force_inventory=True)
                    wake.set()
                    self._redirect()
                elif path == "/reset":
                    if form.get("confirmation", [""])[0] != "RESET":
                        raise ConnectorError("confirmation de remise à zéro absente")
                    # La pause empêche les workers de réserver un nouvel objet.
                    # Le thread runtime exécute ensuite le reset, après la fin
                    # des quelques tâches qui étaient déjà en cours.
                    set_paused(config, True)
                    state.reset_requested()
                    wake.set()
                    self._redirect()
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
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    config = Config.from_env()
    with database(config):
        pass

    STOP.clear()
    WAKE.clear()
    state = RuntimeState()
    restore_cycle_outcome(config, state)

    def stop_service(_signum: int, _frame: object) -> None:
        STOP.set()
        WAKE.set()

    signal.signal(signal.SIGTERM, stop_service)
    signal.signal(signal.SIGINT, stop_service)
    worker = threading.Thread(
        target=runtime_loop,
        args=(config, state),
        name="openarchiver-cycle",
        daemon=True,
    )
    worker.start()
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
        server.server_close()
        worker.join(timeout=5)


if __name__ == "__main__":
    main()
