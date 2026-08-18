"""Cœur du connecteur OpenArchiver vers OpenRAG.

Ce que l'on veut : sélectionner des sources OpenArchiver et indexer dans
OpenRAG un Markdown stable par mail ainsi que chaque pièce jointe compatible.
Pourquoi : rendre les archives interrogeables sans coupler les deux projets ni
recharger les contenus déjà connus à chacun des scans.
Comment : parcourir l'API OpenArchiver de manière conservative, conserver les
identités et la file dans SQLite, puis utiliser l'API publique OpenRAG.
Compatibilité : le protocole multipart, le suivi des tâches et les réservations
SQLite reprennent les principes éprouvés du connecteur NAS sans le modifier.
KISS : un module Python standard, un fichier SQLite, aucun fork, broker,
framework web, accès PostgreSQL, montage NFS ou accès Kubernetes.

Hypothèse à confirmer au runtime : ``replace_duplicates=true`` remplace bien
le document OpenRAG portant le même nom. Aucun appel DELETE n'est implémenté.
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
import unicodedata
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
DEFAULT_EXTENSIONS = (
    ".asc,.asciidoc,.adoc,.csv,.docx,.htm,.html,.md,.pdf,.txt,.xlsx"
)
SAFE_EXTENSION = re.compile(r"^\.[a-z0-9]{1,10}$")
STOP = threading.Event()
WAKE = threading.Event()


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
    scan_interval_seconds: int = 3600
    task_timeout_seconds: int = 3600
    max_file_bytes: int = 104_857_600
    max_auto_retries: int = 3
    retry_base_seconds: int = 300
    retry_max_seconds: int = 3600
    supported_extensions: frozenset[str] = frozenset(DEFAULT_EXTENSIONS.split(","))
    openarchiver_requests_per_minute: int = 90
    ingestion_concurrency: int = 2
    ingestion_concurrency_max: int = 4
    page_limit: int = 250
    openarchiver_link_template: str = ""
    request_timeout_seconds: int = 30
    cycle_retry_seconds: int = 60
    http_host: str = "0.0.0.0"
    http_port: int = 8080

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "Config":
        values = os.environ if env is None else env
        extensions = frozenset(
            _normalise_extension(item)
            for item in values.get("SUPPORTED_EXTENSIONS", DEFAULT_EXTENSIONS).split(",")
            if item.strip()
        )
        concurrency_max = max(1, int(values.get("INGESTION_CONCURRENCY_MAX", "4")))
        config = cls(
            openarchiver_base_url=values.get(
                "OPENARCHIVER_BASE_URL",
                "http://openarchiver.openarchiver.svc.cluster.local:3000/api/v1",
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
                "OPENRAG_INGEST_PATH", "/v1/documents/ingest"
            ),
            openrag_task_path=values.get(
                "OPENRAG_TASK_PATH", "/v1/tasks/{task_id}/enhanced"
            ),
            openrag_api_key_file=Path(
                values.get("OPENRAG_API_KEY_FILE", "/var/run/secrets/openrag/api-key")
            ),
            state_db=Path(values.get("STATE_DB", "/state/connector.sqlite3")),
            scan_interval_seconds=max(60, int(values.get("SCAN_INTERVAL_SECONDS", "3600"))),
            task_timeout_seconds=max(1, int(values.get("TASK_TIMEOUT_SECONDS", "3600"))),
            max_file_bytes=max(1, int(values.get("MAX_FILE_BYTES", "104857600"))),
            max_auto_retries=max(1, int(values.get("MAX_AUTO_RETRIES", "3"))),
            retry_base_seconds=max(1, int(values.get("RETRY_BASE_SECONDS", "300"))),
            retry_max_seconds=max(1, int(values.get("RETRY_MAX_SECONDS", "3600"))),
            supported_extensions=extensions,
            openarchiver_requests_per_minute=max(
                1, int(values.get("OPENARCHIVER_REQUESTS_PER_MINUTE", "90"))
            ),
            ingestion_concurrency=min(
                concurrency_max, max(1, int(values.get("INGESTION_CONCURRENCY", "2")))
            ),
            ingestion_concurrency_max=concurrency_max,
            page_limit=max(1, int(values.get("OPENARCHIVER_PAGE_LIMIT", "250"))),
            openarchiver_link_template=values.get("OPENARCHIVER_LINK_TEMPLATE", ""),
            request_timeout_seconds=max(
                1, int(values.get("REQUEST_TIMEOUT_SECONDS", "30"))
            ),
            cycle_retry_seconds=max(
                5, int(values.get("CYCLE_RETRY_SECONDS", "60"))
            ),
            http_host=values.get("HTTP_HOST", "0.0.0.0"),
            http_port=min(65_535, max(1, int(values.get("HTTP_PORT", "8080")))),
        )
        _validate_internal_http_url(config.openarchiver_base_url, "OPENARCHIVER_BASE_URL")
        _validate_internal_http_url(config.openrag_base_url, "OPENRAG_BASE_URL")
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


def read_secret(path: Path, label: str) -> str:
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise ConnectorError(f"le fichier de clé {label} est vide")
    return value


def connect_db(config: Config) -> sqlite3.Connection:
    """Crée et migre idempotemment l'état local."""
    config.state_db.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(config.state_db, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=30000")
    db.execute("PRAGMA journal_mode=WAL")
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


def replace_source_selection(config: Config, source_ids: Iterable[str]) -> list[str]:
    """Remplace atomiquement la sélection par des sources déjà découvertes."""
    requested = sorted(set(source_ids))
    with database(config) as db:
        known = {
            str(row[0])
            for row in db.execute("SELECT id FROM sources")
        }
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
                raise ConnectorError("appel OpenArchiver temporairement indisponible") from None
        raise ConnectorError("appel OpenArchiver épuisé")

    def _backoff(self, attempt: int) -> float:
        return float(
            min(
                self.config.retry_max_seconds,
                self.config.retry_base_seconds * (2**attempt),
            )
        )

    def json(self, path: str) -> object:
        response = self._open(path)
        try:
            raw = response.read()
        finally:
            response.close()
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
        payload = self.json(
            "/archived-emails/" + urllib.parse.quote(email_id, safe="")
        )
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


def refresh_sources(config: Config, client: OpenArchiverClient) -> list[dict[str, object]]:
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
    config: Config, client: OpenArchiverClient, source_id: str
) -> tuple[dict[str, dict[str, object]], int]:
    found: dict[str, dict[str, object]] = {}
    page = 1
    announced_total: int | None = None
    while True:
        payload = client.list_emails(source_id, page, config.page_limit)
        items = payload.get("items")
        total = payload.get("total")
        if not isinstance(items, list) or not isinstance(total, int) or total < 0:
            raise IncompleteScanError(f"pagination invalide pour la source {source_id}")
        response_limit = payload.get("limit", config.page_limit)
        if not isinstance(response_limit, int) or response_limit < 1:
            raise IncompleteScanError(f"limite de pagination invalide pour la source {source_id}")
        if announced_total is None:
            announced_total = total
        elif total != announced_total:
            raise IncompleteScanError(f"total instable pour la source {source_id}")
        for raw in items:
            email = _validate_email(_require_object(raw, "mail"), source_id)
            found[str(email["id"])] = email
        if page * response_limit >= total:
            break
        if not items:
            break
        page += 1
    return found, announced_total or 0


def _stable_source_inventory(
    config: Config, client: OpenArchiverClient, source_id: str
) -> tuple[dict[str, dict[str, object]], bool]:
    repeated = False
    for attempt in range(2):
        try:
            found, total = _scan_source_pass(config, client, source_id)
            coherent = len(found) == total
        except IncompleteScanError:
            found, coherent = {}, False
        if coherent:
            return found, repeated
        repeated = True
        if attempt == 1:
            raise IncompleteScanError(
                f"inventaire incohérent après deux passages pour la source {source_id}"
            )
    raise AssertionError("boucle de stabilisation invalide")


def scan_selected_sources(config: Config, client: OpenArchiverClient) -> ScanResult:
    source_ids = selected_source_ids(config)
    # Le marqueur nanoseconde évite de confondre deux scans lancés dans la même
    # seconde lors du classement conservatif des absences.
    scan_started = time.time_ns()
    global_emails: dict[str, dict[str, object]] = {}
    repeated = False
    try:
        for source_id in source_ids:
            found, source_repeated = _stable_source_inventory(config, client, source_id)
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
        for email in global_emails.values():
            _upsert_email(db, email, scan_started)
        if source_ids:
            placeholders = ",".join("?" for _ in source_ids)
            db.execute(
                f"""
                UPDATE emails SET status='missing'
                WHERE source_id IN ({placeholders}) AND last_seen_at < ?
                  AND status NOT IN ('downloading','ingesting')
                """,
                (*source_ids, scan_started),
            )
        db.execute(
            "INSERT OR REPLACE INTO settings(key,value) VALUES ('last_scan_error','')"
        )
    return ScanResult(len(source_ids), len(global_emails), True, repeated)


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
            result.append(f"{name} <{address}>" if name and address else address or name)
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
            id, source_id, thread_id, sent_at, subject, sender_name,
            sender_email, recipients_json, cc_json, message_id, storage_path,
            storage_hash, size_bytes, has_attachments, fingerprint,
            openrag_filename, status, first_seen_at, last_seen_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            source_id=excluded.source_id, thread_id=excluded.thread_id,
            sent_at=excluded.sent_at, subject=excluded.subject,
            sender_name=excluded.sender_name, sender_email=excluded.sender_email,
            recipients_json=excluded.recipients_json, cc_json=excluded.cc_json,
            message_id=excluded.message_id, storage_path=excluded.storage_path,
            storage_hash=excluded.storage_hash, size_bytes=excluded.size_bytes,
            has_attachments=excluded.has_attachments,
            sha256=CASE
                WHEN emails.fingerprint <> excluded.fingerprint THEN ''
                ELSE emails.sha256 END,
            status=CASE
                WHEN emails.fingerprint <> excluded.fingerprint THEN 'queued'
                WHEN emails.status IN ('missing','unavailable') THEN 'queued'
                ELSE emails.status END,
            attempts=CASE WHEN emails.fingerprint <> excluded.fingerprint THEN 0 ELSE emails.attempts END,
            next_retry_at=CASE WHEN emails.fingerprint <> excluded.fingerprint THEN 0 ELSE emails.next_retry_at END,
            last_error=CASE WHEN emails.fingerprint <> excluded.fingerprint THEN '' ELSE emails.last_error END,
            task_id=CASE WHEN emails.fingerprint <> excluded.fingerprint THEN '' ELSE emails.task_id END,
            fingerprint=excluded.fingerprint, last_seen_at=excluded.last_seen_at
        """,
        (
            email["id"],
            email["source_id"],
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
    return f"openarchiver-mail-{_safe_identifier(email_id)}.md"


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


class OpenRAGClient:
    def __init__(self, config: Config, *, sleeper: Callable[[float], None] = time.sleep) -> None:
        self.config = config
        self.sleeper = sleeper

    def upload(self, path: Path, remote_name: str) -> str:
        parsed = urllib.parse.urlsplit(self.config.openrag_base_url)
        boundary = f"openarchiver-{uuid.uuid4().hex}"
        content_type = mimetypes.guess_type(remote_name)[0] or "application/octet-stream"
        prefix = (
            f"--{boundary}\r\n"
            'Content-Disposition: form-data; name="replace_duplicates"\r\n\r\n'
            "true\r\n"
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="file"; filename="{remote_name}"\r\n'
            f"Content-Type: {content_type}\r\n\r\n"
        ).encode("utf-8")
        suffix = f"\r\n--{boundary}--\r\n".encode("ascii")
        connection = http.client.HTTPConnection(
            parsed.hostname, parsed.port or 80, timeout=self.config.request_timeout_seconds
        )
        target = (parsed.path.rstrip("/") + self.config.openrag_ingest_path) or "/"
        try:
            connection.putrequest("POST", target)
            connection.putheader(
                "X-API-Key", read_secret(self.config.openrag_api_key_file, "OpenRAG")
            )
            connection.putheader("Accept", "application/json")
            connection.putheader("Content-Type", f"multipart/form-data; boundary={boundary}")
            connection.putheader("Content-Length", str(len(prefix) + path.stat().st_size + len(suffix)))
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
        for kind, table in (("email", "emails"), ("attachment", "attachments")):
            selection_clause = (
                """EXISTS (
                       SELECT 1 FROM sources s
                       WHERE s.id=emails.source_id AND s.selected=1
                   )"""
                if kind == "email"
                else """EXISTS (
                       SELECT 1 FROM email_attachments ea
                       JOIN emails e ON e.id=ea.email_id
                       JOIN sources s ON s.id=e.source_id
                       WHERE ea.attachment_id=attachments.id AND s.selected=1
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


def _rows_for_email(config: Config, email_id: str) -> tuple[sqlite3.Row, list[sqlite3.Row], str]:
    with database(config) as db:
        email_row = db.execute("SELECT * FROM emails WHERE id=?", (email_id,)).fetchone()
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
        source = db.execute("SELECT name FROM sources WHERE id=?", (email_row["source_id"],)).fetchone()
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
                row = db.execute("SELECT * FROM emails WHERE id=?", (item.object_id,)).fetchone()
            if row is None:
                raise ConnectorError("mail réservé introuvable")
            if int(row["has_attachments"]):
                detail = openarchiver.email_detail(item.object_id)
                inventory_attachments(config, item.object_id, detail)
            with tempfile.TemporaryDirectory(prefix="openarchiver-mail-") as directory:
                eml = Path(directory) / "message.eml"
                _size, sha256 = openarchiver.download(str(row["storage_path"]), eml)
                _set_object_state(
                    config, "email", item.object_id, "sha256=?", (sha256,)
                )
                body, headers = parse_eml(eml)
                current, attachments, source_name = _rows_for_email(config, item.object_id)
                markdown_row = dict(current)
                if not markdown_row["subject"]:
                    markdown_row["subject"] = headers["subject"]
                if not markdown_row["sent_at"]:
                    markdown_row["sent_at"] = headers["date"]
                if not markdown_row["message_id"]:
                    markdown_row["message_id"] = headers["message_id"]
                if not markdown_row["sender_email"] and headers["from"]:
                    markdown_row["sender_email"] = ", ".join(headers["from"])
                if markdown_row["recipients_json"] == "[]" and headers["to"]:
                    markdown_row["recipients_json"] = json.dumps(
                        headers["to"], ensure_ascii=False
                    )
                if markdown_row["cc_json"] == "[]" and headers["cc"]:
                    markdown_row["cc_json"] = json.dumps(headers["cc"], ensure_ascii=False)
                markdown = render_mail_markdown(
                    markdown_row,
                    body,
                    attachments,
                    source_name=source_name,
                    link_template=config.openarchiver_link_template,
                )
                document = Path(directory) / str(current["openrag_filename"])
                document.write_text(markdown, encoding="utf-8")
                _set_object_state(config, "email", item.object_id, "status='ingesting'", ())
                task_id = openrag.upload(document, str(current["openrag_filename"]))
                _set_object_state(config, "email", item.object_id, "task_id=?", (task_id,))
                openrag.wait(task_id)
        else:
            with database(config) as db:
                row = db.execute("SELECT * FROM attachments WHERE id=?", (item.object_id,)).fetchone()
            if row is None:
                raise ConnectorError("pièce jointe réservée introuvable")
            with tempfile.TemporaryDirectory(prefix="openarchiver-attachment-") as directory:
                path = Path(directory) / str(row["openrag_filename"])
                size, sha256 = openarchiver.download(str(row["storage_path"]), path)
                if size > config.max_file_bytes:
                    raise FileTooLargeError("fichier OpenArchiver trop volumineux")
                _set_object_state(
                    config, "attachment", item.object_id, "sha256=?, status='ingesting'", (sha256,)
                )
                task_id = openrag.upload(path, str(row["openrag_filename"]))
                _set_object_state(config, "attachment", item.object_id, "task_id=?", (task_id,))
                openrag.wait(task_id)
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
    return error.__class__.__name__


def process_queue(
    config: Config,
    openarchiver: OpenArchiverClient,
    openrag: OpenRAGClient,
) -> int:
    workers = min(config.ingestion_concurrency, config.ingestion_concurrency_max)

    def worker() -> int:
        processed = 0
        while True:
            item = claim_next(config)
            if item is None:
                return processed
            process_work_item(config, item, openarchiver, openrag)
            processed += 1

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="openarchiver-ingest") as pool:
        return sum(pool.map(lambda _index: worker(), range(workers)))


class RuntimeState:
    """Petit état mémoire pour les probes et l'interface d'exploitation."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.csrf_token = uuid.uuid4().hex
        self.started_at = int(time.time())
        self.last_cycle_started_at = 0
        self.last_cycle_completed_at = 0
        self.last_error = ""
        self.last_scan: ScanResult | None = None
        self.last_processed = 0
        self.ready = False
        self.running = False

    def set_running(self, value: bool) -> None:
        with self.lock:
            self.running = value

    def cycle_started(self) -> None:
        with self.lock:
            self.last_cycle_started_at = int(time.time())

    def cycle_succeeded(self, scan: ScanResult, processed: int) -> None:
        with self.lock:
            self.last_cycle_completed_at = int(time.time())
            self.last_error = ""
            self.last_scan = scan
            self.last_processed = processed
            self.ready = True

    def cycle_failed(self, error: Exception) -> None:
        with self.lock:
            self.last_cycle_completed_at = int(time.time())
            self.last_error = _safe_error(error)
            self.ready = False

    def snapshot(self) -> dict[str, object]:
        with self.lock:
            return {
                "csrf_token": self.csrf_token,
                "started_at": self.started_at,
                "last_cycle_started_at": self.last_cycle_started_at,
                "last_cycle_completed_at": self.last_cycle_completed_at,
                "last_error": self.last_error,
                "last_scan": self.last_scan,
                "last_processed": self.last_processed,
                "ready": self.ready,
                "running": self.running,
            }


def run_cycle(
    config: Config,
    openarchiver: OpenArchiverClient,
    openrag: OpenRAGClient,
) -> tuple[ScanResult, int]:
    refresh_sources(config, openarchiver)
    scan = scan_selected_sources(config, openarchiver)
    if not scan.complete:
        raise IncompleteScanError("inventaire incomplet; ingestion différée")
    processed = process_queue(config, openarchiver, openrag)
    return scan, processed


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
            state.cycle_started()
            try:
                scan, processed = run_cycle(config, archive_client, rag_client)
                state.cycle_succeeded(scan, processed)
                LOG.info(
                    "cycle terminé: sources=%d mails=%d traités=%d",
                    scan.sources,
                    scan.emails,
                    processed,
                )
                delay = config.scan_interval_seconds
            except Exception as error:
                state.cycle_failed(error)
                LOG.error("cycle en échec: %s", _safe_error(error))
                delay = config.cycle_retry_seconds
            wake.wait(delay)
            wake.clear()
    finally:
        state.set_running(False)


def _status_counts(config: Config) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {"emails": {}, "attachments": {}}
    with database(config) as db:
        for table in result:
            for row in db.execute(
                f"SELECT status, COUNT(*) AS count FROM {table} GROUP BY status"
            ):
                result[table][str(row["status"])] = int(row["count"])
    return result


def render_status_page(config: Config, state: RuntimeState) -> str:
    snapshot = state.snapshot()
    sources = source_rows(config)
    counts = _status_counts(config)
    csrf = html.escape(str(snapshot["csrf_token"]), quote=True)

    source_lines = []
    for row in sources:
        source_id = html.escape(str(row["id"]), quote=True)
        label = html.escape(str(row["name"] or row["id"]))
        provider = html.escape(str(row["provider"] or "—"))
        checked = " checked" if int(row["selected"]) else ""
        source_lines.append(
            f'<label><input type="checkbox" name="source_id" value="{source_id}"'
            f"{checked}> <strong>{label}</strong> "
            f'<span class="muted">({provider}, {source_id})</span></label>'
        )
    if not source_lines:
        source_lines.append(
            '<p class="muted">Aucune source découverte. '
            "Vérifier le Secret et OpenArchiver.</p>"
        )

    def count_lines(table: str) -> str:
        values = counts[table]
        if not values:
            return '<span class="muted">aucun objet</span>'
        return ", ".join(
            f"{html.escape(status)}={count}" for status, count in sorted(values.items())
        )

    error = html.escape(str(snapshot["last_error"] or "aucune"))
    ready = "prêt" if snapshot["ready"] else "non prêt"
    return f"""<!doctype html>
<html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>OpenArchiver vers OpenRAG</title>
<style>
body{{font-family:system-ui,sans-serif;max-width:960px;margin:2rem auto;padding:0 1rem;color:#202124}}
fieldset{{border:1px solid #ccc;border-radius:.5rem;padding:1rem}}label{{display:block;margin:.6rem 0}}
button{{padding:.55rem .9rem;margin:.5rem .5rem 0 0}}code{{background:#f2f2f2;padding:.1rem .3rem}}
.muted{{color:#666}}.error{{color:#9b1c1c}}dt{{font-weight:700}}dd{{margin-bottom:.5rem}}
</style></head><body>
<h1>Connecteur OpenArchiver → OpenRAG</h1>
<p>État runtime : <strong>{ready}</strong></p>
<dl><dt>Dernière erreur</dt><dd class="error">{error}</dd>
<dt>Mails</dt><dd>{count_lines('emails')}</dd>
<dt>Pièces jointes</dt><dd>{count_lines('attachments')}</dd></dl>
<form method="post" action="/sources"><fieldset><legend>Sources indexées</legend>
<input type="hidden" name="csrf" value="{csrf}">
{''.join(source_lines)}
<button type="submit">Enregistrer la sélection et scanner</button></fieldset></form>
<form method="post" action="/scan"><input type="hidden" name="csrf" value="{csrf}">
<button type="submit">Relancer un cycle</button></form>
<p class="muted">Aucune suppression OpenRAG n'est automatique.
Interface prévue pour un port-forward.</p>
</body></html>"""


def render_metrics(config: Config, state: RuntimeState) -> str:
    snapshot = state.snapshot()
    counts = _status_counts(config)
    selected = len(selected_source_ids(config))
    lines = [
        "# TYPE openarchiver_connector_ready gauge",
        f"openarchiver_connector_ready {1 if snapshot['ready'] else 0}",
        "# TYPE openarchiver_connector_selected_sources gauge",
        f"openarchiver_connector_selected_sources {selected}",
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
                "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'",
            )
            self.end_headers()
            self.wfile.write(payload)

        def _redirect(self) -> None:
            self.send_response(303)
            self.send_header("Location", "/")
            self.send_header("Content-Length", "0")
            self.end_headers()

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
                    ready = bool(state.snapshot()["ready"])
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
                elif path == "/":
                    self._send(
                        200,
                        render_status_page(config, state),
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
                if path == "/sources":
                    replace_source_selection(config, form.get("source_id", []))
                    wake.set()
                    self._redirect()
                elif path == "/scan":
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
    read_secret(config.openarchiver_api_key_file, "OpenArchiver")
    read_secret(config.openrag_api_key_file, "OpenRAG")
    with database(config):
        pass

    STOP.clear()
    WAKE.clear()
    state = RuntimeState()

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
