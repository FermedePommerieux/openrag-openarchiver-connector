import hashlib
import importlib.util
import io
import json
import logging
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
from email.message import EmailMessage
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).parents[1] / "connector.py"
SPEC = importlib.util.spec_from_file_location("openarchiver_connector", MODULE_PATH)
connector = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = connector
SPEC.loader.exec_module(connector)


class Response:
    def __init__(self, body=b"", status=200, chunks=None, headers=None):
        self.body = io.BytesIO(body)
        self.status = status
        self.chunks = iter(chunks) if chunks is not None else None
        self.headers = headers or {}

    def read(self, size=-1):
        if self.chunks is not None:
            try:
                return next(self.chunks)
            except StopIteration:
                return b""
        return self.body.read(size)

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


class FakeArchive:
    def __init__(self, pages=None, sources=None, details=None, downloads=None):
        self.pages = pages or {}
        self.sources = sources or []
        self.details = details or {}
        self.downloads = downloads or {}
        self.calls = []

    def list_sources(self):
        return self.sources

    def list_emails(self, source_id, page, limit):
        self.calls.append((source_id, page, limit))
        value = self.pages[(source_id, page)]
        if isinstance(value, list):
            return value.pop(0)
        return value

    def email_detail(self, email_id):
        return self.details[email_id]

    def download(self, storage_path, destination):
        data = self.downloads[storage_path]
        destination.write_bytes(data)
        import hashlib

        return len(data), hashlib.sha256(data).hexdigest()


class FakeOpenRAG:
    def __init__(self, fail=False, indexed=True):
        self.fail = fail
        self.indexed = indexed
        self.uploads = []

    def upload(self, path, remote_name):
        self.uploads.append((remote_name, path.read_bytes()))
        if self.fail:
            raise connector.ConnectorError("ingestion OpenRAG indisponible")
        return "task-1"

    def ingest_path(self, path):
        return self.upload(path, path.name)

    def ingest_source(self, path, source_url=None):
        return self.ingest_path(path), False

    def wait(self, task_id):
        if self.fail:
            raise connector.ConnectorError("tâche OpenRAG failed")

    def document_is_indexed(self, filename, sha256, *, attempts=3):
        return self.indexed


class ConnectorTests(unittest.TestCase):
    def config(self, root, **overrides):
        oa_key = root / "oa-key"
        rag_key = root / "rag-key"
        oa_key.write_text("oa-secret", encoding="utf-8")
        rag_key.write_text("rag-secret", encoding="utf-8")
        values = {
            "openarchiver_base_url": "http://openarchiver.openarchiver.svc.cluster.local:3000/api/v1",
            "openarchiver_api_key_file": oa_key,
            "openrag_base_url": "http://openrag-backend:8000",
            "openrag_ingest_path": "/v1/documents/ingest-path",
            "openrag_ingest_directory": root / "openrag-documents" / "openarchiver",
            "openrag_task_path": "/v1/tasks/{task_id}/enhanced",
            "openrag_api_key_file": rag_key,
            "state_db": root / "state" / "connector.sqlite3",
            "task_timeout_seconds": 1,
            "max_file_bytes": 1024,
            "max_auto_retries": 3,
            "retry_base_seconds": 1,
            "retry_max_seconds": 4,
            "supported_extensions": frozenset({".pdf", ".txt", ".xlsx"}),
            "openarchiver_requests_per_minute": 10,
            "ingestion_concurrency": 2,
            "ingestion_concurrency_max": 4,
            "page_limit": 2,
        }
        values.update(overrides)
        return connector.Config(**values)

    @staticmethod
    def mail(identifier, source="source-1", **overrides):
        value = {
            "id": identifier,
            "ingestionSourceId": source,
            "threadId": "thread-1",
            "sentAt": "2026-01-01T10:00:00Z",
            "subject": "Sujet de test",
            "senderName": "Émetteur",
            "senderEmail": "sender@example.invalid",
            "recipients": [{"name": "Lecteur", "email": "reader@example.invalid"}],
            "cc": [],
            "messageIdHeader": f"<{identifier}@example.invalid>",
            "storagePath": f"mail/{identifier}.eml",
            "storageHashSha256": f"hash-{identifier}",
            "path": "INBOX",
            "sizeBytes": 100,
            "hasAttachments": False,
        }
        value.update(overrides)
        return value

    def select(self, config, *source_ids):
        for source_id in source_ids:
            connector.set_source_selected(config, source_id, True)

    def rows(self, config, table):
        db = connector.connect_db(config)
        try:
            return list(db.execute(f"SELECT * FROM {table} ORDER BY id"))
        finally:
            db.close()

    def test_configuration_reads_both_key_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = self.config(root)
            env = {
                "OPENARCHIVER_API_KEY_FILE": str(config.openarchiver_api_key_file),
                "OPENRAG_API_KEY_FILE": str(config.openrag_api_key_file),
                "STATE_DB": str(config.state_db),
                "SUPPORTED_EXTENSIONS": "pdf,XLSX",
                "INGESTION_CONCURRENCY": "9",
                "INGESTION_CONCURRENCY_MAX": "3",
            }
            loaded = connector.Config.from_env(env)
            self.assertEqual(
                connector.read_secret(loaded.openarchiver_api_key_file, "OA"),
                "oa-secret",
            )
            self.assertEqual(
                connector.read_secret(loaded.openrag_api_key_file, "RAG"), "rag-secret"
            )
            self.assertEqual(loaded.supported_extensions, frozenset({".pdf", ".xlsx"}))
            self.assertEqual(loaded.ingestion_concurrency, 3)
            self.assertEqual(
                loaded.openarchiver_base_url,
                "http://openarchiver-api.openarchiver.svc.cluster.local:4000/v1",
            )
            self.assertEqual(
                loaded.openrag_ingest_directory,
                Path("/shared/openrag-documents/openarchiver"),
            )
            self.assertEqual(loaded.openrag_ingest_path, "/v1/documents/ingest-path")

            defaults = connector.Config.from_env(
                {
                    "OPENARCHIVER_API_KEY_FILE": str(config.openarchiver_api_key_file),
                    "OPENRAG_API_KEY_FILE": str(config.openrag_api_key_file),
                    "STATE_DB": str(config.state_db),
                }
            )
            self.assertEqual(defaults.ingestion_concurrency, 3)
            self.assertEqual(defaults.ingestion_concurrency_max, 6)
            self.assertEqual(
                defaults.supported_extensions,
                frozenset(
                    {
                        ".asc",
                        ".asciidoc",
                        ".adoc",
                        ".csv",
                        ".docx",
                        ".htm",
                        ".html",
                        ".md",
                        ".pdf",
                        ".txt",
                        ".xlsx",
                    }
                ),
            )
            self.assertEqual(defaults.openrag_auth_mode, "disabled")

    def test_configuration_enables_openrag_auth_only_with_public_https_url(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            env = {
                "OPENARCHIVER_API_KEY_FILE": str(config.openarchiver_api_key_file),
                "OPENRAG_API_KEY_FILE": str(config.openrag_api_key_file),
                "STATE_DB": str(config.state_db),
                "OPENRAG_AUTH_MODE": "auto",
                "CONNECTOR_PUBLIC_URL": "https://connector.example.test",
            }
            loaded = connector.Config.from_env(env)
            self.assertEqual(loaded.openrag_auth_mode, "auto")
            self.assertEqual(
                loaded.connector_public_url, "https://connector.example.test"
            )
            for public_url in ("", "http://connector.example.test"):
                with self.subTest(public_url=public_url), self.assertRaises(ValueError):
                    connector.Config.from_env(
                        {**env, "CONNECTOR_PUBLIC_URL": public_url}
                    )

    def test_runtime_urls_are_persisted_validated_and_restored(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = self.config(
                root,
                connector_public_url="https://bootstrap.example.test",
            )
            self.assertFalse(connector.runtime_urls_are_persisted(config))
            connector.persist_runtime_urls(
                config,
                openrag_base_url="http://openrag-api.openrag.svc.cluster.local:8000/",
                connector_public_url="https://connector.example.test/",
            )

            self.assertEqual(
                config.openrag_base_url,
                "http://openrag-api.openrag.svc.cluster.local:8000",
            )
            self.assertEqual(
                config.connector_public_url, "https://connector.example.test"
            )
            self.assertTrue(connector.runtime_urls_are_persisted(config))

            restored = self.config(
                root,
                openrag_base_url="http://bootstrap-openrag:8000",
                connector_public_url="https://bootstrap.example.test",
            )
            self.assertTrue(connector.restore_runtime_urls(restored))
            self.assertEqual(restored.openrag_base_url, config.openrag_base_url)
            self.assertEqual(
                restored.connector_public_url, config.connector_public_url
            )

            with self.assertRaises(connector.ConnectorError):
                connector.persist_runtime_urls(
                    restored,
                    openrag_base_url="https://public-openrag.example.test",
                    connector_public_url="https://connector.example.test",
                )
            with self.assertRaises(connector.ConnectorError):
                connector.persist_runtime_urls(
                    restored,
                    openrag_base_url="http://openrag-backend:8000",
                    connector_public_url="http://connector.example.test",
                )
            with self.assertRaises(connector.ConnectorError):
                connector.persist_runtime_urls(
                    restored,
                    openrag_base_url="http://openrag-backend:8000",
                    connector_public_url="https://connector.example.test/callback",
                )
            self.assertEqual(restored.openrag_base_url, config.openrag_base_url)
            self.assertEqual(
                restored.connector_public_url, config.connector_public_url
            )

    def test_openrag_auth_client_mirrors_noauth_identity_and_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(
                Path(directory),
                openrag_auth_mode="auto",
                connector_public_url="https://connector.example.test",
            )
            responses = iter(
                (
                    Response(b'{"authenticated":false,"no_auth_mode":true}'),
                    Response(
                        json.dumps(
                            {
                                "authenticated": True,
                                "user": {
                                    "user_id": "google-123",
                                    "email": "reader@example.test",
                                    "name": "Lectrice",
                                    "provider": "google",
                                },
                            }
                        ).encode()
                    ),
                    Response(
                        json.dumps(
                            {
                                "roles": ["user"],
                                "permissions": ["knowledge:upload"],
                                "rbac_enforced": True,
                            }
                        ).encode()
                    ),
                )
            )
            auth = connector.OpenRAGAuthClient(
                config, opener=lambda *_args, **_kwargs: next(responses)
            )
            anonymous = auth.resolve()
            self.assertIsNotNone(anonymous)
            self.assertTrue(anonymous.no_auth_mode)
            principal = auth.resolve("header.payload.signature")
            self.assertEqual(principal.user_id, "google-123")
            self.assertEqual(principal.roles, frozenset({"user"}))
            self.assertTrue(principal.can("knowledge:upload"))
            self.assertFalse(principal.can("config:write"))

            required_config = self.config(
                Path(directory),
                openrag_auth_mode="required",
                connector_public_url="https://connector.example.test",
            )
            required_auth = connector.OpenRAGAuthClient(
                required_config,
                opener=lambda *_args, **_kwargs: Response(
                    b'{"authenticated":false,"no_auth_mode":true}'
                ),
            )
            with self.assertRaisesRegex(
                connector.ConnectorError, "sans authentification"
            ):
                required_auth.resolve()

    def test_openrag_auth_client_delegates_oauth_and_extracts_cookie(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(
                Path(directory),
                openrag_auth_mode="auto",
                connector_public_url="https://connector.example.test",
            )
            requests = []
            callback_headers = EmailMessage()
            callback_headers["Set-Cookie"] = (
                "auth_token=header.payload.signature; HttpOnly; Path=/; SameSite=lax"
            )
            responses = iter(
                (
                    Response(b'{"authenticated":false,"user":null}'),
                    Response(
                        json.dumps(
                            {
                                "connection_id": "oauth-state-1",
                                "oauth_config": {
                                    "client_id": "google-client",
                                    "scopes": ["openid", "email", "profile"],
                                    "redirect_uri": "https://connector.example.test/auth/callback",
                                    "authorization_endpoint": "https://accounts.google.com/o/oauth2/v2/auth",
                                    "prompt": "consent",
                                },
                            }
                        ).encode()
                    ),
                    Response(b'{"purpose":"app_auth"}', headers=callback_headers),
                )
            )

            def opener(request, **_kwargs):
                requests.append(request)
                return next(responses)

            auth = connector.OpenRAGAuthClient(config, opener=opener)
            authorization_url, state = auth.begin_login()
            self.assertEqual(state, "oauth-state-1")
            self.assertTrue(
                authorization_url.startswith(
                    "https://accounts.google.com/o/oauth2/v2/auth?"
                )
            )
            self.assertEqual(
                urllib.parse.parse_qs(urllib.parse.urlsplit(authorization_url).query)[
                    "redirect_uri"
                ],
                ["https://connector.example.test/auth/callback"],
            )
            token = auth.complete_login(state, "authorization-code")
            self.assertEqual(token, "header.payload.signature")
            self.assertEqual(requests[1].full_url, config.openrag_base_url + "/auth/init")
            self.assertEqual(
                json.loads(requests[2].data)["connection_id"], "oauth-state-1"
            )

    def test_manual_concurrency_defaults_to_three_and_is_capped_at_six(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(
                Path(directory),
                ingestion_concurrency=3,
                ingestion_concurrency_max=6,
            )
            state = connector.RuntimeState()
            self.assertEqual(
                connector.effective_ingestion_concurrency(config, state), 3
            )
            snapshot = state.snapshot()
            self.assertEqual(snapshot["ingestion_concurrency_effective"], 3)
            config.ingestion_concurrency = 99
            self.assertEqual(connector.effective_ingestion_concurrency(config), 6)

    def test_manual_pool_size_is_persisted_validated_and_restored(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = self.config(root, ingestion_concurrency=3)
            connector.POOL_RECONFIGURE.clear()
            self.assertFalse(connector.runtime_pool_size_is_persisted(config))
            self.assertEqual(connector.persist_runtime_pool_size(config, "5"), 5)
            self.assertEqual(config.ingestion_concurrency, 5)
            self.assertTrue(connector.POOL_RECONFIGURE.is_set())
            self.assertTrue(connector.runtime_pool_size_is_persisted(config))

            restored = self.config(root, ingestion_concurrency=3)
            self.assertTrue(connector.restore_runtime_pool_size(restored))
            self.assertEqual(restored.ingestion_concurrency, 5)
            for invalid in (0, 7, "auto", ""):
                with self.subTest(invalid=invalid), self.assertRaises(
                    connector.ConnectorError
                ):
                    connector.persist_runtime_pool_size(restored, invalid)

    def test_secret_rotation_is_atomic_and_only_renders_safe_prefixes(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            openarchiver_key = "oa_live_1234567890_secret_suffix"
            openrag_key = "orag_abcdefghijk_secret_suffix"
            connector.write_secret(
                config.openarchiver_api_key_file, openarchiver_key, "OpenArchiver"
            )
            connector.write_secret(
                config.openrag_api_key_file, openrag_key, "OpenRAG"
            )
            self.assertEqual(
                connector.read_secret(config.openarchiver_api_key_file, "OpenArchiver"),
                openarchiver_key,
            )
            self.assertEqual(config.openarchiver_api_key_file.stat().st_mode & 0o777, 0o600)
            page = connector.render_status_page(config, connector.RuntimeState())
            self.assertIn("OpenArchiver : Configurée", page)
            self.assertIn("OpenRAG : Configurée", page)
            self.assertIn("oa_live_1234...", page)
            self.assertIn("orag_abcdefg...", page)
            self.assertNotIn(openarchiver_key, page)
            self.assertNotIn(openrag_key, page)
            self.assertNotIn("secret_suffix", page)

    def test_short_secret_prefix_always_masks_the_last_four_characters(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "api-key"
            path.write_text("short-key", encoding="utf-8")
            self.assertEqual(connector.secret_display_prefix(path), "short")

    def test_rejects_non_internal_or_non_http_urls(self):
        invalid = (
            {"OPENARCHIVER_BASE_URL": "https://openarchiver.svc/api/v1"},
            {"OPENARCHIVER_BASE_URL": "http://archives.example.com/api/v1"},
            {"OPENRAG_BASE_URL": "https://openrag-backend:8000"},
            {"OPENRAG_BASE_URL": "http://user:password@openrag-backend:8000"},
        )
        for values in invalid:
            with self.subTest(values=values), self.assertRaises(ValueError):
                connector.Config.from_env(values)

    def test_database_schema_and_idempotent_migration(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            for _ in range(2):
                db = connector.connect_db(config)
                tables = {
                    row[0]
                    for row in db.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                email_columns = {
                    row["name"] for row in db.execute("PRAGMA table_info(emails)")
                }
                db.close()
            self.assertTrue(
                {
                    "sources",
                    "mailboxes",
                    "emails",
                    "attachments",
                    "email_attachments",
                    "settings",
                    "users",
                    "audit_log",
                }
                <= tables
            )
            self.assertIn("sha256", email_columns)
            self.assertIn("mailbox_path", email_columns)
            version_db = connector.connect_db(config)
            try:
                self.assertEqual(
                    version_db.execute("PRAGMA user_version").fetchone()[0],
                    connector.SCHEMA_VERSION,
                )
                self.assertEqual(
                    version_db.execute("PRAGMA journal_mode").fetchone()[0],
                    "delete",
                )
                self.assertEqual(
                    version_db.execute("PRAGMA temp_store").fetchone()[0],
                    2,
                )
            finally:
                version_db.close()
            self.assertFalse(connector.is_paused(config))

    def test_openrag_identity_snapshot_and_audit_do_not_persist_pii(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            principal = connector.ConnectorPrincipal(
                user_id="google-123",
                email="private@example.test",
                name="Nom privé",
                provider="google",
                roles=frozenset({"user"}),
                permissions=frozenset({"knowledge:upload"}),
                authenticated=True,
                rbac_enforced=True,
            )
            connector.sync_connector_user(config, principal)
            connector.record_audit(config, principal, "inventory.scan")
            with connector.database(config) as db:
                user = db.execute("SELECT * FROM users").fetchone()
                audit = db.execute("SELECT * FROM audit_log").fetchone()
                columns = {
                    row["name"] for row in db.execute("PRAGMA table_info(users)")
                }
            self.assertEqual(user["id"], "google-123")
            self.assertEqual(json.loads(user["roles_json"]), ["user"])
            self.assertNotIn("email", columns)
            self.assertNotIn("name", columns)
            self.assertEqual(audit["actor_user_id"], "google-123")
            self.assertEqual(audit["action"], "inventory.scan")

    def test_v3_to_v4_migration_preserves_the_existing_ingestion_queue(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            self._insert_email(config, self.mail("mail-before-auth"))
            with connector.database(config) as db:
                db.execute(
                    """UPDATE emails
                       SET status='ingesting', attempts=2, task_id='task-existing'
                       WHERE id='mail-before-auth'"""
                )
                db.execute("DROP TABLE audit_log")
                db.execute("DROP TABLE users")
                db.execute("PRAGMA user_version=3")

            migrated = connector.connect_db(config)
            try:
                row = migrated.execute(
                    "SELECT status, attempts, task_id FROM emails WHERE id=?",
                    ("mail-before-auth",),
                ).fetchone()
                tables = {
                    item[0]
                    for item in migrated.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                version = migrated.execute("PRAGMA user_version").fetchone()[0]
            finally:
                migrated.close()

            self.assertEqual(dict(row), {
                "status": "ingesting",
                "attempts": 2,
                "task_id": "task-existing",
            })
            self.assertTrue({"users", "audit_log"} <= tables)
            self.assertEqual(version, connector.SCHEMA_VERSION)

    def test_database_connections_do_not_repeat_migrations_during_a_write(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            writer = connector.connect_db(config)
            writer.execute("BEGIN IMMEDIATE")
            try:
                reader = connector.connect_db(config)
                try:
                    self.assertEqual(
                        reader.execute("SELECT COUNT(*) FROM settings").fetchone()[0],
                        1,
                    )
                finally:
                    reader.close()
            finally:
                writer.rollback()
                writer.close()

    def test_database_migrates_wal_to_delete_journal(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            legacy = connector.connect_db(config)
            legacy.execute("PRAGMA user_version=1")
            self.assertEqual(legacy.execute("PRAGMA journal_mode=WAL").fetchone()[0], "wal")
            legacy.close()

            migrated = connector.connect_db(config)
            try:
                self.assertEqual(
                    migrated.execute("PRAGMA journal_mode").fetchone()[0],
                    "delete",
                )
                self.assertEqual(
                    migrated.execute("PRAGMA user_version").fetchone()[0],
                    connector.SCHEMA_VERSION,
                )
            finally:
                migrated.close()

    def test_sqlite_errors_keep_safe_operational_detail(self):
        error = sqlite3.OperationalError("database is locked")
        self.assertEqual(
            connector._safe_error(error),
            "OperationalError: database is locked",
        )

    def test_legacy_markdown_mail_is_requeued_as_original_eml(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            email = connector._validate_email(self.mail("mail-1"), "source-1")
            db = connector.connect_db(config)
            connector._upsert_email(db, email, 1)
            db.execute(
                """UPDATE emails SET openrag_filename=?, status='validated',
                   sha256='old', task_id='old-task' WHERE id=?""",
                ("openarchiver-mail-mail-1.md", "mail-1"),
            )
            connector._upsert_email(db, email, 2)
            row = db.execute("SELECT * FROM emails WHERE id='mail-1'").fetchone()
            db.commit()
            db.close()
            self.assertEqual(
                row["openrag_filename"],
                connector.mail_openrag_filename("mail-1", "Sujet de test"),
            )
            self.assertEqual(row["status"], "queued")
            self.assertEqual(row["sha256"], "")
            self.assertEqual(row["task_id"], "")

    def test_name_migration_requeues_existing_mail_and_attachment(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            self._insert_email(
                config,
                self.mail("01760ab2-cfcb-445d-b8a4-d1678d781435", subject="Titre été"),
            )
            connector.inventory_attachments(
                config,
                "01760ab2-cfcb-445d-b8a4-d1678d781435",
                {
                    "attachments": [
                        {
                            "id": "attachment-1",
                            "filename": "Facture août.pdf",
                            "sizeBytes": 4,
                            "storagePath": "att/facture",
                        }
                    ]
                },
            )
            with connector.database(config) as db:
                db.execute(
                    """UPDATE emails SET openrag_filename='openarchiver-mail-old.eml',
                       status='validated', sha256='old', attempts=2,
                       task_id='old-task'"""
                )
                db.execute(
                    """UPDATE attachments
                       SET openrag_filename='openarchiver-attachment-old.pdf',
                           status='validated', sha256='old', attempts=2,
                           task_id='old-task'"""
                )
                db.execute("PRAGMA user_version=2")

            migrated = connector.connect_db(config)
            try:
                email = migrated.execute("SELECT * FROM emails").fetchone()
                attachment = migrated.execute("SELECT * FROM attachments").fetchone()
            finally:
                migrated.close()

            self.assertEqual(
                email["openrag_filename"],
                "Titre-ete--01760ab2cfcb.eml",
            )
            self.assertEqual(email["status"], "queued")
            self.assertEqual(email["sha256"], "")
            self.assertEqual(email["attempts"], 0)
            self.assertEqual(email["task_id"], "")
            self.assertTrue(
                attachment["openrag_filename"].startswith(
                    "Titre-ete--01760ab2cfcb--Facture-aout--"
                )
            )
            self.assertTrue(attachment["openrag_filename"].endswith(".pdf"))
            self.assertEqual(attachment["status"], "queued")
            self.assertEqual(attachment["sha256"], "")
            self.assertEqual(attachment["attempts"], 0)
            self.assertEqual(attachment["task_id"], "")

    def test_refresh_and_select_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            client = FakeArchive(
                sources=[
                    {
                        "id": "root",
                        "name": "Racine",
                        "provider": "imap",
                        "mergedIntoId": None,
                    }
                ]
            )
            connector.refresh_sources(config, client)
            connector.set_source_selected(config, "root", True)
            self.assertEqual(connector.selected_source_ids(config), ["root"])

    def test_replace_selection_limits_queue_claims(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            self._insert_email(config, self.mail("one", "source-1"))
            self._insert_email(config, self.mail("two", "source-2"))
            connector.replace_source_selection(config, ["source-1"])
            first = connector.claim_next(config, now=1)
            self.assertEqual(first.object_id, "one")
            self.assertIsNone(connector.claim_next(config, now=1))
            connector.replace_source_selection(config, ["source-2"])
            second = connector.claim_next(config, now=1)
            self.assertEqual(second.object_id, "two")

    def test_replace_selection_rejects_unknown_source(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            connector.set_source_selected(config, "known", True)
            with self.assertRaisesRegex(connector.ConnectorError, "inconnue"):
                connector.replace_source_selection(config, ["missing"])
            self.assertEqual(connector.selected_source_ids(config), ["known"])

    def test_normal_pagination_and_new_mail_is_queued(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            self.select(config, "source-1")
            client = FakeArchive(
                pages={
                    ("source-1", 1): {
                        "items": [self.mail("a"), self.mail("b")],
                        "total": 3,
                    },
                    ("source-1", 2): {"items": [self.mail("c")], "total": 3},
                }
            )
            progress = []
            result = connector.scan_selected_sources(
                config,
                client,
                progress=lambda phase, current, total: progress.append(
                    (phase, current, total)
                ),
            )
            self.assertEqual(result, connector.ScanResult(1, 3, True, False))
            self.assertEqual(
                progress,
                [
                    ("Inventaire de la source source-1", 2, 3),
                    ("Inventaire de la source source-1", 3, 3),
                ],
            )
            self.assertEqual(
                [row["status"] for row in self.rows(config, "emails")], ["queued"] * 3
            )

    def test_duplicate_uuid_across_overlapping_merged_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory), page_limit=10)
            self.select(config, "root", "child")
            shared = self.mail("same", "child")
            client = FakeArchive(
                pages={
                    ("root", 1): {
                        "items": [shared, self.mail("root-only", "root")],
                        "total": 2,
                    },
                    ("child", 1): {"items": [shared], "total": 1},
                }
            )
            result = connector.scan_selected_sources(config, client)
            self.assertTrue(result.complete)
            self.assertEqual(result.emails, 2)
            self.assertEqual(len(self.rows(config, "emails")), 2)

    def test_merged_source_keeps_selected_inventory_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory), page_limit=10)
            self.select(config, "root")
            child_mail = self.mail("child-mail", "child")
            result = connector.scan_selected_sources(
                config,
                FakeArchive(pages={("root", 1): {"items": [child_mail], "total": 1}}),
            )
            self.assertTrue(result.complete)
            row = self.rows(config, "emails")[0]
            self.assertEqual(row["source_id"], "root")
            connector.replace_mailbox_selection(config, [("root", "INBOX")])
            self.assertIsNotNone(connector.claim_next(config, now=1))

    def test_total_can_move_during_pagination(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            self.select(config, "source-1")
            client = FakeArchive(
                pages={
                    ("source-1", 1): {
                        "items": [self.mail("a"), self.mail("b")],
                        "total": 3,
                    },
                    ("source-1", 2): {
                        "items": [self.mail("c"), self.mail("d")],
                        "total": 4,
                    },
                }
            )
            result = connector.scan_selected_sources(config, client)
            self.assertTrue(result.complete)
            self.assertFalse(result.repeated)
            self.assertEqual(result.emails, 4)
            self.assertEqual(
                client.calls,
                [("source-1", 1, 2), ("source-1", 2, 2)],
            )

    def test_total_mismatch_is_accepted_and_does_not_mark_missing(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            self.select(config, "source-1")
            stable = FakeArchive(
                pages={
                    ("source-1", 1): {
                        "items": [self.mail("a"), self.mail("b")],
                        "total": 2,
                    }
                }
            )
            connector.scan_selected_sources(config, stable)
            broken = FakeArchive(
                pages={
                    ("source-1", 1): [
                        {"items": [self.mail("a")], "total": 2},
                        {"items": [self.mail("a")], "total": 2},
                    ]
                }
            )
            result = connector.scan_selected_sources(config, broken)
            self.assertTrue(result.complete)
            self.assertEqual(result.emails, 1)
            self.assertNotIn(
                "missing", [row["status"] for row in self.rows(config, "emails")]
            )

    def test_unchanged_mail_stays_validated_and_changed_mail_is_requeued(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory), page_limit=10)
            self.select(config, "source-1")
            original = self.mail("a")
            connector.scan_selected_sources(
                config,
                FakeArchive(pages={("source-1", 1): {"items": [original], "total": 1}}),
            )
            db = connector.connect_db(config)
            db.execute(
                "UPDATE emails SET status='validated', last_success_at=10, sha256='old-sha'"
            )
            db.commit()
            db.close()
            connector.scan_selected_sources(
                config,
                FakeArchive(
                    pages={("source-1", 1): {"items": [dict(original)], "total": 1}}
                ),
            )
            self.assertEqual(self.rows(config, "emails")[0]["status"], "validated")
            changed = dict(original, storageHashSha256="new-hash")
            connector.scan_selected_sources(
                config,
                FakeArchive(pages={("source-1", 1): {"items": [changed], "total": 1}}),
            )
            row = self.rows(config, "emails")[0]
            self.assertEqual(row["status"], "queued")
            self.assertEqual(row["sha256"], "")

    def test_recipient_shapes_split_to_and_cc(self):
        raw = self.mail(
            "mail",
            recipients=[
                {"name": "To", "email": "to@example.invalid", "type": "to"},
                {"name": "Copie", "email": "cc@example.invalid", "recipientType": "cc"},
            ],
        )
        parsed = connector._validate_email(raw, "source-1")
        self.assertEqual(parsed["recipients"], ["To <to@example.invalid>"])
        self.assertEqual(parsed["cc"], ["Copie <cc@example.invalid>"])

        raw["recipients"] = {
            "to": ["one@example.invalid"],
            "cc": ["copy@example.invalid"],
        }
        parsed = connector._validate_email(raw, "source-1")
        self.assertEqual(parsed["recipients"], ["one@example.invalid"])
        self.assertEqual(parsed["cc"], ["copy@example.invalid"])

    def test_rate_limiter_waits_after_configured_count(self):
        now = [0.0]
        sleeps = []

        def sleep(seconds):
            sleeps.append(seconds)
            now[0] += seconds

        limiter = connector.RateLimiter(2, clock=lambda: now[0], sleeper=sleep)
        limiter.acquire()
        limiter.acquire()
        limiter.acquire()
        self.assertEqual(sleeps, [60.0])

    def test_429_retry_after_is_honoured(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            calls = []
            error = urllib.error.HTTPError(
                "url", 429, "limited", {"Retry-After": "2"}, None
            )
            outcomes = [error, Response(b"[]")]

            def opener(*_args, **_kwargs):
                value = outcomes.pop(0)
                if isinstance(value, Exception):
                    raise value
                return value

            client = connector.OpenArchiverClient(
                config, opener=opener, sleeper=calls.append
            )
            self.assertEqual(client.list_sources(), [])
            self.assertEqual(calls, [2.0])

    def test_http_401_403_404_are_explicit_and_5xx_retries(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            for status in (401, 403, 404):

                def opener(*_args, _status=status, **_kwargs):
                    raise urllib.error.HTTPError("url", _status, "error", {}, None)

                client = connector.OpenArchiverClient(
                    config, opener=opener, sleeper=lambda _s: None
                )
                with (
                    self.subTest(status=status),
                    self.assertRaises(connector.HTTPStatusError) as caught,
                ):
                    client.list_sources()
                self.assertEqual(caught.exception.status, status)

            attempts = []

            def temporary(*_args, **_kwargs):
                attempts.append(1)
                raise urllib.error.HTTPError("url", 503, "error", {}, None)

            with self.assertRaises(connector.HTTPStatusError):
                connector.OpenArchiverClient(
                    config, opener=temporary, sleeper=lambda _s: None
                ).list_sources()
            self.assertEqual(len(attempts), config.max_auto_retries)

    def test_timeout_and_invalid_json_are_sanitised(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            timeout_client = connector.OpenArchiverClient(
                config,
                opener=mock.Mock(side_effect=TimeoutError("body-secret")),
                sleeper=lambda _s: None,
            )
            with self.assertRaisesRegex(connector.ConnectorError, "indisponible"):
                timeout_client.list_sources()
            invalid = connector.OpenArchiverClient(
                config, opener=lambda *_a, **_k: Response(b"not-json")
            )
            with self.assertRaisesRegex(connector.ConnectorError, "JSON"):
                invalid.list_sources()

    def test_incomplete_json_response_is_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            calls = []

            class InterruptedResponse(Response):
                def read(self, size=-1):
                    raise connector.http.client.IncompleteRead(b"[")

            outcomes = [InterruptedResponse(), Response(b"[]")]

            def opener(*_args, **_kwargs):
                calls.append(1)
                return outcomes.pop(0)

            client = connector.OpenArchiverClient(
                config, opener=opener, sleeper=lambda _seconds: None
            )
            self.assertEqual(client.list_sources(), [])
            self.assertEqual(len(calls), 2)

    def write_message(self, root, message):
        path = root / "mail.eml"
        path.write_bytes(message.as_bytes())
        return path

    def test_eml_text_plain_and_unicode_name(self):
        with tempfile.TemporaryDirectory() as directory:
            message = EmailMessage()
            message["Subject"] = "Réunion été"
            message["From"] = "Élodie Test <sender@example.invalid>"
            message["To"] = "Destinataire <reader@example.invalid>"
            message.set_content("Bonjour\n-- signature")
            body, headers = connector.parse_eml(
                self.write_message(Path(directory), message)
            )
            self.assertIn("Bonjour", body)
            self.assertIn("Élodie", headers["from"][0])

    def test_eml_multipart_alternative_prefers_plain(self):
        with tempfile.TemporaryDirectory() as directory:
            message = EmailMessage()
            message.set_content("Version texte")
            message.add_alternative("<p>Version HTML</p>", subtype="html")
            body, _ = connector.parse_eml(self.write_message(Path(directory), message))
            self.assertEqual(body, "Version texte")

    def test_eml_multipart_mixed_does_not_extract_attachment_body(self):
        with tempfile.TemporaryDirectory() as directory:
            message = EmailMessage()
            message.set_content("Corps du mail")
            message.add_attachment(
                b"CONTENU-DE-PIECE-JOINTE",
                maintype="application",
                subtype="octet-stream",
                filename="document.bin",
            )
            body, _ = connector.parse_eml(self.write_message(Path(directory), message))
            self.assertEqual(body, "Corps du mail")

    def test_eml_html_fallback_removes_script_style_and_tags(self):
        with tempfile.TemporaryDirectory() as directory:
            message = EmailMessage()
            message.add_header("Content-Type", "text/html; charset=utf-8")
            message.set_payload(
                "<style>secret-css</style><p>Bonjour <b>monde</b></p><script>secret-js</script>"
            )
            body, _ = connector.parse_eml(self.write_message(Path(directory), message))
            self.assertEqual(body, "Bonjour monde")

    def test_invalid_declared_encoding_falls_back_without_crash(self):
        with tempfile.TemporaryDirectory() as directory:
            raw = b"Content-Type: text/plain; charset=charset-inconnu\r\nContent-Transfer-Encoding: base64\r\n\r\nQm9uam91cg=="
            path = Path(directory) / "invalid.eml"
            path.write_bytes(raw)
            body, _ = connector.parse_eml(path)
            self.assertEqual(body, "Bonjour")

    def test_markdown_is_stable_and_does_not_copy_thread_bodies(self):
        row = {
            "id": "mail-1",
            "subject": "Sujet",
            "source_id": "source-1",
            "thread_id": "thread-1",
            "sent_at": "2026-01-01",
            "sender_name": "Alice",
            "sender_email": "alice@example.invalid",
            "recipients_json": '["Bob <bob@example.invalid>"]',
            "cc_json": "[]",
            "message_id": "<mail-1@example.invalid>",
        }
        attachments = [
            {"id": "pj-2", "filename": "été.pdf"},
            {"id": "pj-1", "filename": "a.txt"},
        ]
        first = connector.render_mail_markdown(
            row,
            "Corps courant",
            attachments,
            source_name="Archive",
            link_template="https://oa.invalid/dashboard/archived-emails/{email_id}",
        )
        second = connector.render_mail_markdown(
            row,
            "Corps courant",
            list(reversed(attachments)),
            source_name="Archive",
            link_template="https://oa.invalid/dashboard/archived-emails/{email_id}",
        )
        self.assertEqual(first, second)
        self.assertIn("Corps courant", first)
        self.assertNotIn("corps d'un autre mail", first)

    def _insert_email(self, config, email=None, status="queued"):
        raw = email or self.mail("mail-1")
        source_id = str(raw.get("ingestionSourceId") or "source-1")
        data = connector._validate_email(raw, source_id)
        connector.set_source_selected(config, data["source_id"], True)
        db = connector.connect_db(config)
        connector._upsert_email(db, data, 1)
        db.execute(
            """
            INSERT OR REPLACE INTO mailboxes(
                source_id, path, selected, message_count,
                first_seen_at, last_seen_at
            ) VALUES (?, ?, 1, 1, 1, 1)
            """,
            (data["source_id"], data["mailbox_path"]),
        )
        db.execute("UPDATE emails SET status=? WHERE id=?", (status, data["id"]))
        db.commit()
        db.close()
        return data["id"]

    def test_attachment_compatible_shared_and_stable_names(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            self._insert_email(config, self.mail("mail-1"))
            self._insert_email(config, self.mail("mail-2"))
            detail = {
                "attachments": [
                    {
                        "id": "att-1",
                        "filename": "Classeur été.XLSX",
                        "mimeType": "application/test",
                        "sizeBytes": 10,
                        "storagePath": "att/1",
                    }
                ]
            }
            connector.inventory_attachments(config, "mail-1", detail)
            connector.inventory_attachments(config, "mail-2", detail)
            rows = self.rows(config, "attachments")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["status"], "queued")
            self.assertEqual(
                rows[0]["openrag_filename"],
                connector.attachment_openrag_filename(
                    "att-1", "Classeur été.XLSX", "Sujet de test", "mail-1"
                ),
            )
            db = connector.connect_db(config)
            links = db.execute("SELECT COUNT(*) FROM email_attachments").fetchone()[0]
            db.close()
            self.assertEqual(links, 2)

    def test_attachment_too_large_or_unsupported_is_non_indexable(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory), max_file_bytes=10)
            self._insert_email(config)
            detail = {
                "attachments": [
                    {
                        "id": "large",
                        "filename": "large.pdf",
                        "sizeBytes": 11,
                        "storagePath": "att/large",
                    },
                    {
                        "id": "unknown",
                        "filename": "archive.exe",
                        "sizeBytes": 1,
                        "storagePath": "att/unknown",
                    },
                ]
            }
            connector.inventory_attachments(config, "mail-1", detail)
            rows = self.rows(config, "attachments")
            self.assertEqual(
                [row["status"] for row in rows], ["non_indexable", "non_indexable"]
            )
            safe_name = connector.attachment_openrag_filename(
                "x", "../../evil.EXE", "Objet du mail"
            )
            self.assertTrue(safe_name.startswith("Objet-du-mail--evil--"))
            self.assertTrue(safe_name.endswith(".exe"))
            self.assertNotIn("/", safe_name)

    def test_changed_attachment_resets_transient_ingestion_state(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            self._insert_email(config)
            detail = {
                "attachments": [
                    {
                        "id": "att",
                        "filename": "doc.pdf",
                        "sizeBytes": 4,
                        "storagePath": "att/old",
                    }
                ]
            }
            connector.inventory_attachments(config, "mail-1", detail)
            db = connector.connect_db(config)
            db.execute(
                """UPDATE attachments SET status='validated', sha256='old-sha',
                   task_id='old-task', next_retry_at=123 WHERE id='att'"""
            )
            db.commit()
            db.close()
            detail["attachments"][0]["storagePath"] = "att/new"
            connector.inventory_attachments(config, "mail-1", detail)
            row = self.rows(config, "attachments")[0]
            self.assertEqual(row["status"], "queued")
            self.assertEqual(row["sha256"], "")
            self.assertEqual(row["task_id"], "")
            self.assertEqual(row["next_retry_at"], 0)

    def test_remote_names_are_stable_and_escape_unsafe_identifiers(self):
        first = connector.mail_openrag_filename(
            'id"\r\nheader', "Suppression de vos annonces sur leboncoin.fr"
        )
        second = connector.mail_openrag_filename(
            'id"\r\nother', "Suppression de vos annonces sur leboncoin.fr"
        )
        self.assertEqual(
            first,
            connector.mail_openrag_filename(
                'id"\r\nheader', "Suppression de vos annonces sur leboncoin.fr"
            ),
        )
        self.assertNotEqual(first, second)
        self.assertTrue(
            first.startswith("Suppression-de-vos-annonces-sur-leboncoin.fr--")
        )
        self.assertNotIn("\r", first)
        self.assertNotIn('"', first)

        attachment = connector.attachment_openrag_filename(
            "attachment-id",
            "Facture août 2026.pdf",
            "Suppression de vos annonces sur leboncoin.fr",
            "01760ab2-cfcb-445d-b8a4-d1678d781435",
        )
        self.assertTrue(
            attachment.startswith(
                "Suppression-de-vos-annonces-sur-leboncoin.fr--01760ab2cfcb--Facture-aout-2026--"
            )
        )
        self.assertTrue(attachment.endswith(".pdf"))
        self.assertLessEqual(len(attachment), 255)

    def test_download_streams_and_computes_sha256(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = self.config(root)
            response = Response(chunks=[b"abc", b"def"])
            client = connector.OpenArchiverClient(
                config, opener=lambda *_a, **_k: response
            )
            destination = root / "download"
            size, digest = client.download("path", destination)
            self.assertEqual(size, 6)
            self.assertEqual(
                digest,
                "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721",
            )
            self.assertEqual(destination.read_bytes(), b"abcdef")

    def test_download_enforces_maximum_size(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = self.config(root, max_file_bytes=3)
            client = connector.OpenArchiverClient(
                config, opener=lambda *_a, **_k: Response(chunks=[b"abcd"])
            )
            with self.assertRaisesRegex(connector.ConnectorError, "volumineux"):
                client.download("path", root / "download")

    def test_deposit_source_publishes_atomically_and_cleans_partial_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = self.config(root)
            archive = FakeArchive(downloads={"mail/source.eml": b"raw source"})
            destination, size, digest = connector.deposit_source(
                config, archive, "mail/source.eml", "source.eml"
            )
            self.assertEqual(destination.read_bytes(), b"raw source")
            self.assertEqual(size, 10)
            self.assertEqual(
                digest,
                "16911a2110ab626e1b62476366de38bba71cadcea22d02df42d701aba6cdd409",
            )
            self.assertEqual(list(config.openrag_ingest_directory.glob("*.part")), [])

            class FailingArchive:
                def download(self, _storage_path, temporary):
                    temporary.write_bytes(b"partial")
                    raise connector.ConnectorError("download failed")

            with self.assertRaisesRegex(connector.ConnectorError, "download failed"):
                connector.deposit_source(
                    config, FailingArchive(), "mail/failure.eml", "failure.eml"
                )
            self.assertFalse((config.openrag_ingest_directory / "failure.eml").exists())
            self.assertEqual(list(config.openrag_ingest_directory.glob("*.part")), [])

    def test_openrag_task_success_and_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            client = connector.OpenRAGClient(config, sleeper=lambda _s: None)
            with mock.patch.object(
                client, "task", return_value={"status": "completed", "failed_files": 0}
            ):
                client.wait("ok")
            with mock.patch.object(
                client,
                "task",
                return_value={"status": "completed", "failed_files": ["file"]},
            ):
                with self.assertRaisesRegex(
                    connector.ConnectorError, "fichiers en échec"
                ):
                    client.wait("bad")
            with mock.patch.object(client, "task", return_value={"status": "failed"}):
                with self.assertRaisesRegex(connector.ConnectorError, "failed"):
                    client.wait("bad")

    def test_openrag_running_task_is_polled_every_250ms(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            sleeps = []
            client = connector.OpenRAGClient(config, sleeper=sleeps.append)
            with mock.patch.object(
                client,
                "task",
                side_effect=[
                    {"status": "running"},
                    {"status": "completed", "failed_files": 0},
                ],
            ):
                client.wait("running")
            self.assertEqual(sleeps, [0.25])

    def test_openrag_task_404_falls_back_to_standard_endpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            client = connector.OpenRAGClient(config, sleeper=lambda _s: None)
            responses = [
                connector.HTTPStatusError(404, "lecture tâche OpenRAG"),
                {"status": "completed", "failed_files": 0},
            ]
            with mock.patch.object(client, "task", side_effect=responses) as task:
                client.wait("task/id")
            self.assertEqual(task.call_count, 2)
            self.assertEqual(
                task.call_args_list[1].kwargs["path"], "/v1/tasks/task%2Fid"
            )

    def test_openrag_task_missing_from_both_endpoints_is_lost(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            client = connector.OpenRAGClient(config, sleeper=lambda _s: None)
            missing = connector.HTTPStatusError(404, "lecture tâche OpenRAG")
            with mock.patch.object(client, "task", side_effect=[missing, missing]):
                with self.assertRaisesRegex(
                    connector.LostTaskError, "tâche OpenRAG inconnue"
                ):
                    client.wait("lost-task")

    def test_openrag_document_id_matches_content_hash(self):
        self.assertEqual(
            connector.openrag_document_id(
                "3a6eb0790f39ac87c94f3856b2dd2c5d110e6811602261a9a923d3bb23adc8b7"
            ),
            "Om6weQ85rIfJTzhWst0sXREO",
        )
        with self.assertRaisesRegex(connector.ConnectorError, "SHA-256"):
            connector.openrag_document_id("not-a-hash")

    def test_openrag_index_proof_uses_exact_filename_chunks_and_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            client = connector.OpenRAGClient(config, sleeper=lambda _s: None)
            sha256 = hashlib.sha256(b"data").hexdigest()
            payload = json.dumps(
                {
                    "files": [
                        {
                            "filename": "openarchiver-mail-mail-1.eml",
                            "document_id": connector.openrag_document_id(sha256),
                            "source_url": "https://archive.example.test/mail-1",
                            "chunk_count": 2,
                        }
                    ]
                }
            ).encode()
            opener = mock.Mock(return_value=Response(payload))
            with mock.patch.object(connector.urllib.request, "urlopen", opener):
                self.assertTrue(
                    client.document_is_indexed(
                        "openarchiver-mail-mail-1.eml", sha256
                    )
                )
            request = opener.call_args.args[0]
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
            self.assertEqual(
                query["data_sources"], ["openarchiver-mail-mail-1.eml"]
            )

            with mock.patch.object(
                client,
                "indexed_document",
                return_value={
                    "filename": "openarchiver-mail-mail-1.eml",
                    "document_id": "stale-document",
                    "chunk_count": 2,
                },
            ):
                self.assertFalse(
                    client.document_is_indexed(
                        "openarchiver-mail-mail-1.eml", sha256
                    )
                )

    def test_mail_rate_is_rendered_without_remote_pending_state(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            state = connector.RuntimeState()
            state.mail_rate_updated(73)
            page = connector.render_status_page(config, state)
            self.assertIn('id="mail-rate" class="stat-value">73</strong>', page)
            self.assertNotIn("pending du connecteur", page)
            status = json.loads(connector.render_live_status(state))
            self.assertEqual(status["mails_per_minute"], 73)
            self.assertFalse(any("queue" in key for key in status))

    def test_mail_rate_counts_recent_validated_selected_emails_only(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory), page_limit=10)
            self.select(config, "source-1")
            connector.scan_selected_sources(
                config,
                FakeArchive(
                    pages={
                        ("source-1", 1): {
                            "items": [
                                self.mail("recent"),
                                self.mail("old"),
                                self.mail("failed"),
                            ],
                            "total": 3,
                        }
                    }
                ),
            )
            connector.replace_mailbox_selection(config, [("source-1", "INBOX")])
            with connector.database(config) as db:
                db.execute(
                    "UPDATE emails SET status='validated', last_success_at=980 "
                    "WHERE id='recent'"
                )
                db.execute(
                    "UPDATE emails SET status='validated', last_success_at=939 "
                    "WHERE id='old'"
                )
                db.execute(
                    "UPDATE emails SET status='failed', last_success_at=990 "
                    "WHERE id='failed'"
                )

            self.assertEqual(
                connector.selected_mails_validated_last_minute(config, now=1000), 1
            )

    def test_reconciliation_restores_found_and_marks_missing_validated_lost(self):
        class ReconciliationOpenRAG:
            def __init__(self, documents):
                self.documents = documents
                self.calls = []

            def indexed_documents(self, filenames):
                self.calls.append(list(filenames))
                return {
                    name: self.documents[name]
                    for name in filenames
                    if name in self.documents
                }

        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            missing_id = self._insert_email(
                config, self.mail("missing-mail"), "validated"
            )
            restored_id = self._insert_email(
                config, self.mail("restored-mail"), "lost"
            )
            failed_id = self._insert_email(
                config, self.mail("failed-mail"), "failed"
            )
            hashes = {
                missing_id: hashlib.sha256(b"missing").hexdigest(),
                restored_id: hashlib.sha256(b"restored").hexdigest(),
                failed_id: hashlib.sha256(b"failed").hexdigest(),
            }
            with connector.database(config) as db:
                for object_id, sha256 in hashes.items():
                    db.execute(
                        "UPDATE emails SET sha256=?, attempts=3 WHERE id=?",
                        (sha256, object_id),
                    )
                restored_name = db.execute(
                    "SELECT openrag_filename FROM emails WHERE id=?", (restored_id,)
                ).fetchone()[0]
                failed_name = db.execute(
                    "SELECT openrag_filename FROM emails WHERE id=?", (failed_id,)
                ).fetchone()[0]
            rag = ReconciliationOpenRAG(
                {
                    restored_name: {
                        "filename": restored_name,
                        "document_id": connector.openrag_document_id(
                            hashes[restored_id]
                        ),
                        "chunk_count": 4,
                    },
                    failed_name: {
                        "filename": failed_name,
                        "document_id": "obsolete",
                        "chunk_count": 1,
                    },
                }
            )
            progress = []
            result = connector.reconcile_openrag(
                config, rag, progress=lambda current, total: progress.append((current, total))
            )
            self.assertEqual(result, connector.ReconciliationResult(3, 1, 1))
            self.assertEqual(progress, [(0, 3), (3, 3)])
            rows = {row["id"]: row for row in self.rows(config, "emails")}
            self.assertEqual(rows[missing_id]["status"], "lost")
            self.assertEqual(rows[missing_id]["attempts"], 0)
            self.assertGreater(rows[missing_id]["next_retry_at"], 0)
            self.assertEqual(rows[restored_id]["status"], "validated")
            self.assertEqual(rows[restored_id]["last_error"], "")
            self.assertEqual(rows[failed_id]["status"], "failed")

    def test_reconciliation_button_exposes_live_progress(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            state = connector.RuntimeState()
            wake = threading.Event()
            server = connector.ThreadingHTTPServer(
                ("127.0.0.1", 0),
                connector.make_http_handler(
                    config, state, reconciliation_wake=wake
                ),
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base = f"http://127.0.0.1:{server.server_port}"
                with urllib.request.urlopen(base + "/") as response:
                    page = response.read().decode()
                self.assertIn('action="/reconcile"', page)
                self.assertIn('id="reconciliation-progress"', page)
                request = urllib.request.Request(
                    base + "/reconcile",
                    data=urllib.parse.urlencode(
                        {"csrf": state.snapshot()["csrf_token"]}
                    ).encode(),
                )
                with urllib.request.urlopen(request) as response:
                    self.assertEqual(response.status, 200)
                self.assertTrue(wake.is_set())
                self.assertTrue(state.snapshot()["reconciliation_requested_at"])
                state.reconciliation_started()
                state.reconciliation_progress(25, 100)
                with urllib.request.urlopen(base + "/status.json") as response:
                    status = json.loads(response.read())
                self.assertTrue(status["reconciliation_in_progress"])
                self.assertEqual(status["reconciliation_current"], 25)
                self.assertEqual(status["reconciliation_total"], 100)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

    def test_openrag_ingest_path_uses_json_replace_duplicates(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = self.config(root)
            document = config.openrag_ingest_directory / "document.eml"
            document.parent.mkdir(parents=True)
            document.write_text("contenu", encoding="utf-8")

            opener = mock.Mock(return_value=Response(b'{"task_id":"task-upload"}'))
            with mock.patch.object(connector.urllib.request, "urlopen", opener):
                task_id = connector.OpenRAGClient(config).ingest_path(document)
            self.assertEqual(task_id, "task-upload")
            request = opener.call_args.args[0]
            self.assertEqual(request.method, "POST")
            self.assertEqual(
                json.loads(request.data),
                {"path": str(document.resolve()), "replace_duplicates": True},
            )
            self.assertEqual(request.headers["Content-type"], "application/json")

    def test_openrag_ingest_path_rejects_a_path_outside_shared_inbox(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = self.config(root)
            outside = root / "outside.eml"
            outside.write_bytes(b"source")
            with self.assertRaisesRegex(connector.ConnectorError, "hors du dossier"):
                connector.OpenRAGClient(config).ingest_path(outside)

    def test_multipart_upload_disables_local_archive_and_forwards_source_url(self):
        class Connection:
            def __init__(self):
                self.target = ""
                self.headers = {}
                self.body = bytearray()

            def putrequest(self, method, target):
                self.method = method
                self.target = target

            def putheader(self, name, value):
                self.headers[name.lower()] = value

            def endheaders(self):
                pass

            def send(self, content):
                self.body.extend(content)

            def getresponse(self):
                return Response(b'{"task_id":"task-api"}')

            def close(self):
                pass

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = self.config(root)
            document = root / "document.eml"
            document.write_bytes(b"raw-source")
            connection = Connection()
            with mock.patch.object(
                connector.http.client,
                "HTTPConnection",
                return_value=connection,
            ):
                task_id = connector.OpenRAGClient(config).upload(
                    document,
                    "document.eml",
                    "https://archive.example.test/source.eml",
                )

            body = bytes(connection.body)
            self.assertEqual(task_id, "task-api")
            self.assertEqual(connection.method, "POST")
            self.assertEqual(connection.target, "/v1/documents/ingest")
            self.assertEqual(int(connection.headers["content-length"]), len(body))
            self.assertIn(
                b'name="archive_source"\r\n\r\nfalse\r\n',
                body,
            )
            self.assertIn(
                b'name="source_url"\r\n\r\nhttps://archive.example.test/source.eml\r\n',
                body,
            )
            self.assertIn(b"raw-source", body)

    def test_remote_source_url_template_is_encoded_and_optional(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = self.config(root)
            self.assertIsNone(
                connector.render_remote_source_url(
                    config,
                    kind="email",
                    object_id="mail-1",
                    storage_path="mail/source.eml",
                    email_id="mail-1",
                )
            )
            configured = self.config(
                root,
                openarchiver_source_url_template=(
                    "https://archive.example.test/api/v1/storage/download"
                    "?path={storage_path}"
                ),
            )
            self.assertEqual(
                connector.render_remote_source_url(
                    configured,
                    kind="email",
                    object_id="mail-1",
                    storage_path="mail/a source.eml",
                    email_id="mail-1",
                ),
                "https://archive.example.test/api/v1/storage/download"
                "?path=mail%2Fa%20source.eml",
            )
            invalid = self.config(
                root,
                openarchiver_source_url_template=(
                    "https://archive.example.test/source\x7f/{object_id}"
                ),
            )
            with self.assertRaisesRegex(connector.ConnectorError, "invalide"):
                connector.render_remote_source_url(
                    invalid,
                    kind="email",
                    object_id="mail-1",
                    storage_path="mail/source.eml",
                    email_id="mail-1",
                )

    def test_mail_and_attachments_are_distinct_documents_with_the_same_source_url(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(
                Path(directory),
                openarchiver_source_url_template=(
                    "https://openarchiver.example.test/dashboard/archived-emails/"
                    "{email_id}"
                ),
            )
            email_url = connector.render_remote_source_url(
                config,
                kind="email",
                object_id="mail-1",
                storage_path="emails/mail-1.eml",
                email_id="mail-1",
            )
            attachment_urls = [
                connector.render_remote_source_url(
                    config,
                    kind="attachment",
                    object_id=attachment_id,
                    storage_path=f"attachments/{attachment_id}.pdf",
                    email_id="mail-1",
                )
                for attachment_id in ("attachment-1", "attachment-2")
            ]

            self.assertEqual(attachment_urls, [email_url, email_url])
            self.assertEqual(
                len(
                    {
                        connector.mail_openrag_filename("mail-1"),
                        connector.attachment_openrag_filename(
                            "attachment-1", "facture.pdf"
                        ),
                        connector.attachment_openrag_filename(
                            "attachment-2", "annexe.pdf"
                        ),
                    }
                ),
                3,
            )

    def test_auto_mode_falls_back_to_multipart_and_reuses_it(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = self.config(root, openrag_ingest_mode="auto")
            document = config.openrag_ingest_directory / "document.eml"
            document.parent.mkdir(parents=True)
            document.write_bytes(b"source")
            client = connector.OpenRAGClient(config)
            with (
                mock.patch.object(
                    client,
                    "ingest_path",
                    side_effect=connector.HTTPStatusError(403, "ingestion OpenRAG"),
                ) as ingest_path,
                mock.patch.object(client, "upload", return_value="task-api") as upload,
            ):
                first = client.ingest_source(
                    document, "https://archive.example.test/source.eml"
                )
                second = client.ingest_source(document)

            self.assertEqual(first, ("task-api", True))
            self.assertEqual(second, ("task-api", True))
            ingest_path.assert_called_once_with(document)
            self.assertEqual(upload.call_count, 2)
            upload.assert_any_call(
                document,
                "document.eml",
                "https://archive.example.test/source.eml",
            )

    def test_runtime_cycle_refreshes_sources_without_implicit_selection(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            archive = FakeArchive(
                sources=[{"id": "source-1", "name": "Archive", "provider": "imap"}]
            )
            scan, processed = connector.run_cycle(config, archive, FakeOpenRAG())
            self.assertEqual(scan, connector.ScanResult(0, 0, True, False))
            self.assertEqual(processed, 0)
            self.assertEqual(connector.selected_source_ids(config), [])
            self.assertEqual(
                [row["id"] for row in connector.source_rows(config)], ["source-1"]
            )

    def test_inventory_is_reused_until_a_manual_refresh(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            now = int(time.time())
            with connector.database(config) as db:
                db.executemany(
                    "INSERT OR REPLACE INTO settings(key,value) VALUES (?,?)",
                    (
                        ("last_inventory_completed_at", str(now)),
                        ("last_inventory_sources", "1"),
                        ("last_inventory_emails", "1"),
                    ),
                )
            archive = FakeArchive()
            with mock.patch.object(
                archive,
                "list_sources",
                side_effect=AssertionError("inventaire distant inattendu"),
            ):
                scan, processed = connector.run_cycle(
                    config,
                    archive,
                    FakeOpenRAG(),
                    force_inventory=False,
                )

            self.assertEqual(scan, connector.ScanResult(1, 1, True, False))
            self.assertEqual(processed, 0)
            self.assertIsNotNone(
                connector.cached_inventory(config, now=now + 365 * 24 * 3600)
            )

    def test_old_inventory_processes_queue_without_automatic_refresh(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            self._insert_email(config, self.mail("queued-mail"))
            now = int(time.time())
            with connector.database(config) as db:
                db.executemany(
                    "INSERT OR REPLACE INTO settings(key,value) VALUES (?,?)",
                    (
                        ("last_inventory_completed_at", str(now - 3600)),
                        ("last_inventory_sources", "1"),
                        ("last_inventory_emails", "1"),
                    ),
                )
            archive = FakeArchive(
                sources=[{"id": "source-1", "name": "Archive", "provider": "imap"}],
                pages={
                    ("source-1", 1): {
                        "items": [self.mail("queued-mail")],
                        "total": 1,
                    }
                },
            )

            def finish_queue(*_args, **_kwargs):
                with connector.database(config) as db:
                    db.execute(
                        "UPDATE emails SET status='validated' WHERE id='queued-mail'"
                    )
                return 1

            with mock.patch.object(
                connector, "process_queue", side_effect=finish_queue
            ) as process_queue:
                scan, processed = connector.run_cycle(
                    config,
                    archive,
                    FakeOpenRAG(),
                    force_inventory=False,
                )

            self.assertEqual(scan, connector.ScanResult(1, 1, True, False))
            self.assertEqual(processed, 1)
            process_queue.assert_called_once()
            self.assertEqual(archive.calls, [])

            scan, processed = connector.run_cycle(
                config,
                archive,
                FakeOpenRAG(),
                force_inventory=False,
            )
            self.assertEqual(scan, connector.ScanResult(1, 1, True, False))
            self.assertEqual(processed, 0)
            self.assertEqual(archive.calls, [])

            scan, processed = connector.run_cycle(
                config,
                archive,
                FakeOpenRAG(),
                force_inventory=True,
            )
            self.assertEqual(scan, connector.ScanResult(1, 1, True, False))
            self.assertEqual(processed, 0)
            self.assertEqual(archive.calls, [("source-1", 1, 2)])

    def test_paused_nonempty_queue_blocks_automatic_but_not_manual_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            self._insert_email(config, self.mail("queued-mail"))
            connector.set_paused(config, True)
            now = int(time.time())
            with connector.database(config) as db:
                db.executemany(
                    "INSERT OR REPLACE INTO settings(key,value) VALUES (?,?)",
                    (
                        ("last_inventory_completed_at", str(now - 3600)),
                        ("last_inventory_sources", "1"),
                        ("last_inventory_emails", "1"),
                    ),
                )
            archive = FakeArchive(
                sources=[{"id": "source-1", "name": "Archive", "provider": "imap"}],
                pages={
                    ("source-1", 1): {
                        "items": [self.mail("queued-mail")],
                        "total": 1,
                    }
                },
            )

            scan, processed = connector.run_cycle(
                config,
                archive,
                FakeOpenRAG(),
                force_inventory=False,
            )
            self.assertEqual(scan, connector.ScanResult(1, 1, True, False))
            self.assertEqual(processed, 0)
            self.assertEqual(archive.calls, [])

            scan, processed = connector.run_cycle(
                config,
                archive,
                FakeOpenRAG(),
                force_inventory=True,
            )
            self.assertEqual(scan, connector.ScanResult(1, 1, True, False))
            self.assertEqual(processed, 0)
            self.assertEqual(archive.calls, [("source-1", 1, 2)])

    def test_existing_inventory_gets_a_legacy_validity_timestamp(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            now = int(time.time())
            self._insert_email(config, self.mail("legacy"))
            with connector.database(config) as db:
                db.execute(
                    "UPDATE emails SET last_seen_at=? WHERE id='legacy'",
                    (now * 1_000_000_000,),
                )

            cached = connector.cached_inventory(config, now=now)

            self.assertIsNotNone(cached)
            self.assertEqual(cached[0], connector.ScanResult(1, 1, True, False))
            self.assertEqual(cached[1], now)

    def test_manual_inventory_accepts_a_total_change(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory), page_limit=2)
            connector.set_source_selected(config, "source-1", True)
            now = int(time.time())
            with connector.database(config) as db:
                db.executemany(
                    "INSERT OR REPLACE INTO settings(key,value) VALUES (?,?)",
                    (
                        ("last_inventory_completed_at", str(now)),
                        ("last_inventory_sources", "1"),
                        ("last_inventory_emails", "1"),
                    ),
                )
            first_page = {
                "items": [self.mail("one"), self.mail("two")],
                "total": 3,
                "limit": 2,
            }
            moving_page = {"items": [], "total": 4, "limit": 2}
            archive = FakeArchive(
                sources=[{"id": "source-1", "name": "Archive", "provider": "imap"}],
                pages={
                    ("source-1", 1): first_page,
                    ("source-1", 2): moving_page,
                },
            )

            scan, processed = connector.run_cycle(
                config,
                archive,
                FakeOpenRAG(),
                force_inventory=True,
            )

            self.assertEqual(scan, connector.ScanResult(1, 2, True, False))
            self.assertEqual(processed, 0)
            self.assertEqual(connector.cached_inventory(config, now=now)[0], scan)
            self.assertEqual(
                archive.calls,
                [("source-1", 1, 2), ("source-1", 2, 2)],
            )

    def test_http_probes_and_source_selection(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            connector.set_source_selected(config, "source-1", True)
            state = connector.RuntimeState()
            state.set_running(True)
            state.cycle_succeeded(connector.ScanResult(1, 0, True, False), 0)
            wake = threading.Event()
            server = connector.ThreadingHTTPServer(
                ("127.0.0.1", 0),
                connector.make_http_handler(config, state, wake=wake),
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            base = f"http://127.0.0.1:{server.server_port}"
            try:
                with urllib.request.urlopen(base + "/healthz") as response:
                    self.assertEqual(response.status, 200)
                with urllib.request.urlopen(base + "/readyz") as response:
                    self.assertEqual(response.status, 200)
                state.cycle_failed(connector.ConnectorError("échec temporaire"))
                with urllib.request.urlopen(base + "/readyz") as response:
                    self.assertEqual(response.status, 200)
                self.assertFalse(state.snapshot()["ready"])
                with urllib.request.urlopen(base + "/") as response:
                    page = response.read().decode("utf-8")
                self.assertIn("source-1", page)
                self.assertIn("Clés API", page)
                self.assertIn("Amorçage Rancher/Fleet", page)

                secrets_request = urllib.request.Request(
                    base + "/secrets",
                    data=urllib.parse.urlencode(
                        {
                            "csrf": state.snapshot()["csrf_token"],
                            "openarchiver_key": "oa-rotated",
                            "openrag_key": "rag-rotated",
                        }
                    ).encode("ascii"),
                )
                with urllib.request.urlopen(secrets_request) as response:
                    rotated_page = response.read().decode("utf-8")
                self.assertEqual(
                    connector.read_secret(
                        config.openarchiver_api_key_file, "OpenArchiver"
                    ),
                    "oa-rotated",
                )
                self.assertEqual(
                    connector.read_secret(config.openrag_api_key_file, "OpenRAG"),
                    "rag-rotated",
                )
                self.assertNotIn("oa-rotated", rotated_page)
                self.assertNotIn("rag-rotated", rotated_page)
                self.assertTrue(wake.is_set())
                wake.clear()

                configuration_request = urllib.request.Request(
                    base + "/configuration",
                    data=urllib.parse.urlencode(
                        {
                            "csrf": state.snapshot()["csrf_token"],
                            "openrag_base_url": "http://new-openrag:8000/",
                            "connector_public_url": "https://connector.example.test/",
                            "ingestion_pool_size": "5",
                        }
                    ).encode("ascii"),
                )
                with urllib.request.urlopen(configuration_request) as response:
                    configuration_page = response.read().decode("utf-8")
                self.assertEqual(config.openrag_base_url, "http://new-openrag:8000")
                self.assertEqual(
                    config.connector_public_url, "https://connector.example.test"
                )
                self.assertEqual(config.ingestion_concurrency, 5)
                self.assertIn('value="http://new-openrag:8000"', configuration_page)
                self.assertIn(
                    'value="https://connector.example.test"', configuration_page
                )
                self.assertIn("Enregistrée sur le PVC", configuration_page)
                self.assertIn(
                    'name="ingestion_pool_size" value="5"', configuration_page
                )
                self.assertTrue(connector.POOL_RECONFIGURE.is_set())
                self.assertTrue(wake.is_set())
                wake.clear()

                body = urllib.parse.urlencode(
                    {"csrf": state.snapshot()["csrf_token"]}
                ).encode("ascii")
                request = urllib.request.Request(base + "/sources", data=body)
                with urllib.request.urlopen(request) as response:
                    self.assertEqual(response.status, 200)
                self.assertEqual(connector.selected_source_ids(config), [])
                self.assertTrue(wake.is_set())
                self.assertGreater(state.snapshot()["cycle_requested_at"], 0)
                self.assertTrue(state.snapshot()["force_inventory_requested"])
                with urllib.request.urlopen(base + "/") as response:
                    requested_page = response.read().decode("utf-8")
                self.assertNotIn('http-equiv="refresh"', requested_page)
                self.assertIn('role="tablist"', requested_page)
                self.assertIn("État de l’ingestion", requested_page)
                self.assertIn("Configuration", requested_page)
                self.assertIn('id="inventory-summary"', requested_page)
                with urllib.request.urlopen(base + "/inventory-status") as response:
                    status_page = response.read().decode("utf-8")
                self.assertIn("Inventaire demandé, en attente", status_page)
                self.assertIn('http-equiv="refresh"', status_page)
                self.assertTrue(state.cycle_started())
                with urllib.request.urlopen(base + "/inventory-status") as response:
                    active_page = response.read().decode("utf-8")
                self.assertIn("Inventaire en cours — Démarrage", active_page)
                self.assertIn('class="running"', active_page)
                self.assertIn('aria-busy="true"', active_page)
                with urllib.request.urlopen(base + "/status.json") as response:
                    live_status = json.loads(response.read())
                self.assertTrue(live_status["cycle_in_progress"])
                self.assertIn(
                    "Démarrage de l’inventaire",
                    live_status["inventory_status"],
                )
                with urllib.request.urlopen(base + "/ui.js") as response:
                    ui_script = response.read().decode("utf-8")
                self.assertIn('fetch("/status.json"', ui_script)
                self.assertIn('new EventSource("/events")', ui_script)
                self.assertIn("window.setInterval(update, 30000)", ui_script)
                self.assertNotIn("window.setInterval(update, 2000)", ui_script)
                self.assertNotIn("window.location.reload()", ui_script)
                self.assertIn("champs et sélections ont été conservés", ui_script)
                self.assertIn("Inventaire interrompu", ui_script)
                self.assertIn("openarchiver-connector-tab", ui_script)

                pause = urllib.request.Request(
                    base + "/pause",
                    data=urllib.parse.urlencode(
                        {
                            "csrf": state.snapshot()["csrf_token"],
                            "action": "pause",
                        }
                    ).encode("ascii"),
                )
                with urllib.request.urlopen(pause) as response:
                    self.assertEqual(response.status, 200)
                self.assertTrue(connector.is_paused(config))

                now = int(time.time())
                with connector.database(config) as db:
                    db.execute(
                        """INSERT INTO mailboxes(
                               source_id, path, first_seen_at, last_seen_at
                           ) VALUES (?, ?, ?, ?)""",
                        ("source-1", "INBOX", now, now),
                    )
                wake.clear()
                mailboxes = urllib.request.Request(
                    base + "/mailboxes",
                    data=urllib.parse.urlencode(
                        {
                            "csrf": state.snapshot()["csrf_token"],
                            "mailbox": json.dumps(["source-1", "INBOX"]),
                        }
                    ).encode("ascii"),
                )
                with urllib.request.urlopen(mailboxes) as response:
                    mailbox_page = response.read().decode("utf-8")
                self.assertTrue(connector.is_paused(config))
                self.assertFalse(wake.is_set())
                self.assertEqual(state.snapshot()["cycle_requested_at"], 0)
                self.assertIn("Enregistrer les dossiers", mailbox_page)
                self.assertNotIn(
                    "Enregistrer les dossiers et lancer",
                    mailbox_page,
                )

                removed_reset = urllib.request.Request(
                    base + "/reset",
                    data=urllib.parse.urlencode(
                        {
                            "csrf": state.snapshot()["csrf_token"],
                            "confirmation": "RESET",
                        }
                    ).encode("ascii"),
                )
                with self.assertRaises(urllib.error.HTTPError) as removed:
                    urllib.request.urlopen(removed_reset)
                self.assertEqual(removed.exception.code, 404)

                bad = urllib.request.Request(
                    base + "/scan",
                    data=urllib.parse.urlencode({"csrf": "bad"}).encode("ascii"),
                )
                with urllib.request.urlopen(bad) as response:
                    stale_page = response.read().decode("utf-8")
                    self.assertTrue(response.geturl().endswith("/?form=expired"))
                self.assertIn("OpenArchiver vers OpenRAG", stale_page)
                state.set_running(False)
                with self.assertRaises(urllib.error.HTTPError) as stopped:
                    urllib.request.urlopen(base + "/healthz")
                self.assertEqual(stopped.exception.code, 503)
                with self.assertRaises(urllib.error.HTTPError) as stopped:
                    urllib.request.urlopen(base + "/readyz")
                self.assertEqual(stopped.exception.code, 503)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

    def test_http_interface_follows_openrag_identity_and_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(
                Path(directory),
                openrag_auth_mode="auto",
                connector_public_url="https://connector.example.test",
            )
            state = connector.RuntimeState()
            state.set_running(True)
            principal = connector.ConnectorPrincipal(
                user_id="google-123",
                email="reader@example.test",
                name="Lectrice OpenRAG",
                provider="google",
                roles=frozenset({"user"}),
                permissions=frozenset({"knowledge:upload"}),
                authenticated=True,
                rbac_enforced=True,
            )

            class FakeAuth:
                def resolve(self, token=""):
                    return principal if token == "valid-token" else None

                def begin_login(self):
                    return "https://accounts.google.com/o/oauth2/v2/auth?test=1", "state-1"

                def complete_login(self, state, code):
                    if state != "state-1" or code != "code-1":
                        raise connector.ConnectorError("callback invalide")
                    return "valid-token"

                def logout(self, _token):
                    pass

            server = connector.ThreadingHTTPServer(
                ("127.0.0.1", 0),
                connector.make_http_handler(
                    config, state, auth_client=FakeAuth()
                ),
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            base = f"http://127.0.0.1:{server.server_port}"
            try:
                class NoRedirect(urllib.request.HTTPRedirectHandler):
                    def redirect_request(self, *_args, **_kwargs):
                        return None

                no_redirect = urllib.request.build_opener(NoRedirect())
                login_request = urllib.request.Request(
                    base + "/auth/login",
                    data=urllib.parse.urlencode(
                        {"csrf": state.snapshot()["csrf_token"]}
                    ).encode(),
                )
                with self.assertRaises(urllib.error.HTTPError) as login_redirect:
                    no_redirect.open(login_request)
                self.assertEqual(login_redirect.exception.code, 303)
                self.assertTrue(
                    login_redirect.exception.headers["Location"].startswith(
                        "https://accounts.google.com/"
                    )
                )
                state_cookie = login_redirect.exception.headers["Set-Cookie"]
                self.assertIn("openrag_oauth_state=state-1", state_cookie)
                self.assertIn("HttpOnly", state_cookie)
                self.assertIn("Secure", state_cookie)

                callback_request = urllib.request.Request(
                    base + "/auth/callback?state=state-1&code=code-1",
                    headers={"Cookie": "openrag_oauth_state=state-1"},
                )
                with self.assertRaises(urllib.error.HTTPError) as callback_redirect:
                    no_redirect.open(callback_request)
                self.assertEqual(callback_redirect.exception.code, 303)
                callback_cookies = callback_redirect.exception.headers.get_all(
                    "Set-Cookie"
                )
                self.assertTrue(
                    any("auth_token=valid-token" in value for value in callback_cookies)
                )

                with urllib.request.urlopen(base + "/") as response:
                    login_page = response.read().decode()
                    self.assertTrue(response.geturl().endswith("/login"))
                self.assertIn("Continuer avec Google", login_page)

                with self.assertRaises(urllib.error.HTTPError) as unauthorized:
                    urllib.request.urlopen(base + "/status.json")
                self.assertEqual(unauthorized.exception.code, 401)

                authenticated = urllib.request.Request(
                    base + "/", headers={"Cookie": "auth_token=valid-token"}
                )
                with urllib.request.urlopen(authenticated) as response:
                    page = response.read().decode()
                self.assertIn("Lectrice OpenRAG", page)
                self.assertIn("Identité synchronisée avec OpenRAG", page)
                self.assertIn("espace d’exploitation partagé", page)

                forbidden = urllib.request.Request(
                    base + "/pause",
                    data=urllib.parse.urlencode(
                        {
                            "csrf": state.snapshot()["csrf_token"],
                            "action": "pause",
                        }
                    ).encode(),
                    headers={"Cookie": "auth_token=valid-token"},
                )
                with self.assertRaises(urllib.error.HTTPError) as denied:
                    urllib.request.urlopen(forbidden)
                self.assertEqual(denied.exception.code, 403)

                removed_reset = urllib.request.Request(
                    base + "/reset",
                    data=urllib.parse.urlencode(
                        {"csrf": state.snapshot()["csrf_token"]}
                    ).encode(),
                    headers={"Cookie": "auth_token=valid-token"},
                )
                with self.assertRaises(urllib.error.HTTPError) as removed:
                    urllib.request.urlopen(removed_reset)
                self.assertEqual(removed.exception.code, 404)

                with connector.database(config) as db:
                    users = db.execute("SELECT id FROM users").fetchall()
                self.assertEqual([row["id"] for row in users], ["google-123"])
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

    def test_status_event_stream_emits_each_runtime_change(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            state = connector.RuntimeState()
            state.set_running(True)
            server = connector.ThreadingHTTPServer(
                ("127.0.0.1", 0), connector.make_http_handler(config, state)
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            response = urllib.request.urlopen(
                f"http://127.0.0.1:{server.server_port}/events", timeout=2
            )

            def read_event():
                fields = {}
                while True:
                    line = response.readline().decode("utf-8").rstrip("\r\n")
                    if not line:
                        return fields
                    key, value = line.split(":", 1)
                    fields[key] = value.lstrip()

            try:
                initial = read_event()
                self.assertEqual(initial["event"], "status")
                self.assertFalse(json.loads(initial["data"])["cycle_requested"])

                state.cycle_requested(force_inventory=True)
                changed = read_event()
                self.assertEqual(changed["event"], "status")
                self.assertGreater(int(changed["id"]), int(initial["id"]))
                self.assertTrue(json.loads(changed["data"])["cycle_requested"])
            finally:
                response.close()
                state.set_running(False)
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

    def test_status_page_uses_the_openrag_shell_without_external_assets(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            state = connector.RuntimeState()
            state.set_running(True)
            page = connector.render_status_page(config, state)

        self.assertIn('<div class="app">', page)
        self.assertIn('class="brand-logo"', page)
        self.assertIn('aria-label="Sections du connecteur"', page)
        self.assertEqual(page.count('role="tab"'), 3)
        self.assertIn('class="status-grid"', page)
        self.assertIn("@media(max-width:620px)", page)
        self.assertIn('name="viewport"', page)
        self.assertIn('<script src="/ui.js" defer></script>', page)
        self.assertIn('id="inventory-summary"', page)
        self.assertIn('id="inventory-dot"', page)
        self.assertIn('id="inventory-button"', page)
        self.assertIn('id="inventory-completion"', page)
        self.assertIn('id="mailbox-selection-list"', page)
        self.assertIn('id="mailbox-selection-badge"', page)
        self.assertIn('fetch("/?inventory-fragment=1"', connector.UI_SCRIPT)
        self.assertIn("new DOMParser()", connector.UI_SCRIPT)
        self.assertIn("choices.has(input.value)", connector.UI_SCRIPT)
        self.assertIn("observedActive = false", connector.UI_SCRIPT)

    def test_status_page_separates_selected_queue_from_local_history(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            self._insert_email(
                config,
                self.mail("selected", "source-1", hasAttachments=True),
            )
            self._insert_email(config, self.mail("historical", "source-2"))
            connector.replace_mailbox_selection(config, [("source-1", "INBOX")])

            global_counts = connector._status_counts(config)
            selected_counts = connector._status_counts(config, selected_only=True)
            attachment_mail_count = connector.selected_mail_with_attachments(config)
            page = connector.render_status_page(config, connector.RuntimeState())

        self.assertEqual(global_counts["emails"]["queued"], 2)
        self.assertEqual(selected_counts["emails"]["queued"], 1)
        self.assertEqual(attachment_mail_count, 1)
        self.assertIn("Mails dans la sélection", page)
        self.assertIn("Débit récent", page)
        self.assertIn("pas encore détaillées", page)
        self.assertIn("Historique local conservé : 2 mail(s)", page)
        self.assertIn("Les éléments hors sélection ne sont pas envoyés", page)

    def test_selected_attachment_counts_shared_attachment_once(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            self._insert_email(config, self.mail("mail-1"))
            self._insert_email(config, self.mail("mail-2"))
            detail = {
                "attachments": [
                    {
                        "id": "shared",
                        "filename": "shared.pdf",
                        "mimeType": "application/pdf",
                        "sizeBytes": 10,
                        "storagePath": "att/shared",
                    }
                ]
            }
            connector.inventory_attachments(config, "mail-1", detail)
            connector.inventory_attachments(config, "mail-2", detail)

            counts = connector._status_counts(config, selected_only=True)

        self.assertEqual(counts["attachments"]["queued"], 1)

    def test_last_cycle_is_restored_after_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            scan = connector.ScanResult(1, 1, True, False)
            connector.persist_cycle_outcome(
                config,
                completed_at=123456,
                scan=scan,
                processed=1,
                error="",
            )
            restored = connector.RuntimeState()
            connector.restore_cycle_outcome(config, restored)

            snapshot = restored.snapshot()
            self.assertEqual(snapshot["last_cycle_completed_at"], 123456)
            self.assertEqual(snapshot["last_scan"], scan)
            self.assertEqual(snapshot["last_processed"], 1)
            self.assertTrue(snapshot["ready"])
            self.assertIn("terminé le", connector.inventory_status(snapshot))

    def test_status_page_makes_running_inventory_explicit(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            connector.set_paused(config, True)
            state = connector.RuntimeState()
            state.cycle_started()
            page = connector.render_status_page(config, state)

        self.assertIn("Inventaire en cours…", page)
        self.assertIn(
            "Inventaire en cours — Démarrage de l’inventaire — démarré le",
            connector.inventory_status(state.snapshot()),
        )
        self.assertIn('type="button" disabled', page)
        self.assertIn(">En pause<", page)

        state.cycle_progress("Lecture des messages des sources sélectionnées", 5, 10)
        self.assertIn(
            "Lecture des messages des sources sélectionnées",
            connector.inventory_status(state.snapshot()),
        )
        live = json.loads(connector.render_live_status(state))
        self.assertEqual(live["progress_current"], 5)
        self.assertEqual(live["progress_total"], 10)
        self.assertEqual(live["cycle_stage"], "inventory")
        self.assertIn('id="cycle-progress"', page)
        self.assertIn('id="cycle-progress-bar"', page)
        self.assertIn('id="cycle-progress-label"', page)
        self.assertIn('role="tablist"', page)
        self.assertIn('data-workspace-tab="ingestion"', page)
        self.assertIn('data-workspace-tab="sources"', page)
        self.assertIn('data-workspace-tab="configuration"', page)
        self.assertIn('data-workspace-panel="sources" hidden', page)
        ingestion_panel, sources_and_configuration = page.split(
            '<section id="workspace-panel-sources"', 1
        )
        sources_panel = sources_and_configuration.split(
            '<section id="workspace-panel-configuration"', 1
        )[0]
        self.assertNotIn('id="retry-card"', ingestion_panel)
        self.assertIn('id="retry-card"', sources_panel)
        self.assertNotIn("Remise à zéro", page)
        self.assertNotIn('action="/reset"', page)
        self.assertNotIn('<script src="http', page)
        self.assertIn('action="/configuration"', page)
        self.assertIn('name="openrag_base_url"', page)
        self.assertIn('name="connector_public_url"', page)
        self.assertIn('name="ingestion_pool_size"', page)
        self.assertIn('min="1" max="6"', page)
        self.assertIn('value="http://openrag-backend:8000"', page)

        state.cycle_progress("Traitement de l’ingestion OpenRAG", 6, 10)
        ingestion_page = connector.render_status_page(config, state)
        ingestion_live = json.loads(connector.render_live_status(state))
        self.assertEqual(ingestion_live["cycle_stage"], "ingestion")
        self.assertIn(
            "Ingestion en cours — Traitement de l’ingestion",
            ingestion_live["inventory_status"],
        )
        self.assertIn(">Ingestion OpenRAG<", ingestion_page)
        self.assertIn("Inventaire terminé", ingestion_page)
        self.assertIn("Cycle en cours…", ingestion_page)
        self.assertNotIn("Inventaire en cours…", ingestion_page)

    def test_mailbox_discovery_selection_and_pause_are_conservative(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory), page_limit=10)
            self.select(config, "source-1")
            result = connector.scan_selected_sources(
                config,
                FakeArchive(
                    pages={
                        ("source-1", 1): {
                            "items": [
                                self.mail("inbox", path="INBOX"),
                                self.mail("archive", path="Archives/2026"),
                            ],
                            "total": 2,
                        }
                    }
                ),
            )
            self.assertTrue(result.complete)
            rows = connector.mailbox_rows(config)
            self.assertEqual(
                [(row["path"], row["message_count"]) for row in rows],
                [("Archives/2026", 1), ("INBOX", 1)],
            )
            self.assertIsNone(connector.claim_next(config, now=1))

            connector.replace_mailbox_selection(config, [("source-1", "INBOX")])
            claimed = connector.claim_next(config, now=1)
            self.assertIsNotNone(claimed)
            self.assertEqual(claimed.object_id, "inbox")

            connector.replace_mailbox_selection(
                config,
                [("source-1", "INBOX"), ("source-1", "Archives/2026")],
            )
            connector.set_paused(config, True)
            self.assertTrue(connector.is_paused(config))
            self.assertIsNone(connector.claim_next(config, now=1))
            connector.set_paused(config, False)
            self.assertEqual(connector.claim_next(config, now=1).object_id, "archive")

    def test_paused_runtime_creates_initial_inventory_without_indexing(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory), scan_interval_seconds=3600)
            self.select(config, "source-1")
            connector.set_paused(config, True)
            archive = FakeArchive(
                sources=[{"id": "source-1", "name": "Archive", "provider": "imap"}],
                pages={
                    ("source-1", 1): {
                        "items": [self.mail("mail-1", path="INBOX")],
                        "total": 1,
                    }
                },
            )
            rag = FakeOpenRAG()
            state = connector.RuntimeState()
            stop = threading.Event()
            wake = threading.Event()
            thread = threading.Thread(
                target=connector.runtime_loop,
                args=(config, state),
                kwargs={
                    "openarchiver": archive,
                    "openrag": rag,
                    "stop": stop,
                    "wake": wake,
                },
                daemon=True,
            )
            thread.start()
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline and not connector.mailbox_rows(config):
                time.sleep(0.01)
            stop.set()
            wake.set()
            thread.join(timeout=2)

            self.assertFalse(thread.is_alive())
            self.assertEqual(
                [row["path"] for row in connector.mailbox_rows(config)], ["INBOX"]
            )
            self.assertEqual(rag.uploads, [])
            self.assertTrue(state.snapshot()["ready"])

    def test_mailbox_selection_rejects_unknown_path(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            with self.assertRaisesRegex(connector.ConnectorError, "inconnu"):
                connector.replace_mailbox_selection(config, [("source-1", "INBOX")])

    def test_deselected_mailbox_blocks_an_existing_attachment(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            self._insert_email(config, self.mail("mail-1", path="INBOX"))
            connector.inventory_attachments(
                config,
                "mail-1",
                {
                    "attachments": [
                        {
                            "id": "att-1",
                            "filename": "document.pdf",
                            "sizeBytes": 4,
                            "storagePath": "att/path",
                        }
                    ]
                },
            )
            db = connector.connect_db(config)
            db.execute("UPDATE emails SET status='validated'")
            db.commit()
            db.close()

            connector.replace_mailbox_selection(config, [])
            self.assertIsNone(connector.claim_next(config, now=1))
            connector.replace_mailbox_selection(config, [("source-1", "INBOX")])
            claimed = connector.claim_next(config, now=1)
            self.assertIsNotNone(claimed)
            self.assertEqual((claimed.kind, claimed.object_id), ("attachment", "att-1"))

    def test_process_mail_downloads_once_then_validates(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            self._insert_email(config)
            item = connector.claim_next(config, now=1)
            self.assertIsNotNone(item)
            message = EmailMessage()
            message.set_content("Corps sans donnée réelle")
            archive = FakeArchive(downloads={"mail/mail-1.eml": message.as_bytes()})
            rag = FakeOpenRAG()
            connector.process_work_item(config, item, archive, rag)
            row = self.rows(config, "emails")[0]
            self.assertEqual(row["status"], "validated")
            self.assertEqual(
                row["sha256"],
                connector.hashlib.sha256(message.as_bytes()).hexdigest(),
            )
            self.assertEqual(
                rag.uploads[0][0],
                connector.mail_openrag_filename("mail-1", "Sujet de test"),
            )
            self.assertEqual(rag.uploads[0][1], message.as_bytes())

    def test_attachment_process_stream_hash_and_validate(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            self._insert_email(config)
            connector.inventory_attachments(
                config,
                "mail-1",
                {
                    "attachments": [
                        {
                            "id": "att",
                            "filename": "doc.pdf",
                            "sizeBytes": 4,
                            "storagePath": "att/path",
                        }
                    ]
                },
            )
            db = connector.connect_db(config)
            db.execute("UPDATE emails SET status='validated'")
            db.commit()
            db.close()
            item = connector.claim_next(config, now=1)
            connector.process_work_item(
                config,
                item,
                FakeArchive(downloads={"att/path": b"data"}),
                FakeOpenRAG(),
            )
            row = self.rows(config, "attachments")[0]
            self.assertEqual(row["status"], "validated")
            self.assertEqual(
                row["sha256"],
                "3a6eb0790f39ac87c94f3856b2dd2c5d110e6811602261a9a923d3bb23adc8b7",
            )

    def test_attachment_actual_oversize_becomes_non_indexable_without_upload(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory), max_file_bytes=3)
            self._insert_email(config)
            connector.inventory_attachments(
                config,
                "mail-1",
                {
                    "attachments": [
                        {
                            "id": "att",
                            "filename": "doc.pdf",
                            "sizeBytes": 0,
                            "storagePath": "att/path",
                        }
                    ]
                },
            )
            db = connector.connect_db(config)
            db.execute("UPDATE emails SET status='validated'")
            db.commit()
            db.close()
            item = connector.claim_next(config, now=1)
            rag = FakeOpenRAG()
            connector.process_work_item(
                config, item, FakeArchive(downloads={"att/path": b"four"}), rag
            )
            row = self.rows(config, "attachments")[0]
            self.assertEqual(row["status"], "non_indexable")
            self.assertEqual(rag.uploads, [])

    def test_failure_has_bounded_retries(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory), max_auto_retries=2)
            self._insert_email(config)
            archive = FakeArchive(
                downloads={"mail/mail-1.eml": b"Subject: test\r\n\r\nbody"}
            )
            connector.process_work_item(
                config,
                connector.claim_next(config, now=1),
                archive,
                FakeOpenRAG(fail=True),
            )
            row = self.rows(config, "emails")[0]
            self.assertEqual(row["status"], "failed")
            self.assertGreater(row["next_retry_at"], 0)
            db = connector.connect_db(config)
            db.execute("UPDATE emails SET next_retry_at=1")
            db.commit()
            db.close()
            connector.process_work_item(
                config,
                connector.claim_next(config, now=2),
                archive,
                FakeOpenRAG(fail=True),
            )
            row = self.rows(config, "emails")[0]
            self.assertEqual(row["attempts"], 2)
            self.assertEqual(row["next_retry_at"], 0)

    def test_unknown_openrag_task_is_marked_lost_and_reindexable(self):
        class LostOpenRAG(FakeOpenRAG):
            def wait(self, task_id):
                raise connector.LostTaskError(f"tâche OpenRAG inconnue: {task_id}")

        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory), retry_base_seconds=10)
            self._insert_email(config)
            item = connector.claim_next(config, now=1)
            connector.process_work_item(
                config,
                item,
                FakeArchive(downloads={"mail/mail-1.eml": b"Subject: test\r\n\r\nbody"}),
                LostOpenRAG(indexed=False),
            )
            row = self.rows(config, "emails")[0]
            self.assertEqual(row["status"], "lost")
            self.assertEqual(row["task_id"], "task-1")
            self.assertGreater(row["next_retry_at"], 0)
            self.assertIsNone(connector.claim_next(config, now=row["next_retry_at"] - 1))
            retry = connector.claim_next(config, now=row["next_retry_at"])
            self.assertEqual((retry.kind, retry.object_id, retry.attempts), ("email", "mail-1", 2))
            claimed = self.rows(config, "emails")[0]
            self.assertEqual(claimed["status"], "downloading")
            self.assertEqual(claimed["task_id"], "")

    def test_unknown_openrag_task_with_matching_chunks_is_validated(self):
        class LostOpenRAG(FakeOpenRAG):
            def wait(self, task_id):
                raise connector.LostTaskError(f"tâche OpenRAG inconnue: {task_id}")

        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            self._insert_email(config)
            item = connector.claim_next(config, now=1)
            connector.process_work_item(
                config,
                item,
                FakeArchive(downloads={"mail/mail-1.eml": b"Subject: test\r\n\r\nbody"}),
                LostOpenRAG(indexed=True),
            )
            row = self.rows(config, "emails")[0]
            self.assertEqual(row["status"], "validated")
            self.assertEqual(row["task_id"], "task-1")

    def test_completed_task_without_matching_chunks_is_failed(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            self._insert_email(config)
            item = connector.claim_next(config, now=1)
            connector.process_work_item(
                config,
                item,
                FakeArchive(downloads={"mail/mail-1.eml": b"Subject: test\r\n\r\nbody"}),
                FakeOpenRAG(indexed=False),
            )
            row = self.rows(config, "emails")[0]
            self.assertEqual(row["status"], "failed")
            self.assertIn("sans document indexé", row["last_error"])

    def test_retry_ui_separates_lost_and_failed_and_requeues_selection(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            lost_id = self._insert_email(config, self.mail("lost-mail"), "lost")
            failed_id = self._insert_email(config, self.mail("failed-mail"), "failed")
            with connector.database(config) as db:
                db.execute(
                    "UPDATE emails SET attempts=3, task_id='old-task', "
                    "last_error='tâche inconnue' WHERE id=?",
                    (lost_id,),
                )
                db.execute(
                    "UPDATE emails SET attempts=2, last_error='échec Langflow' WHERE id=?",
                    (failed_id,),
                )

            page = connector.render_status_page(config, connector.RuntimeState())
            self.assertIn('id="retry-tab-lost"', page)
            self.assertIn('id="retry-tab-failed"', page)
            self.assertIn("Lost · 1", page)
            self.assertIn("Failed · 1", page)
            self.assertIn(
                connector.mail_openrag_filename("lost-mail", "Sujet de test"), page
            )
            self.assertIn(
                connector.mail_openrag_filename("failed-mail", "Sujet de test"), page
            )

            updated = connector.requeue_objects(
                config, [("email", lost_id, "lost")]
            )
            self.assertEqual(updated, 1)
            rows = {row["id"]: row for row in self.rows(config, "emails")}
            self.assertEqual(rows[lost_id]["status"], "queued")
            self.assertEqual(rows[lost_id]["attempts"], 0)
            self.assertEqual(rows[lost_id]["task_id"], "")
            self.assertEqual(rows[failed_id]["status"], "failed")

    def test_retry_http_action_requests_immediate_cycle(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            object_id = self._insert_email(config, self.mail("failed-mail"), "failed")
            state = connector.RuntimeState()
            wake = threading.Event()
            server = connector.ThreadingHTTPServer(
                ("127.0.0.1", 0),
                connector.make_http_handler(config, state, wake=wake),
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                request = urllib.request.Request(
                    f"http://127.0.0.1:{server.server_port}/retry",
                    data=urllib.parse.urlencode(
                        {
                            "csrf": state.snapshot()["csrf_token"],
                            "object": json.dumps(["email", object_id, "failed"]),
                        }
                    ).encode("utf-8"),
                )
                with urllib.request.urlopen(request) as response:
                    self.assertEqual(response.status, 200)
                row = self.rows(config, "emails")[0]
                self.assertEqual(row["status"], "queued")
                self.assertTrue(wake.is_set())
                self.assertGreater(state.snapshot()["cycle_requested_at"], 0)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

    def test_recovery_only_marks_interrupted_operations_failed(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory), max_auto_retries=3)
            self._insert_email(config, self.mail("active"), "ingesting")
            self._insert_email(config, self.mail("done"), "validated")
            self.assertEqual(connector.recover_interrupted(config), 1)
            rows = {row["id"]: row for row in self.rows(config, "emails")}
            self.assertEqual(rows["active"]["status"], "failed")
            self.assertGreater(rows["active"]["next_retry_at"], 0)
            self.assertEqual(rows["done"]["status"], "validated")

    def test_recovery_does_not_exceed_retry_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory), max_auto_retries=3)
            self._insert_email(config, self.mail("exhausted"), "ingesting")
            db = connector.connect_db(config)
            db.execute("UPDATE emails SET attempts=3 WHERE id='exhausted'")
            db.commit()
            db.close()
            self.assertEqual(connector.recover_interrupted(config), 1)
            row = self.rows(config, "emails")[0]
            self.assertEqual(row["status"], "failed")
            self.assertEqual(row["next_retry_at"], 0)
            self.assertIsNone(connector.claim_next(config, now=2**31))

    def test_concurrent_claim_has_no_duplicate(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            self._insert_email(config)
            barrier = threading.Barrier(3)
            results = []

            def claim():
                barrier.wait()
                results.append(connector.claim_next(config, now=1))

            threads = [threading.Thread(target=claim) for _ in range(2)]
            for thread in threads:
                thread.start()
            barrier.wait()
            for thread in threads:
                thread.join()
            self.assertEqual(sum(result is not None for result in results), 1)

    def test_process_queue_keeps_slot_alive_after_claim_error(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory), ingestion_concurrency=1)
            item = connector.WorkItem("email", "mail-1", 1)
            with (
                mock.patch.object(
                    connector,
                    "claim_next",
                    side_effect=[sqlite3.OperationalError("database is locked"), item, None],
                ),
                mock.patch.object(connector, "process_work_item") as process,
                mock.patch.object(connector.time, "sleep") as sleep,
                mock.patch.object(connector, "selected_queue_pending_count", return_value=1),
            ):
                processed = connector.process_queue(
                    config, FakeArchive(), FakeOpenRAG()
                )

            self.assertEqual(processed, 1)
            process.assert_called_once_with(
                config, item, mock.ANY, mock.ANY
            )
            sleep.assert_called_once_with(1.0)

    def test_process_queue_keeps_slot_alive_after_unhandled_processing_error(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory), ingestion_concurrency=1)
            first = connector.WorkItem("email", "mail-1", 1)
            second = connector.WorkItem("email", "mail-2", 1)
            with (
                mock.patch.object(
                    connector, "claim_next", side_effect=[first, second, None]
                ),
                mock.patch.object(
                    connector,
                    "process_work_item",
                    side_effect=[RuntimeError("unexpected"), None],
                ) as process,
                mock.patch.object(connector.time, "sleep") as sleep,
                mock.patch.object(connector, "selected_queue_pending_count", return_value=2),
            ):
                processed = connector.process_queue(
                    config, FakeArchive(), FakeOpenRAG()
                )

            self.assertEqual(processed, 1)
            self.assertEqual(process.call_count, 2)
            sleep.assert_called_once_with(1.0)

    def test_process_queue_ignores_progress_callback_error(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory), ingestion_concurrency=1)
            first = connector.WorkItem("email", "mail-1", 1)
            second = connector.WorkItem("email", "mail-2", 1)
            progress_calls = 0

            def progress(_current, _total):
                nonlocal progress_calls
                progress_calls += 1
                if progress_calls == 2:
                    raise RuntimeError("UI unavailable")

            with (
                mock.patch.object(
                    connector, "claim_next", side_effect=[first, second, None]
                ),
                mock.patch.object(connector, "process_work_item") as process,
                mock.patch.object(connector, "selected_queue_pending_count", return_value=2),
            ):
                processed = connector.process_queue(
                    config,
                    FakeArchive(),
                    FakeOpenRAG(),
                    progress=progress,
                )

            self.assertEqual(processed, 2)
            self.assertEqual(process.call_count, 2)
            self.assertEqual(progress_calls, 3)

    def test_process_queue_restarts_after_manual_pool_change(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory), ingestion_concurrency=1)
            first = connector.WorkItem("email", "mail-1", 1)

            def process_then_reconfigure(*_args):
                connector.apply_runtime_pool_size(config, 3)

            with (
                mock.patch.object(
                    connector, "claim_next", side_effect=[first, AssertionError]
                ) as claim,
                mock.patch.object(
                    connector,
                    "process_work_item",
                    side_effect=process_then_reconfigure,
                ),
                mock.patch.object(
                    connector, "selected_queue_pending_count", return_value=2
                ),
            ):
                processed = connector.process_queue(
                    config, FakeArchive(), FakeOpenRAG()
                )

            self.assertEqual(processed, 1)
            self.assertEqual(claim.call_count, 1)
            self.assertEqual(config.ingestion_concurrency, 3)

    def test_no_automatic_delete_and_logs_exclude_keys_and_bodies(self):
        source = MODULE_PATH.read_text(encoding="utf-8")
        self.assertNotIn('method="DELETE"', source)
        self.assertNotIn("delete_openrag", source.lower())
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            self._insert_email(config)
            item = connector.claim_next(config, now=1)
            body = "CORPS-TRES-SECRET"
            archive = FakeArchive(
                downloads={"mail/mail-1.eml": f"Subject: test\r\n\r\n{body}".encode()}
            )
            stream = io.StringIO()
            handler = logging.StreamHandler(stream)
            connector.LOG.addHandler(handler)
            try:
                connector.process_work_item(
                    config, item, archive, FakeOpenRAG(fail=True)
                )
            finally:
                connector.LOG.removeHandler(handler)
            logs = stream.getvalue()
            self.assertNotIn("oa-secret", logs)
            self.assertNotIn("rag-secret", logs)
            self.assertNotIn(body, logs)


if __name__ == "__main__":
    unittest.main()
