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
    def __init__(self, body=b"", status=200, chunks=None):
        self.body = io.BytesIO(body)
        self.status = status
        self.chunks = iter(chunks) if chunks is not None else None

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
    def __init__(self, fail=False):
        self.fail = fail
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

    def test_secret_rotation_is_atomic_private_and_never_rendered(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            connector.write_secret(
                config.openarchiver_api_key_file, "nouvelle-cle-oa", "OpenArchiver"
            )
            connector.write_secret(
                config.openrag_api_key_file, "nouvelle-cle-rag", "OpenRAG"
            )
            self.assertEqual(
                connector.read_secret(config.openarchiver_api_key_file, "OpenArchiver"),
                "nouvelle-cle-oa",
            )
            self.assertEqual(config.openarchiver_api_key_file.stat().st_mode & 0o777, 0o600)
            page = connector.render_status_page(config, connector.RuntimeState())
            self.assertIn("OpenArchiver : Configurée", page)
            self.assertIn("OpenRAG : Configurée", page)
            self.assertNotIn("nouvelle-cle-oa", page)
            self.assertNotIn("nouvelle-cle-rag", page)

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
            finally:
                version_db.close()
            self.assertFalse(connector.is_paused(config))

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
            self.assertEqual(row["openrag_filename"], "openarchiver-mail-mail-1.eml")
            self.assertEqual(row["status"], "queued")
            self.assertEqual(row["sha256"], "")
            self.assertEqual(row["task_id"], "")

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
            result = connector.scan_selected_sources(config, client)
            self.assertEqual(result, connector.ScanResult(1, 3, True, False))
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

    def test_pages_moved_trigger_one_stabilisation_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            self.select(config, "source-1")
            client = FakeArchive(
                pages={
                    ("source-1", 1): [
                        {"items": [self.mail("a"), self.mail("b")], "total": 3},
                        {"items": [self.mail("a"), self.mail("b")], "total": 3},
                    ],
                    ("source-1", 2): [
                        {"items": [self.mail("b")], "total": 3},
                        {"items": [self.mail("c")], "total": 3},
                    ],
                }
            )
            result = connector.scan_selected_sources(config, client)
            self.assertTrue(result.complete)
            self.assertTrue(result.repeated)
            self.assertEqual(result.emails, 3)

    def test_incomplete_second_pass_does_not_mark_missing(self):
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
            self.assertFalse(result.complete)
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
                rows[0]["openrag_filename"], "openarchiver-attachment-att-1.xlsx"
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
            self.assertEqual(
                connector.attachment_openrag_filename("x", "../../evil.EXE"),
                "openarchiver-attachment-x.exe",
            )

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
        first = connector.mail_openrag_filename('id"\r\nheader')
        second = connector.mail_openrag_filename('id"\r\nother')
        self.assertEqual(first, connector.mail_openrag_filename('id"\r\nheader'))
        self.assertNotEqual(first, second)
        self.assertNotIn("\r", first)
        self.assertNotIn('"', first)

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

                body = urllib.parse.urlencode(
                    {"csrf": state.snapshot()["csrf_token"]}
                ).encode("ascii")
                request = urllib.request.Request(base + "/sources", data=body)
                with urllib.request.urlopen(request) as response:
                    self.assertEqual(response.status, 200)
                self.assertEqual(connector.selected_source_ids(config), [])
                self.assertTrue(wake.is_set())
                self.assertGreater(state.snapshot()["cycle_requested_at"], 0)
                with urllib.request.urlopen(base + "/") as response:
                    requested_page = response.read().decode("utf-8")
                self.assertNotIn('http-equiv="refresh"', requested_page)
                self.assertIn("Rafraîchir l’état", requested_page)
                self.assertIn('id="inventory-summary"', requested_page)
                with urllib.request.urlopen(base + "/inventory-status") as response:
                    status_page = response.read().decode("utf-8")
                self.assertIn("Inventaire demandé, en attente", status_page)
                self.assertIn('http-equiv="refresh"', status_page)
                state.cycle_started()
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
                self.assertNotIn("window.location.reload()", ui_script)
                self.assertIn("champs et sélections ont été conservés", ui_script)
                self.assertIn("Inventaire interrompu", ui_script)

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

                reset = urllib.request.Request(
                    base + "/reset",
                    data=urllib.parse.urlencode(
                        {
                            "csrf": state.snapshot()["csrf_token"],
                            "confirmation": "RESET",
                        }
                    ).encode("ascii"),
                )
                with urllib.request.urlopen(reset) as response:
                    reset_page = response.read().decode("utf-8")
                self.assertTrue(state.snapshot()["reset_requested_at"])
                self.assertIn("Remise à zéro demandée", reset_page)
                self.assertIn("Remettre la base locale à zéro", reset_page)

                bad = urllib.request.Request(
                    base + "/scan",
                    data=urllib.parse.urlencode({"csrf": "bad"}).encode("ascii"),
                )
                with self.assertRaises(urllib.error.HTTPError) as caught:
                    urllib.request.urlopen(bad)
                self.assertEqual(caught.exception.code, 403)
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

    def test_status_page_uses_the_openrag_shell_without_external_assets(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            state = connector.RuntimeState()
            state.set_running(True)
            page = connector.render_status_page(config, state)

        self.assertIn('<div class="app">', page)
        self.assertIn('class="brand-logo"', page)
        self.assertIn('aria-label="Navigation"', page)
        self.assertIn('class="status-grid"', page)
        self.assertIn("@media(max-width:620px)", page)
        self.assertIn('name="viewport"', page)
        self.assertIn('<script src="/ui.js" defer></script>', page)
        self.assertIn('id="inventory-summary"', page)
        self.assertIn('id="inventory-dot"', page)
        self.assertIn('id="inventory-button"', page)
        self.assertIn('id="inventory-completion"', page)

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

        state.cycle_progress("Lecture des messages des sources sélectionnées")
        self.assertIn(
            "Lecture des messages des sources sélectionnées",
            connector.inventory_status(state.snapshot()),
        )
        self.assertNotIn('<script src="http', page)
        self.assertNotIn("https://", page)
        self.assertNotIn("http://", page)

    def test_runtime_manifests_expose_the_interface_securely_on_the_lan(self):
        root = MODULE_PATH.parent
        deployment = (root / "deployment.yaml").read_text(encoding="utf-8")
        service = (root / "service.yaml").read_text(encoding="utf-8")
        kustomization = (root / "kustomization.yaml").read_text(encoding="utf-8")
        ingress_http = (root / "ingress-http.yaml").read_text(encoding="utf-8")
        ingress_https = (root / "ingress-https.yaml").read_text(encoding="utf-8")
        middleware_auth = (root / "middleware-auth.yaml").read_text(encoding="utf-8")
        middleware_headers = (root / "middleware-headers.yaml").read_text(
            encoding="utf-8"
        )
        middleware_redirect = (root / "middleware-redirect-https.yaml").read_text(
            encoding="utf-8"
        )
        hostname = "openrag-openarchiver-connector.ferme-de-pommerieux.fr"
        self.assertIn("automountServiceAccountToken: false", deployment)
        self.assertIn("readOnlyRootFilesystem: true", deployment)
        self.assertIn("openrag-openarchiver-connector-api", deployment)
        self.assertIn(
            "http://openarchiver-api.openarchiver.svc.cluster.local:4000/v1",
            deployment,
        )
        self.assertIn("openarchiver-api-key", deployment)
        self.assertIn("openrag-api-key", deployment)
        self.assertIn("name: bootstrap-api-keys", deployment)
        self.assertIn("optional: true", deployment)
        self.assertIn("/state/secrets/openarchiver-api-key", deployment)
        self.assertIn("/state/secrets/openrag-api-key", deployment)
        self.assertIn("chmod 0600", deployment)
        self.assertIn("OPENRAG_INGEST_DIRECTORY", deployment)
        self.assertIn("/shared/openrag-documents/openarchiver", deployment)
        self.assertIn("/v1/documents/ingest-path", deployment)
        self.assertIn("claimName: openrag-shared", deployment)
        self.assertIn("type: ClusterIP", service)
        self.assertNotIn("NodePort", service)
        self.assertIn(hostname, ingress_http)
        self.assertIn(hostname, ingress_https)
        self.assertIn("router.entrypoints: web\n", ingress_http)
        self.assertIn("redirect-https@kubernetescrd", ingress_http)
        self.assertIn("router.entrypoints: websecure", ingress_https)
        self.assertIn('router.tls: "true"', ingress_https)
        self.assertIn("auth@kubernetescrd", ingress_https)
        self.assertIn("headers@kubernetescrd", ingress_https)
        self.assertIn(
            "secret: openrag-openarchiver-connector-basic-auth",
            middleware_auth,
        )
        self.assertIn("contentTypeNosniff: true", middleware_headers)
        self.assertIn("scheme: https", middleware_redirect)
        self.assertNotIn(
            "ipAllowList", "\n".join((middleware_auth, middleware_headers))
        )
        for resource in (
            "pvc.yaml",
            "deployment.yaml",
            "service.yaml",
            "middleware-auth.yaml",
            "middleware-headers.yaml",
            "middleware-redirect-https.yaml",
            "ingress-http.yaml",
            "ingress-https.yaml",
        ):
            self.assertIn(f"- {resource}", kustomization)

    def test_runtime_bundle_contains_no_kubernetes_secret_value(self):
        root = MODULE_PATH.parent
        manifests = "\n".join(
            path.read_text(encoding="utf-8") for path in root.glob("*.yaml")
        )
        self.assertNotIn("kind: Secret", manifests)
        self.assertNotIn("stringData:", manifests)
        self.assertNotIn("data:\n  openarchiver-api-key", manifests)

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

    def test_pending_reset_clears_local_database_and_stays_paused(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory), scan_interval_seconds=3600)
            self._insert_email(config)
            state = connector.RuntimeState()
            state.reset_requested()
            connector.set_paused(config, True)
            stop = threading.Event()
            wake = threading.Event()
            thread = threading.Thread(
                target=connector.runtime_loop,
                args=(config, state),
                kwargs={
                    "openarchiver": FakeArchive(),
                    "openrag": FakeOpenRAG(),
                    "stop": stop,
                    "wake": wake,
                },
                daemon=True,
            )
            thread.start()
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline and not state.snapshot()["last_reset_at"]:
                time.sleep(0.01)
            stop.set()
            wake.set()
            thread.join(timeout=2)

            self.assertFalse(thread.is_alive())
            self.assertGreater(state.snapshot()["last_reset_at"], 0)
            self.assertTrue(connector.is_paused(config))
            db = connector.connect_db(config)
            try:
                counts = {
                    table: db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    for table in (
                        "sources",
                        "mailboxes",
                        "emails",
                        "attachments",
                        "email_attachments",
                    )
                }
            finally:
                db.close()
            self.assertEqual(counts, {table: 0 for table in counts})

    def test_paused_runtime_manually_inventories_without_indexing(self):
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
            time.sleep(0.05)
            self.assertEqual(connector.mailbox_rows(config), [])
            state.cycle_requested()
            wake.set()
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

    def test_openarchiver_backend_has_a_private_service(self):
        manifest = (
            MODULE_PATH.parents[3] / "apps/openarchiver/openarchiver.yaml"
        ).read_text(encoding="utf-8")
        api_service = manifest.split("  name: openarchiver-api", 1)[1].split("---", 1)[
            0
        ]
        self.assertIn("type: ClusterIP", api_service)
        self.assertIn("port: 4000", api_service)
        self.assertIn("targetPort: backend", api_service)
        self.assertNotIn("NodePort", api_service)

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
            self.assertEqual(rag.uploads[0][0], "openarchiver-mail-mail-1.eml")
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
