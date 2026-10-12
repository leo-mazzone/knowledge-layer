"""Opt-in integration checks against a disposable PostgreSQL + pgvector database.

KNOWLEDGE_TEST_DATABASE_URL enables these checks. Tables use an isolated schema;
the vector extension is installed in public if absent.
"""

import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Event
from uuid import uuid4

import pytest

from knowledge.models import Document, FetchedContent, SearchFilters, Section
from knowledge.processing import SectionChunker
from knowledge.storage.postgres import PostgresRepository


@pytest.mark.skipif(
    not (os.getenv("KNOWLEDGE_TEST_DATABASE_URL")), reason="Set KNOWLEDGE_TEST_DATABASE_URL"
)
class TestPostgres:
    @pytest.fixture(scope="class", autouse=True)
    def database(self, request):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import make_conninfo

        dsn = os.environ["KNOWLEDGE_TEST_DATABASE_URL"]
        schema = "knowledge_test_" + uuid4().hex
        with psycopg.connect(dsn) as conn:
            conn.execute("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public")
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            repository = PostgresRepository(
                make_conninfo(dsn, options=f"-c search_path={schema},public")
            )
            repository.initialize()
            repository.initialize()
            request.cls.repository = repository
            yield repository
        finally:
            with psycopg.connect(dsn) as conn:
                conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))

    @pytest.fixture(autouse=True)
    def setup(self):
        self.collection = "exports-" + uuid4().hex
        self.document = Document(
            "test",
            uuid4().hex,
            "https://www.gov.uk/demo",
            "Export evidence",
            (
                Section("Keep export evidence and customs records.", ("Evidence",)),
                Section("Apply for an EORI number.", ("Registration",)),
            ),
            document_type="guidance",
            publishers=("HMRC",),
        )
        self.raw = FetchedContent("/demo", {"body": "original"})
        self.chunks = SectionChunker().chunk(self.document)
        self.repository.publish(
            self.document,
            self.raw,
            self.collection,
            "first",
            self.chunks,
            [[1, 0, 0], [0, 1, 0]],
            "test:3",
        )

    def test_hybrid_search_filters_and_read_pagination(self):
        hits = self.repository.search(
            "export evidence",
            [1, 0, 0],
            "test:3",
            SearchFilters(collection=self.collection, publisher="HMRC"),
            5,
        )
        assert hits[0]["document_id"] == self.document.id
        assert "lexical" in hits[0]["matched_by"]
        assert "semantic" in hits[0]["matched_by"]
        page = self.repository.read(self.document.id, hits[0]["version_id"], 0, 1)
        assert page["next_chunk"] == 1
        assert len(page["passages"]) == 1
        assert (
            self.repository.search(
                "exports", [1, 0, 0], "test:3", SearchFilters(collection="absent"), 5
            )
            == []
        )
        # Incompatible dimensions must never enter the distance calculation.
        assert (
            self.repository.search(
                "exports", [1, 0], "another-model:2", SearchFilters(collection=self.collection), 5
            )
            == []
        )

    def test_publish_failure_rolls_back_and_versions_remain_readable(self):
        from psycopg.errors import UniqueViolation

        old = self.repository.read(self.document.id, None, 0, 5)["version_id"]
        updated = replace(self.document, title="Updated export evidence")
        with pytest.raises(UniqueViolation):
            self.repository.publish(
                updated,
                self.raw,
                self.collection,
                "broken",
                [self.chunks[0], self.chunks[0]],
                [[1, 0, 0], [1, 0, 0]],
                "test:3",
            )
        assert self.repository.current_fingerprint(self.document.id, self.collection) == "first"
        self.repository.publish(
            updated,
            self.raw,
            self.collection,
            "second",
            self.chunks,
            [[1, 0, 0], [0, 1, 0]],
            "test:3",
        )
        assert self.repository.read(self.document.id, None, 0, 5)["version_id"] != old
        assert (
            self.repository.read(self.document.id, old, 0, 5)["metadata"]["title"]
            == "Export evidence"
        )
        assert not self.repository.publish(
            updated,
            self.raw,
            self.collection,
            "second",
            self.chunks,
            [[1, 0, 0], [0, 1, 0]],
            "test:3",
        )

    def test_prefix_change_excludes_old_vectors_and_retains_history(self):
        from knowledge.embedding_profiles import resolve_profile

        old = self.repository.read(self.document.id, None, 0, 5)["version_id"]
        signature = "test:3" + resolve_profile("nomic").signature_suffix
        filters = SearchFilters(collection=self.collection)
        assert self.repository.search("export evidence", [1, 0, 0], signature, filters, 5) == []
        self.repository.publish(
            self.document,
            self.raw,
            self.collection,
            "prefixed",
            self.chunks,
            [[1, 0, 0], [0, 1, 0]],
            signature,
        )
        hits = self.repository.search("export evidence", [1, 0, 0], signature, filters, 5)
        assert "lexical" in hits[0]["matched_by"]
        assert "semantic" in hits[0]["matched_by"]
        assert hits[0]["version_id"] != old
        assert self.repository.search("export evidence", [1, 0, 0], "test:3", filters, 5) == []
        assert self.repository.read(self.document.id, old, 0, 5)["version_id"] == old

    def test_reconciliation_audit_rollback_source_scope_and_historical_reads(self):
        old = self.repository.read(self.document.id, None, 0, 5)["version_id"]
        other_collection = self.collection + "-other"
        self.repository.publish(
            self.document,
            self.raw,
            other_collection,
            "first",
            self.chunks,
            [[1, 0, 0], [0, 1, 0]],
            "test:3",
        )
        unrelated = replace(self.document, source="another-source", source_id=uuid4().hex)
        self.repository.publish(
            unrelated,
            self.raw,
            self.collection,
            "unrelated",
            self.chunks,
            [[1, 0, 0], [0, 1, 0]],
            "test:3",
        )
        summary = {"status": "succeeded", "errors": []}
        with self.repository.collection_lock(self.collection):
            with pytest.raises(ValueError, match="does not exist"):
                self.repository.reconcile_collection(
                    self.collection, "test", set(), str(uuid4()), summary
                )
            assert self.repository.current_fingerprint(self.document.id, self.collection) == "first"
            run_id = self.repository.start_run("test", self.collection)
            self.repository.reconcile_collection(self.collection, "test", set(), run_id, summary)
        assert summary["retirement"]["ids"] == [self.document.id]
        assert self.repository.current_fingerprint(self.document.id, self.collection) is None
        assert self.repository.current_fingerprint(self.document.id, other_collection) == "first"
        assert self.repository.current_fingerprint(unrelated.id, self.collection) == "unrelated"
        assert self.repository.read(self.document.id, old, 0, 5)["version_id"] == old
        with self.repository.connect() as conn:
            row = conn.execute(
                "SELECT status, summary FROM ingestion_runs WHERE id=%s", (run_id,)
            ).fetchone()
        assert row["status"] == "succeeded"
        assert row["summary"]["retirement"]["ids"] == [self.document.id]
        hits = self.repository.search(
            "export evidence", [1, 0, 0], "test:3", SearchFilters(collection=self.collection), 5
        )
        assert self.document.id not in [hit["document_id"] for hit in hits]

    def test_collection_lock_serializes_same_collection_but_not_others(self):
        entered = Event()
        release = Event()
        second_entered = Event()

        def first():
            with self.repository.collection_lock(self.collection):
                entered.set()
                if not release.wait(10):
                    raise RuntimeError("Test lock was not released")

        def second():
            with self.repository.collection_lock(self.collection):
                second_entered.set()

        with ThreadPoolExecutor(max_workers=2) as executor:
            first_future = executor.submit(first)
            try:
                assert entered.wait(5)
                second_future = executor.submit(second)
                assert not second_entered.wait(0.1)
                with self.repository.collection_lock(self.collection + "-independent"):
                    assert not second_entered.is_set()
            finally:
                release.set()
            first_future.result(timeout=5)
            second_future.result(timeout=5)
        assert second_entered.is_set()

    def test_source_lock_rejects_concurrent_runs_and_releases_after_failure(self):
        source = "govuk-lock-test-" + uuid4().hex
        with self.repository.source_lock(source):
            with (
                pytest.raises(RuntimeError, match="already running"),
                self.repository.source_lock(source),
            ):
                pytest.fail("Concurrent source run was allowed")
            with self.repository.source_lock(source + "-another"):
                pass
        with (
            pytest.raises(RuntimeError, match="simulated failure"),
            self.repository.source_lock(source),
        ):
            raise RuntimeError("simulated failure")
        with self.repository.source_lock(source):
            pass
