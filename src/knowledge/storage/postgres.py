"""Canonical document store, transactional publication, and hybrid retrieval."""

from collections.abc import Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import quote
from uuid import uuid4

from knowledge.models import Chunk, Document, FetchedContent, SearchFilters


def vector_literal(vector: Sequence[float]) -> str:
    return "[" + ",".join(str(float(x)) for x in vector) + "]"


def citation_url(url: str, locator: str | None) -> str:
    if not locator:
        return url
    if locator.startswith("part:"):
        return url.rstrip("/") + "/" + quote(locator[5:], safe="")
    if locator.startswith("page=") and locator[5:].isdigit():
        return url + "#" + locator
    return url + "#" + quote(locator, safe="")


def fuse_rankings(lexical: list[dict], semantic: list[dict], limit: int) -> list[dict]:
    """Reciprocal rank fusion avoids comparing incompatible score scales."""
    merged: dict[tuple[str, int], dict] = {}
    for name, rows in (("lexical", lexical), ("semantic", semantic)):
        for rank, row in enumerate(rows, start=1):
            key = (str(row["version_id"]), row["ordinal"])
            hit = merged.setdefault(key, {**row, "score": 0.0, "matched_by": []})
            hit["score"] += 1 / (60 + rank)
            hit["matched_by"].append(name)
    return sorted(
        merged.values(), key=lambda row: (-row["score"], str(row["version_id"]), row["ordinal"])
    )[:limit]


def public_metadata(metadata: dict) -> dict:
    """Keep rich source records in storage without flooding model context."""
    result = {key: value for key, value in metadata.items() if key != "source_metadata"}
    source = metadata.get("source_metadata", {})
    result["source_metadata"] = {
        key: source[key]
        for key in (
            "base_path",
            "schema_name",
            "content_representation",
            "withdrawn_notice",
            "parent",
            "page_methods",
        )
        if key in source
    }
    return result


class PostgresRepository:
    def __init__(self, dsn: str):
        self.dsn = dsn

    def connect(self):
        import psycopg
        from psycopg.rows import dict_row

        return psycopg.connect(self.dsn, row_factory=dict_row, connect_timeout=10)

    def initialize(self):
        with self.connect() as conn:
            conn.execute("SELECT pg_advisory_xact_lock(7046031)")
            conn.execute(Path(__file__).with_name("schema.sql").read_text())

    def health(self):
        with self.connect() as conn:
            if not conn.execute(
                "SELECT version FROM knowledge_schema WHERE version = 1"
            ).fetchone():
                raise RuntimeError("Knowledge schema is not initialized")

    def start_run(self, connector: str, collection: str) -> str:
        run_id = str(uuid4())
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO ingestion_runs (id, connector, collection) VALUES (%s,%s,%s)",
                (run_id, connector, collection),
            )
        return run_id

    @contextmanager
    def source_lock(self, source):
        """Reject concurrent requests to the same source across all collections."""
        with self.connect() as conn:
            conn.autocommit = True
            key = "knowledge:source:" + source
            acquired = conn.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s, 0)) AS acquired", (key,)
            ).fetchone()["acquired"]
            if not acquired:
                raise RuntimeError(
                    f"Another {source} ingestion is already running; retry after it finishes"
                )
            try:
                yield
            finally:
                conn.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (key,))

    @contextmanager
    def collection_lock(self, collection):
        """Hold a session lock across the run's independent publication transactions."""
        with self.connect() as conn:
            conn.autocommit = True
            key = "knowledge:collection:" + collection
            conn.execute("SELECT pg_advisory_lock(hashtextextended(%s, 0))", (key,))
            try:
                yield
            finally:
                conn.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (key,))

    def reconcile_collection(self, collection, source, keep_ids, run_id, summary):
        """Retire only memberships; persist the audit in the same transaction."""
        from psycopg.types.json import Jsonb

        with self.connect() as conn:
            retired = conn.execute(
                """DELETE FROM collection_documents h USING documents d
                   WHERE h.document_id=d.id AND h.collection=%s AND d.source=%s
                   AND h.document_id <> ALL(%s::uuid[]) RETURNING h.document_id""",
                (collection, source, sorted(keep_ids)),
            ).fetchall()
            ids = sorted(str(row["document_id"]) for row in retired)
            summary["retirement"] = {
                "performed": True,
                "reason": "verified clean run",
                "count": len(ids),
                "ids": ids,
            }
            updated = conn.execute(
                """UPDATE ingestion_runs SET finished_at=now(), status=%s, summary=%s WHERE id=%s""",
                (summary["status"], Jsonb(summary), run_id),
            )
            if updated.rowcount != 1:
                raise ValueError("Reconciliation run does not exist")

    def finish_run(self, run_id: str, summary: dict[str, Any]):
        from psycopg.types.json import Jsonb

        with self.connect() as conn:
            conn.execute(
                """UPDATE ingestion_runs SET finished_at=now(), status=%s, summary=%s
                            WHERE id=%s""",
                (summary["status"], Jsonb(summary), run_id),
            )

    def current_fingerprint(self, document_id: str, collection: str) -> str | None:
        with self.connect() as conn:
            row = conn.execute(
                """SELECT v.fingerprint FROM collection_documents h
                                  JOIN document_versions v ON v.id=h.version_id
                                  WHERE h.document_id=%s AND h.collection=%s""",
                (document_id, collection),
            ).fetchone()
            return row["fingerprint"] if row else None

    def publish(
        self,
        document: Document,
        raw: FetchedContent,
        collection: str,
        fingerprint: str,
        chunks: Sequence[Chunk],
        vectors: Sequence[Sequence[float]],
        embedding_signature: str,
        processing: dict[str, Any] | None = None,
    ) -> bool:
        from psycopg.types.json import Jsonb

        if len(chunks) != len(vectors):
            raise ValueError("Chunk/vector count mismatch")
        with self.connect() as conn:
            # Serialize publication for this document, including first-time inserts.
            conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (document.id,))
            conn.execute(
                """INSERT INTO documents (id, source, source_id) VALUES (%s,%s,%s)
                            ON CONFLICT DO NOTHING""",
                (document.id, document.source, document.source_id),
            )
            current = conn.execute(
                """SELECT v.fingerprint FROM collection_documents h
                                      JOIN document_versions v ON v.id=h.version_id
                                      WHERE h.document_id=%s AND h.collection=%s""",
                (document.id, collection),
            ).fetchone()
            if current and current["fingerprint"] == fingerprint:
                return False
            existing = conn.execute(
                "SELECT id FROM document_versions WHERE document_id=%s AND fingerprint=%s",
                (document.id, fingerprint),
            ).fetchone()
            version_id = str(existing["id"]) if existing else str(uuid4())
            if not existing:
                conn.execute(
                    """INSERT INTO document_versions
                    (id, document_id, fingerprint, metadata, extracted_text, raw_content, embedding_signature, processing)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        version_id,
                        document.id,
                        fingerprint,
                        Jsonb(document.metadata()),
                        document.text,
                        Jsonb(raw.payload),
                        embedding_signature,
                        Jsonb(processing or {}),
                    ),
                )
                with conn.cursor() as cursor:
                    cursor.executemany(
                        """INSERT INTO chunks
                        (version_id, ordinal, text, heading_path, locator, embedding, search_text)
                        VALUES (%s,%s,%s,%s,%s,%s::vector,to_tsvector('english', %s))""",
                        [
                            (
                                version_id,
                                c.ordinal,
                                c.text,
                                Jsonb(c.heading_path),
                                c.locator,
                                vector_literal(v),
                                " ".join([document.title, *c.heading_path, c.text]),
                            )
                            for c, v in zip(chunks, vectors, strict=True)
                        ],
                    )
            # This pointer changes only after every chunk has been persisted successfully.
            conn.execute(
                """INSERT INTO collection_documents (collection, document_id, version_id)
                            VALUES (%s,%s,%s) ON CONFLICT (collection, document_id)
                            DO UPDATE SET version_id=EXCLUDED.version_id""",
                (collection, document.id, version_id),
            )
        return True

    def search(
        self,
        query: str,
        vector: Sequence[float],
        embedding_signature: str,
        filters: SearchFilters,
        limit: int,
    ) -> list[dict[str, Any]]:
        from psycopg.types.json import Jsonb

        where = ["v.embedding_signature=%s"]
        parameters: list[Any] = [embedding_signature]
        for field, value in (("source", filters.source), ("document_type", filters.document_type)):
            if value:
                where.append(f"v.metadata->>'{field}'=%s")
                parameters.append(value)
        if filters.publisher:
            where.append("v.metadata->'publishers' @> %s")
            parameters.append(Jsonb([filters.publisher]))
        membership = "h.version_id=v.id"
        if filters.collection:
            membership += " AND h.collection=%s"
            parameters.append(filters.collection)
        where.append(f"EXISTS (SELECT 1 FROM collection_documents h WHERE {membership})")
        common = """SELECT v.document_id, v.id AS version_id, v.metadata,
                    c.ordinal, c.text, c.heading_path, c.locator
                    FROM chunks c JOIN document_versions v ON v.id=c.version_id WHERE """
        common += " AND ".join(where)
        candidates = max(30, limit * 4)
        with self.connect() as conn:
            # Both candidate sets use the same snapshot during concurrent refreshes.
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            conn.execute("SET LOCAL statement_timeout = '15s'")
            lexical = conn.execute(
                common
                + """ AND c.search_text @@ websearch_to_tsquery('english', %s)
                    ORDER BY ts_rank_cd(c.search_text, websearch_to_tsquery('english', %s)) DESC,
                    v.id, c.ordinal LIMIT %s""",
                (*parameters, query, query, candidates),
            ).fetchall()
            semantic = conn.execute(
                common + " ORDER BY c.embedding <=> %s::vector, v.id, c.ordinal LIMIT %s",
                (*parameters, vector_literal(vector), candidates),
            ).fetchall()
        return [self.present(hit) for hit in fuse_rankings(lexical, semantic, limit)]

    @staticmethod
    def present(row: dict) -> dict[str, Any]:
        metadata = row["metadata"]
        result = {
            "document_id": str(row["document_id"]),
            "version_id": str(row["version_id"]),
            "chunk": row["ordinal"],
            "text": row["text"],
            "heading_path": row["heading_path"],
            "locator": row["locator"],
            "citation_url": citation_url(metadata["canonical_url"], row["locator"]),
            "metadata": public_metadata(metadata),
        }
        if "score" in row:
            result.update(score=row["score"], matched_by=row["matched_by"])
        return result

    def read(
        self, document_id: str, version_id: str | None, start_chunk: int, limit: int
    ) -> dict[str, Any] | None:
        with self.connect() as conn:
            if version_id:
                version = conn.execute(
                    """SELECT * FROM document_versions
                                           WHERE id=%s AND document_id=%s""",
                    (version_id, document_id),
                ).fetchone()
            else:
                version = conn.execute(
                    """SELECT v.* FROM document_versions v
                    WHERE v.document_id=%s AND EXISTS
                    (SELECT 1 FROM collection_documents h WHERE h.version_id=v.id)
                    ORDER BY v.created_at DESC, v.id LIMIT 1""",
                    (document_id,),
                ).fetchone()
            if not version:
                return None
            rows = conn.execute(
                """SELECT ordinal, text, heading_path, locator FROM chunks
                                   WHERE version_id=%s AND ordinal >= %s ORDER BY ordinal LIMIT %s""",
                (version["id"], start_chunk, limit + 1),
            ).fetchall()
            more = len(rows) > limit
            passages = [
                self.present(
                    {
                        **row,
                        "document_id": document_id,
                        "version_id": version["id"],
                        "metadata": version["metadata"],
                    }
                )
                for row in rows[:limit]
            ]
            return {
                "document_id": document_id,
                "version_id": str(version["id"]),
                "metadata": public_metadata(version["metadata"]),
                "passages": passages,
                "next_chunk": rows[limit]["ordinal"] if more else None,
            }
