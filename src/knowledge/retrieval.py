"""Retrieval use cases, callable from MCP or another application adapter."""

from uuid import UUID

from knowledge.models import SearchFilters
from knowledge.ports import Embedder, Repository
from knowledge.processing import validate_vectors


class KnowledgeService:
    def __init__(self, repository: Repository, embedder: Embedder):
        self.repository = repository
        self.embedder = embedder

    def search(self, query: str, filters: SearchFilters, limit: int = 6) -> dict:
        query = query.strip()
        if not query or len(query) > 2000:
            raise ValueError("Query must contain 1–2000 characters")
        if not 1 <= limit <= 10:
            raise ValueError("Limit must be between 1 and 10")
        vector = self.embedder.embed_query(query)
        validate_vectors([vector], 1, self.embedder.dimensions)
        hits = self.repository.search(query, vector, self.embedder.signature, filters, limit)
        return {
            "query": query,
            "results": hits,
            "note": "Results are stored snapshots. Scores are ranks, not confidence. "
            "No matches does not establish that no requirement exists.",
        }

    def read(
        self, document_id: str, version_id: str | None = None, start_chunk: int = 0, limit: int = 5
    ) -> dict:
        UUID(document_id)
        if version_id:
            UUID(version_id)
        if start_chunk < 0 or not 1 <= limit <= 10:
            raise ValueError("Require start_chunk >= 0 and limit between 1 and 10")
        document = self.repository.read(document_id, version_id, start_chunk, limit)
        if document is None:
            raise ValueError("Document or version not found")
        return document
