"""Small interfaces for adding sources, extraction strategies, and storage backends."""

from collections.abc import Iterable, Sequence
from contextlib import AbstractContextManager
from typing import Any, Protocol

from knowledge.models import Chunk, Document, FetchedContent, SearchFilters, SourceItem


class Connector(Protocol):
    def discover(self, config: dict[str, Any]) -> Iterable[SourceItem]: ...
    def fetch(self, item: SourceItem) -> FetchedContent: ...


class Extractor(Protocol):
    def extract(self, content: FetchedContent) -> Document: ...


class ExpandingConnector(Connector, Protocol):
    """Optional capability for document attachments discovered after fetching a page."""

    def expand(self, content: FetchedContent) -> Iterable[SourceItem]: ...
    def verify_discovery(self) -> bool: ...


class Chunker(Protocol):
    @property
    def signature(self) -> str: ...
    def chunk(self, document: Document) -> Sequence[Chunk]: ...


class Embedder(Protocol):
    @property
    def signature(self) -> str: ...
    @property
    def dimensions(self) -> int: ...
    def embed_documents(self, texts: Sequence[str]) -> list[list[float]]: ...
    def embed_query(self, text: str) -> list[float]: ...


class Repository(Protocol):
    def source_lock(self, source: str) -> AbstractContextManager: ...
    def collection_lock(self, collection: str) -> AbstractContextManager: ...
    def start_run(self, connector: str, collection: str) -> str: ...
    def finish_run(self, run_id: str, summary: dict[str, Any]) -> None: ...
    def reconcile_collection(
        self,
        collection: str,
        source: str,
        keep_ids: set[str],
        run_id: str,
        summary: dict[str, Any],
    ) -> None: ...
    def current_fingerprint(self, document_id: str, collection: str) -> str | None: ...
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
    ) -> bool: ...
    def search(
        self,
        query: str,
        vector: Sequence[float],
        embedding_signature: str,
        filters: SearchFilters,
        limit: int,
    ) -> list[dict[str, Any]]: ...
    def read(
        self,
        document_id: str,
        version_id: str | None,
        start_chunk: int,
        limit: int,
    ) -> dict[str, Any] | None: ...
