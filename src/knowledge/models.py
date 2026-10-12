"""Domain models: independent of connectors, database drivers, and MCP."""

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid5


def utcnow() -> str:
    return datetime.now(UTC).isoformat()


def digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


@dataclass(frozen=True)
class SourceItem:
    """Opaque source reference; connectors interpret it."""

    reference: str
    metadata: dict[str, Any] = field(default_factory=dict)
    key: str | None = None


@dataclass(frozen=True)
class FetchedContent:
    reference: str
    payload: dict[str, Any]
    retrieved_at: str = field(default_factory=utcnow)


@dataclass(frozen=True)
class Section:
    text: str
    heading_path: tuple[str, ...] = ()
    locator: str | None = None


@dataclass(frozen=True)
class Document:
    source: str
    source_id: str
    canonical_url: str
    title: str
    sections: tuple[Section, ...]
    description: str = ""
    document_type: str = "document"
    language: str = "en"
    publishers: tuple[str, ...] = ()
    published_at: str | None = None
    source_updated_at: str | None = None
    retrieved_at: str = field(default_factory=utcnow)
    source_metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def id(self) -> str:
        return str(uuid5(NAMESPACE_URL, f"{self.source}:{self.source_id}"))

    @property
    def fingerprint(self) -> str:
        data = asdict(self)
        data.pop("retrieved_at")
        return digest(data)

    @property
    def text(self) -> str:
        return "\n\n".join(s.text for s in self.sections)

    def metadata(self) -> dict[str, Any]:
        data = asdict(self)
        data.pop("sections")
        return data


@dataclass(frozen=True)
class Chunk:
    ordinal: int
    text: str
    heading_path: tuple[str, ...]
    locator: str | None


@dataclass(frozen=True)
class SearchFilters:
    collection: str | None = None
    source: str | None = None
    document_type: str | None = None
    publisher: str | None = None
