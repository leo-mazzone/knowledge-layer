"""Model input conventions, independent of embedding transport and application use cases."""

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class EmbeddingProfile:
    document_prefix: str = ""
    query_prefix: str = ""

    def documents(self, texts: Sequence[str]) -> list[str]:
        return [self.document_prefix + text for text in texts]

    def query(self, text: str) -> str:
        return self.query_prefix + text

    @property
    def signature_suffix(self) -> str:
        if not self.document_prefix and not self.query_prefix:
            return ""
        encoded = json.dumps(
            [self.document_prefix, self.query_prefix], ensure_ascii=True, separators=(",", ":")
        ).encode("utf-8")
        return ":prefix-v1:" + hashlib.sha256(encoded).hexdigest()


PLAIN_PROFILE = EmbeddingProfile()
NOMIC_PROFILE = EmbeddingProfile("search_document: ", "search_query: ")


def resolve_profile(
    name: str, document_prefix: str | None = None, query_prefix: str | None = None
) -> EmbeddingProfile:
    if name == "nomic":
        return NOMIC_PROFILE
    if name == "plain":
        return PLAIN_PROFILE
    if name == "custom":
        if document_prefix is None or query_prefix is None:
            raise ValueError(
                "Custom embedding profile requires EMBEDDING_DOCUMENT_PREFIX and "
                "EMBEDDING_QUERY_PREFIX (explicitly empty values are allowed)"
            )
        return EmbeddingProfile(document_prefix, query_prefix)
    raise ValueError(f"Unknown embedding profile {name!r}; choose nomic, plain, or custom")
