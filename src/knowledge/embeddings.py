"""Embedding adapter for OpenAI-compatible APIs, including local Ollama."""

import logging
from collections.abc import Sequence

from knowledge.embedding_profiles import PLAIN_PROFILE, EmbeddingProfile
from knowledge.processing import validate_vectors

logger = logging.getLogger(__name__)


class CompatibleEmbedder:
    def __init__(
        self,
        client,
        base_url: str,
        model: str,
        dimensions: int,
        api_key: str = "",
        batch_size: int = 16,
        profile: EmbeddingProfile = PLAIN_PROFILE,
    ):
        if dimensions < 1 or batch_size < 1:
            raise ValueError("Embedding dimensions and batch size must be positive")
        self.client = client
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.dimensions = dimensions
        self.api_key = api_key
        self.batch_size = batch_size
        self.profile = profile

    @property
    def signature(self) -> str:
        return (
            f"compatible-v1:{self.base_url}:{self.model}:{self.dimensions}"
            + self.profile.signature_suffix
        )

    def embed_documents(self, texts: Sequence[str]) -> list[list[float]]:
        return self.embed(self.profile.documents(texts))

    def embed_query(self, text: str) -> list[float]:
        return self.embed([self.profile.query(text)])[0]

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        """Low-level transport; callers supply already prepared inputs."""
        vectors = []
        for start in range(0, len(texts), self.batch_size):
            batch = list(texts[start : start + self.batch_size])
            logger.log(
                logging.INFO if len(texts) > self.batch_size else logging.DEBUG,
                "Embedding batch=%d/%d passages=%d",
                start // self.batch_size + 1,
                (len(texts) + self.batch_size - 1) // self.batch_size,
                len(batch),
            )
            headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
            response = self.client.post(
                self.base_url + "/embeddings",
                headers=headers,
                json={"model": self.model, "input": batch},
            )
            response.raise_for_status()
            data = sorted(response.json()["data"], key=lambda item: item["index"])
            if [item["index"] for item in data] != list(range(len(batch))):
                raise ValueError("Embedding response has duplicate or missing indices")
            result = [item["embedding"] for item in data]
            validate_vectors(result, len(batch), self.dimensions)
            vectors.extend(result)
        return vectors
