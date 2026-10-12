import os
from dataclasses import dataclass

from knowledge.embedding_profiles import NOMIC_PROFILE, EmbeddingProfile, resolve_profile


@dataclass(frozen=True)
class Settings:
    database_url: str
    embedding_base_url: str
    embedding_model: str
    embedding_dimensions: int
    embedding_api_key: str
    host: str
    port: int
    embedding_profile: EmbeddingProfile = NOMIC_PROFILE

    @classmethod
    def from_env(cls):
        return cls(
            database_url=os.getenv(
                "DATABASE_URL", "postgresql://knowledge:knowledge@localhost:5433/knowledge"
            ),
            embedding_base_url=os.getenv("EMBEDDING_BASE_URL", "http://localhost:11434/v1"),
            embedding_model=os.getenv("EMBEDDING_MODEL", "nomic-embed-text"),
            embedding_dimensions=int(os.getenv("EMBEDDING_DIMENSIONS", "768")),
            embedding_api_key=os.getenv("EMBEDDING_API_KEY", ""),
            host=os.getenv("BIND_HOST", "127.0.0.1"),
            port=int(os.getenv("PORT", "8080")),
            embedding_profile=resolve_profile(
                os.getenv("EMBEDDING_PROFILE", "nomic"),
                os.getenv("EMBEDDING_DOCUMENT_PREFIX"),
                os.getenv("EMBEDDING_QUERY_PREFIX"),
            ),
        )


def build_components(settings: Settings):
    import httpx

    from knowledge.embeddings import CompatibleEmbedder
    from knowledge.storage.postgres import PostgresRepository

    client = httpx.Client(
        timeout=httpx.Timeout(120.0, connect=10.0),
        transport=httpx.HTTPTransport(retries=2),
        follow_redirects=False,
        headers={"User-Agent": "govchat-knowledge/0.1"},
    )
    repository = PostgresRepository(settings.database_url)
    embedder = CompatibleEmbedder(
        client,
        settings.embedding_base_url,
        settings.embedding_model,
        settings.embedding_dimensions,
        settings.embedding_api_key,
        profile=settings.embedding_profile,
    )
    return client, repository, embedder
