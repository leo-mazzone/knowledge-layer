"""Embedding purpose, configuration, and version compatibility regressions."""

import os
from unittest.mock import MagicMock, patch

import pytest
from test_knowledge import Client, GovUkExtractor, MemoryRepository, MutableConnector, Response

from knowledge.config import Settings, build_components
from knowledge.embedding_profiles import EmbeddingProfile, resolve_profile
from knowledge.embeddings import CompatibleEmbedder
from knowledge.models import SearchFilters
from knowledge.pipeline import IngestionPipeline
from knowledge.processing import SectionChunker
from knowledge.retrieval import KnowledgeService


def response():
    return Response({"data": [{"index": 0, "embedding": [1, 0, 0]}]})


class TestProfile:
    @pytest.mark.parametrize(
        "profile,document,query",
        (
            (resolve_profile("nomic"), "search_document: text", "search_query: text"),
            (resolve_profile("plain"), "text", "text"),
            (resolve_profile("custom", "  passage: ", ""), "  passage: text", "text"),
        ),
    )
    def test_purpose_inputs_and_plain_transport(self, profile, document, query):
        client = Client([response(), response(), response()])
        embedder = CompatibleEmbedder(client, "http://test/v1", "demo", 3, profile=profile)
        assert embedder.embed_documents(["text"]) == [[1, 0, 0]]
        assert embedder.embed_query("text") == [1, 0, 0]
        embedder.embed(["raw"])
        assert [call[1]["json"]["input"] for call in client.calls] == [
            [document],
            [query],
            ["raw"],
        ]

    @pytest.mark.parametrize(
        "result,message",
        (
            (Response({}, status=503), "HTTP 503"),
            (Response({"data": [{"index": 0, "embedding": [1]}]}), "dimension"),
            (Response({"data": [{"index": 1, "embedding": [1, 0, 0]}]}), "indices"),
        ),
    )
    def test_prefixed_batches_preserve_order_and_validation(self, result, message):
        client = Client(
            [
                Response(
                    {
                        "data": [
                            {"index": 1, "embedding": [0, 1, 0]},
                            {"index": 0, "embedding": [1, 0, 0]},
                        ]
                    }
                ),
                response(),
            ]
        )
        embedder = CompatibleEmbedder(
            client, "http://test/v1", "demo", 3, batch_size=2, profile=resolve_profile("nomic")
        )
        assert embedder.embed_documents(["a", "b", "c"]) == [[1, 0, 0], [0, 1, 0], [1, 0, 0]]
        assert client.calls[1][1]["json"]["input"] == ["search_document: c"]
        bad = CompatibleEmbedder(
            Client([result]), "http://test/v1", "demo", 3, profile=resolve_profile("nomic")
        )
        with pytest.raises((RuntimeError, ValueError), match=message):
            bad.embed_query("text")

    def test_signatures_capture_both_prefixes_and_preserve_plain(self):
        def signature(profile):
            return CompatibleEmbedder(None, "http://test/v1/", "demo", 3, profile=profile).signature

        assert signature(resolve_profile("plain")) == "compatible-v1:http://test/v1:demo:3"
        assert signature(resolve_profile("plain")) == signature(resolve_profile("custom", "", ""))
        nomic = resolve_profile("nomic")
        assert signature(nomic) == signature(
            resolve_profile("custom", "search_document: ", "search_query: ")
        )
        assert (
            len(
                {
                    signature(nomic),
                    signature(EmbeddingProfile("other", nomic.query_prefix)),
                    signature(EmbeddingProfile(nomic.document_prefix, "other")),
                }
            )
            == 3
        )

    @pytest.mark.parametrize(
        "environment",
        (
            {"EMBEDDING_PROFILE": "unknown"},
            {"EMBEDDING_PROFILE": "custom"},
            {"EMBEDDING_PROFILE": "custom", "EMBEDDING_DOCUMENT_PREFIX": ""},
            {"EMBEDDING_PROFILE": "custom", "EMBEDDING_QUERY_PREFIX": ""},
        ),
    )
    def test_configuration_defaults_custom_empty_and_errors(self, environment):
        with patch.dict(os.environ, {}, clear=True):
            assert Settings.from_env().embedding_profile == resolve_profile("nomic")
        with patch.dict(
            os.environ,
            {
                "EMBEDDING_PROFILE": "custom",
                "EMBEDDING_DOCUMENT_PREFIX": " doc: ",
                "EMBEDDING_QUERY_PREFIX": "",
            },
            clear=True,
        ):
            assert Settings.from_env().embedding_profile == EmbeddingProfile(" doc: ", "")
        with patch.dict(os.environ, environment, clear=True), pytest.raises(ValueError):
            Settings.from_env()

    def test_component_wiring(self):
        with patch.dict(os.environ, {}, clear=True):
            settings = Settings.from_env()
        with (
            patch("httpx.Client") as client,
            patch("knowledge.storage.postgres.PostgresRepository"),
        ):
            _, _, embedder = build_components(settings)
        assert embedder.client is client.return_value
        assert embedder.profile == resolve_profile("nomic")

    def test_ingestion_search_and_profile_change(self):
        client = Client([response(), response(), response()])
        repository = MemoryRepository()
        connector = MutableConnector()
        embedder = CompatibleEmbedder(client, "http://test/v1", "demo", 3)
        pipeline = IngestionPipeline(repository, embedder, SectionChunker())

        def ingest():
            return pipeline.run(
                "govuk", connector, GovUkExtractor(), GovUkExtractor.signature, {}, "exports"
            )

        assert ingest()["published"] == 1
        original = repository.versions[0]
        embedder.profile = resolve_profile("nomic")
        assert ingest()["published"] == 1
        assert ingest()["unchanged"] == 1
        assert repository.versions[1][0].title == original[0].title
        assert repository.versions[1][0].sections == original[0].sections
        assert repository.versions[1][1].payload == original[1].payload
        assert repository.versions[1][2] == original[2]
        assert client.calls[1][1]["json"]["input"][0].startswith("search_document: Export goods")
        assert not original[2][0].text.startswith("search_document:")

        search_repository = MagicMock()
        search_repository.search.return_value = [{"text": original[2][0].text}]
        result = KnowledgeService(search_repository, embedder).search(
            " export goods ", SearchFilters()
        )
        assert client.calls[2][1]["json"]["input"] == ["search_query: export goods"]
        assert search_repository.search.call_args.args[:3] == (
            "export goods",
            [1, 0, 0],
            embedder.signature,
        )
        assert result["results"][0]["text"] == original[2][0].text
