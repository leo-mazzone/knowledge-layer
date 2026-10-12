"""Offline behavioural checks; no network, database, or real embedding model needed."""

from contextlib import nullcontext
from dataclasses import replace
from uuid import uuid4

import pytest

from knowledge.connectors.govuk import GovUkExtractor
from knowledge.embeddings import CompatibleEmbedder
from knowledge.models import Document, FetchedContent, SearchFilters, Section, SourceItem
from knowledge.pipeline import IngestionPipeline
from knowledge.processing import SectionChunker, validate_vectors
from knowledge.registry import SourceRegistry
from knowledge.retrieval import KnowledgeService
from knowledge.storage.postgres import citation_url, fuse_rankings, public_metadata


def api_document(**updates):
    return {
        "content_id": "export-guidance-identity",
        "base_path": "/guidance/export-demo",
        "title": "Export goods",
        "document_type": "guidance",
        "locale": "en",
        "first_published_at": "2024-01-01T00:00:00Z",
        "public_updated_at": "2026-01-01T00:00:00Z",
        "details": {"body": '<h2 id="declarations">Declarations</h2><p>Keep evidence.</p>'},
        "links": {"organisations": [{"title": "HM Revenue & Customs"}]},
        **updates,
    }


class Response:
    def __init__(self, data, status=200, headers=None):
        self.data, self.status_code = data, status
        self.headers = headers or {}
        self.content = b"{}"

    def json(self):
        return self.data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def close(self):
        pass


class Client:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return next(self.responses)

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return next(self.responses)


class MemoryRepository:
    def __init__(self):
        self.current = {}
        self.versions = []
        self.runs = {}
        self.fail_publish = False

    def source_lock(self, source):
        return nullcontext()

    def start_run(self, connector, collection):
        run_id = str(uuid4())
        self.runs[run_id] = None
        return run_id

    def finish_run(self, run_id, summary):
        self.runs[run_id] = summary

    def current_fingerprint(self, document_id, collection):
        return self.current.get((document_id, collection))

    def publish(
        self, document, raw, collection, fingerprint, chunks, vectors, signature, processing=None
    ):
        if self.fail_publish:
            raise RuntimeError("Simulated storage failure")
        self.versions.append((document, raw, chunks, vectors))
        self.current[(document.id, collection)] = fingerprint
        return True


class Embedder:
    dimensions = 3
    signature = "test-model:3"

    def __init__(self):
        self.calls = 0
        self.fail = False

    def embed_documents(self, texts):
        self.calls += 1
        if self.fail:
            raise RuntimeError("Simulated embedding failure")
        return [[1.0, 0.0, 0.0] for _ in texts]

    def embed_query(self, text):
        return self.embed_documents([text])[0]


class MutableConnector:
    def __init__(self):
        self.payload = api_document()

    def discover(self, config):
        yield SourceItem("/guidance/export-demo")

    def fetch(self, item):
        return FetchedContent(item.reference, self.payload)


class TestPipeline:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.repository = MemoryRepository()
        self.embedder = Embedder()
        self.connector = MutableConnector()
        self.pipeline = IngestionPipeline(self.repository, self.embedder, SectionChunker())

    def run_ingest(self):
        return self.pipeline.run(
            "govuk", self.connector, GovUkExtractor(), GovUkExtractor.signature, {}, "exports"
        )

    def test_noop_does_not_embed_and_metadata_change_versions(self):
        assert self.run_ingest()["published"] == 1
        assert self.run_ingest()["unchanged"] == 1
        assert self.embedder.calls == 1
        self.connector.payload = api_document(public_updated_at="2026-02-01T00:00:00Z")
        assert self.run_ingest()["published"] == 1
        assert len(self.repository.versions) == 2

    def test_embedding_and_storage_failure_preserve_previous_version(self):
        self.run_ingest()
        previous = dict(self.repository.current)
        self.connector.payload = api_document(title="New edition")
        self.embedder.fail = True
        assert self.run_ingest()["status"] == "failed"
        assert self.repository.current == previous
        self.embedder.fail = False
        self.repository.fail_publish = True
        result = self.run_ingest()
        assert result["status"] == "failed"
        assert self.repository.current == previous
        assert self.repository.runs[result["run_id"]]["errors"]

    def test_reembedding_and_multiple_collections(self):
        self.run_ingest()
        self.embedder.signature = "new-model:3"
        assert self.run_ingest()["published"] == 1
        result = self.pipeline.run(
            "govuk",
            self.connector,
            GovUkExtractor(),
            GovUkExtractor.signature,
            {},
            "other-collection",
        )
        assert result["published"] == 1
        assert len(self.repository.current) == 2

    def test_empty_content_retains_previous(self):
        self.run_ingest()
        self.connector.payload = api_document(details={"body": ""})
        assert self.run_ingest()["status"] == "failed"
        assert len(self.repository.versions) == 1


class TestProcessing:
    def test_embedding_batches_reorder_response_and_validate(self):
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
                Response({"data": [{"index": 0, "embedding": [0, 0, 1]}]}),
            ]
        )
        embedder = CompatibleEmbedder(client, "http://localhost/v1", "demo", 3, batch_size=2)
        assert embedder.embed(["a", "b", "c"]) == [[1, 0, 0], [0, 1, 0], [0, 0, 1]]
        assert client.calls[0][0] == "http://localhost/v1/embeddings"
        assert client.calls[1][1]["json"]["input"] == ["c"]
        bad = CompatibleEmbedder(
            Client(
                [
                    Response(
                        {
                            "data": [
                                {"index": 1, "embedding": [1, 0, 0]},
                            ]
                        }
                    )
                ]
            ),
            "http://localhost/v1",
            "demo",
            3,
        )
        with pytest.raises(ValueError, match="indices"):
            bad.embed(["one"])

    def test_chunks_keep_structured_boundaries_overlap_and_length_caps(self):
        doc = Document(
            "test",
            "1",
            "https://example.test",
            "Title",
            (
                Section("one two three four five six", ("Part one",), "part:first"),
                Section("other content", ("Part two",)),
            ),
        )
        chunks = SectionChunker(max_words=4, overlap_words=1).chunk(doc)
        assert [c.text for c in chunks] == ["one two three four", "four five six", "other content"]
        assert chunks[1].locator == "part:first"
        assert [c.ordinal for c in chunks] == [0, 1, 2]
        long = replace(doc, sections=(Section("x" * 20000),))
        assert all(len(c.text) <= 6000 for c in SectionChunker().chunk(long))

    @pytest.mark.parametrize("vectors", [[], [[0, 0, 0]], [[1, 0]], [[float("nan"), 0, 1]]])
    def test_bad_embeddings_rejected(self, vectors):
        with pytest.raises(ValueError):
            validate_vectors(vectors, 1, 3)

    def test_fusion_prefers_agreement_and_deduplicates(self):
        a = {"version_id": "a", "ordinal": 0}
        b = {"version_id": "b", "ordinal": 0}
        hits = fuse_rankings([a, b], [b], 5)
        assert hits[0]["version_id"] == "b"
        assert hits[0]["matched_by"] == ["lexical", "semantic"]
        assert len(hits) == 2

    def test_source_registry(self):
        registry = SourceRegistry()
        registry.register("custom", lambda client: "adapter")
        assert registry.create("custom", None) == "adapter"
        with pytest.raises(ValueError):
            registry.register("govuk", lambda client: None)

    def test_citation_and_metadata_projection(self):
        assert (
            citation_url("https://www.gov.uk/eori", "part:apply") == "https://www.gov.uk/eori/apply"
        )
        result = public_metadata(
            {
                "title": "Test",
                "source_metadata": {
                    "links": {"large": "payload"},
                    "withdrawn_notice": {"explanation": "Withdrawn"},
                },
            }
        )
        assert "links" not in result["source_metadata"]
        assert "withdrawn_notice" in result["source_metadata"]

    @pytest.mark.parametrize("query,limit", [("", 6), ("x" * 2001, 6), ("exports", 11)])
    def test_service_rejects_unbounded_requests(self, query, limit):
        service = KnowledgeService(MemoryRepository(), Embedder())
        with pytest.raises(ValueError):
            service.search(query, SearchFilters(), limit)
        with pytest.raises(ValueError):
            service.read("not-a-uuid")
