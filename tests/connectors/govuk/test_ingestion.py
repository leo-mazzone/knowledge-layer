"""Organisation inventories, independent attachments and safe reconciliation."""

import base64
import hashlib
import importlib.util
import io
import shutil
import subprocess
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

try:
    import httpx
except ImportError:
    httpx = None
from test_knowledge import Client, Embedder, MemoryRepository, Response, api_document

from knowledge.connectors.govuk import GovUkConnector, GovUkExtractor
from knowledge.connectors.govuk.html import html_sections
from knowledge.connectors.govuk.pdf import DEFAULT_LIMITS, pdf_payload
from knowledge.models import FetchedContent, SourceItem
from knowledge.pipeline import IngestionPipeline
from knowledge.processing import SectionChunker
from knowledge.storage.postgres import citation_url

ORG_CONFIG = {"organisations": ["department-for-business-and-trade"]}
FULL_CONFIG = {**ORG_CONFIG, "attachments": ["html", "pdf"], "reconcile": True}


class ReconcileRepository(MemoryRepository):
    @contextmanager
    def collection_lock(self, collection):
        self.locked = True
        try:
            yield
        finally:
            self.locked = False

    def reconcile_collection(self, collection, source, keep_ids, run_id, summary):
        assert self.locked
        ids = [key[0] for key in self.current if key[1] == collection and key[0] not in keep_ids]
        self.current = {
            key: value
            for key, value in self.current.items()
            if key[0] not in ids or key[1] != collection
        }
        summary["retirement"] = {"performed": True, "count": len(ids), "ids": ids}
        self.finish_run(run_id, summary)


class TestOrganisation:
    @pytest.fixture(autouse=True)
    def setup(self, request):
        self.request = request
        sleeper = patch("knowledge.connectors.govuk.http.time.sleep")
        sleeper.start()
        self.request.addfinalizer(sleeper.stop)

    def test_more_than_1000_and_exact_page_boundary(self):
        responses = [
            Response(
                {
                    "total": 1200,
                    "results": [{"link": f"/page/{i}"} for i in range(start, start + 100)],
                }
            )
            for start in range(0, 1200, 100)
        ]
        connector = GovUkConnector(Client(responses * 2))
        assert len(list(connector.discover(ORG_CONFIG))) == 1200
        assert connector.verify_discovery()
        assert connector.discovery_report["examined"] == 1200
        assert len(connector.client.calls) == 24
        assert connector.client.calls[0][1]["params"]["q"] == ""
        assert connector.client.calls[0][1]["params"]["order"] == "title"

    @pytest.mark.parametrize(
        "responses",
        [
            [Response({"total": 2, "results": [{"link": "/a"}, {"link": "/a"}]})],
            [Response({"total": 2, "results": [{"link": "/a"}]})],
            [
                Response({"total": 101, "results": [{"link": f"/a/{i}"} for i in range(100)]}),
                Response({"total": 102, "results": []}),
            ],
            [Response({}, status=503)],
        ],
    )
    def test_discovery_rejects_duplicates_premature_exhaustion_and_changing_totals(self, responses):
        connector = GovUkConnector(Client(responses))
        with pytest.raises((ValueError, RuntimeError)):
            list(connector.discover(ORG_CONFIG))
        assert not connector.discovery_report["complete"]

    def test_second_pass_requires_same_paths_not_just_count(self):
        connector = GovUkConnector(
            Client(
                [
                    Response({"total": 1, "results": [{"link": "/a"}]}),
                    Response({"total": 1, "results": [{"link": "/b"}]}),
                ]
            )
        )
        list(connector.discover(ORG_CONFIG))
        with pytest.raises(ValueError, match="changed between"):
            connector.verify_discovery()
        assert not connector.discovery_report["complete"]

    @pytest.mark.parametrize(
        "config",
        [
            {"urls": ["/a"], "reconcile": True},
            {"organisations": []},
            {"organisations": ["../evil"]},
            {**ORG_CONFIG, "search": {"query": "x"}},
            {**ORG_CONFIG, "pdf": {"max_bytes": -1}},
            {**ORG_CONFIG, "attachments": ["word"]},
        ],
    )
    def test_invalid_config_cannot_start_reconciliation(self, config):
        with pytest.raises(ValueError):
            list(GovUkConnector(Client([])).discover(config))


class TestAttachment:
    @pytest.fixture(autouse=True)
    def setup(self, request):
        self.request = request
        sleeper = patch("knowledge.connectors.govuk.http.time.sleep")
        sleeper.start()
        self.request.addfinalizer(sleeper.stop)

    def connector(self, handler=lambda request: httpx.Response(200, content=b"%PDF-demo")):
        if httpx is None:
            pytest.skip("Install httpx for HTTP transport checks")
        self.client = httpx.Client(transport=httpx.MockTransport(handler))
        self.request.addfinalizer(self.client.close)
        connector = GovUkConnector(self.client)
        # Bounded manifest: initializes options without performing a search.
        list(connector.discover({"urls": ["/a"], "attachments": ["html", "pdf"]}))
        return connector

    def test_structured_and_legacy_links_deduplicate_and_exclude_office_files(self):
        connector = self.connector()
        data = api_document(
            details={
                "attachments": [
                    {
                        "attachment_type": "file",
                        "content_type": "application/pdf",
                        "url": "https://assets.publishing.service.gov.uk/a.pdf",
                        "id": "a",
                    },
                    {
                        "attachment_type": "html",
                        "url": "https://www.gov.uk/government/publications/demo/html",
                    },
                    {
                        "attachment_type": "file",
                        "url": "https://assets.publishing.service.gov.uk/a.odt",
                    },
                ],
                "documents": [
                    '<a href="https://assets.publishing.service.gov.uk/a.pdf">Duplicate</a><a href="/government/publications/demo/legacy">Legacy HTML</a>'
                ],
            }
        )
        items = list(connector.expand(FetchedContent("/a", data)))
        assert [item.metadata["kind"] for item in items] == ["pdf", "html", "html"]
        assert items[-1].reference == "/government/publications/demo/legacy"
        assert len(connector.exclusions) == 1
        assert connector.exclusions[0]["reason"] == "unsupported"

    def test_streaming_size_limit_and_redirect_origin(self):
        connector = self.connector()
        connector.limits["max_bytes"] = 5
        with pytest.raises(ValueError, match="size limit"):
            connector.fetch(
                SourceItem("https://assets.publishing.service.gov.uk/a.pdf", {"kind": "pdf"})
            )
        connector = self.connector(
            lambda request: httpx.Response(302, headers={"location": "https://evil.example/a.pdf"})
        )
        with pytest.raises(ValueError, match="origin or redirect"):
            connector.fetch(SourceItem("https://www.gov.uk/a.pdf", {"kind": "pdf"}))

    @pytest.mark.parametrize(
        "handler,message",
        [
            (lambda request: httpx.Response(200, content=b"<html>error</html>"), "not a PDF"),
            (lambda request: httpx.Response(302, headers={"location": "/a.pdf"}), "Too many"),
        ],
    )
    def test_non_pdf_and_excessive_redirects(self, handler, message):
        connector = self.connector(handler)
        with pytest.raises(ValueError, match=message):
            connector.fetch(SourceItem("https://www.gov.uk/a.pdf", {"kind": "pdf"}))

    def test_html_headings_tables_lists_and_unanchored_siblings(self):
        sections = html_sections(
            '<h2 id="one">One</h2><ul><li>First</li></ul><h3>Nested</h3><p>Detail</p><h2 id="two">Two</h2><table><tr><td>A</td><td>B</td></tr></table><h2>Three</h2><p>End</p><script>ignore</script>'
        )
        assert [s.heading_path for s in sections] == [
            ("One",),
            ("One", "Nested"),
            ("Two",),
            ("Three",),
        ]
        assert [s.locator for s in sections] == ["one", None, "two", None]
        assert "- First" in sections[0].text
        assert "A | B" in sections[2].text
        assert "ignore" not in sections[-1].text

    @pytest.mark.parametrize(
        "schema,details",
        [
            (
                "publication",
                {
                    "attachments": [
                        {
                            "title": "Report",
                            "url": "https://assets.publishing.service.gov.uk/report.pdf",
                        }
                    ]
                },
            ),
            ("document_collection", {"groups": [{"title": "Group", "documents": ["content-id"]}]}),
        ],
    )
    def test_attachment_only_publication_and_document_collection(self, schema, details):
        document = GovUkExtractor().extract(
            FetchedContent("/a", api_document(schema_name=schema, details=details))
        )
        assert document.sections

    def test_pdf_identity_content_change_and_citation(self):
        parent = {
            "source_id": "parent-id",
            "title": "Report",
            "url": "https://www.gov.uk/a",
            "publishers": ["DBT"],
            "published_at": None,
            "source_updated_at": None,
            "locale": "en",
        }
        payload = {
            "representation": "pdf",
            "sha256": "first",
            "attachment": {
                "parent": parent,
                "attachment": {
                    "id": "stable",
                    "url": "https://assets.publishing.service.gov.uk/a.pdf",
                },
            },
            "pages": [
                {"number": 1, "text": "Native text", "method": "text"},
                {"number": 2, "text": "Scanned text", "method": "ocr"},
            ],
        }
        extract = GovUkExtractor().extract
        a = extract(FetchedContent("a.pdf", payload))
        b = extract(FetchedContent("a.pdf", {**payload, "sha256": "second"}))
        assert a.id == b.id
        assert a.fingerprint != b.fingerprint
        assert a.publishers == ("DBT",)
        assert citation_url(a.canonical_url, a.sections[1].locator) == a.canonical_url + "#page=2"


class TestRetirement:
    @pytest.fixture(autouse=True)
    def setup(self, request):
        self.request = request
        sleeper = patch("knowledge.connectors.govuk.http.time.sleep")
        sleeper.start()
        self.request.addfinalizer(sleeper.stop)
        self.repository = ReconcileRepository()
        self.pipeline = IngestionPipeline(self.repository, Embedder(), SectionChunker())
        self.old = GovUkExtractor().extract(FetchedContent("/old", api_document(content_id="old")))
        self.repository.current[(self.old.id, "dbt")] = "old"
        self.repository.current[(self.old.id, "other")] = "old"

    def run_ingestion(self, responses):
        return self.pipeline.run(
            "govuk",
            GovUkConnector(Client(responses)),
            GovUkExtractor(),
            GovUkExtractor.signature,
            FULL_CONFIG,
            "dbt",
        )

    def test_clean_run_retires_missing_only_in_target_collection(self):
        inventory = Response({"total": 1, "results": [{"link": "/a"}]})
        result = self.run_ingestion([inventory, Response(api_document()), inventory])
        assert result["coverage"] == "complete"
        assert result["retirement"]["ids"] == [self.old.id]
        assert (self.old.id, "dbt") not in self.repository.current
        assert (self.old.id, "other") in self.repository.current

    def test_empty_verified_inventory_retires_previous(self):
        result = self.run_ingestion([Response({"total": 0, "results": []})] * 2)
        assert result["retirement"]["performed"]
        assert result["retirement"]["count"] == 1

    @pytest.mark.parametrize(
        "middle,last",
        [
            (Response({}, status=500), Response({"total": 1, "results": [{"link": "/a"}]})),
            (Response(api_document()), Response({"total": 1, "results": [{"link": "/b"}]})),
        ],
    )
    def test_page_and_verification_failures_block_retirement(self, middle, last):
        inventory = Response({"total": 1, "results": [{"link": "/a"}]})
        result = self.run_ingestion([inventory, middle, last])
        assert result["status"] == "failed"
        assert not result["retirement"]["performed"]
        assert (self.old.id, "dbt") in self.repository.current

    def test_failed_child_does_not_block_parent_update_but_blocks_retirement(self):
        inventory = Response({"total": 1, "results": [{"link": "/a"}]})
        data = api_document(
            details={
                "body": "Parent text",
                "attachments": [
                    {
                        "attachment_type": "html",
                        "url": "https://www.gov.uk/government/publications/a/html",
                    }
                ],
            }
        )
        result = self.run_ingestion(
            [inventory, Response(data), Response({}, status=404), inventory]
        )
        assert result["published"] == 1
        assert result["discovered"] == 2
        assert not result["retirement"]["performed"]

    def test_cycle_unchanged_parent_still_expands_and_exclusions_are_reported(self):
        inventory = Response({"total": 1, "results": [{"link": "/a"}]})
        data = api_document(
            base_path="/a",
            details={
                "body": "Parent text",
                "attachments": [
                    {"attachment_type": "html", "url": "https://www.gov.uk/a"},
                    {
                        "attachment_type": "file",
                        "url": "https://assets.publishing.service.gov.uk/a.csv",
                    },
                ],
            },
        )
        first = self.run_ingestion([inventory, Response(data), inventory])
        second = self.run_ingestion([inventory, Response(data), inventory])
        assert first["discovered"] == 1
        assert second["unchanged"] == 1
        assert len(second["exclusions"]) == 1
        assert second["coverage"] == "partial"
        assert second["retirement"]["performed"]

    @pytest.mark.skipif(not (httpx), reason="Install httpx")
    def test_unchanged_parent_refreshes_pdf_and_removed_attachment_is_retired(self):
        state = {"pdf": b"%PDF-first", "attachments": True}

        def handler(request):
            if request.url.path == "/api/search.json":
                return httpx.Response(200, json={"total": 1, "results": [{"link": "/a"}]})
            if request.url.path == "/api/content/a":
                attachments = (
                    [
                        {
                            "attachment_type": "file",
                            "id": "stable-pdf",
                            "url": "https://assets.publishing.service.gov.uk/a.pdf",
                        }
                    ]
                    if state["attachments"]
                    else []
                )
                return httpx.Response(
                    200,
                    json=api_document(
                        base_path="/a", details={"body": "Parent", "attachments": attachments}
                    ),
                )
            return httpx.Response(200, content=state["pdf"])

        def parse_pdf(data, metadata, limits):
            return {
                "representation": "pdf",
                "attachment": metadata,
                "sha256": hashlib.sha256(data).hexdigest(),
                "pages": [{"number": 1, "method": "text", "text": data.decode()}],
            }

        with (
            httpx.Client(transport=httpx.MockTransport(handler)) as client,
            patch("knowledge.connectors.govuk.connector.pdf_payload", side_effect=parse_pdf),
        ):
            connector = GovUkConnector(client)
            run = lambda: self.pipeline.run(
                "govuk", connector, GovUkExtractor(), GovUkExtractor.signature, FULL_CONFIG, "dbt"
            )
            first = run()
            pdf_id = next(
                version[0].id
                for version in self.repository.versions
                if version[0].document_type == "pdf"
            )
            state["pdf"] = b"%PDF-updated-at-same-url"
            second = run()
            assert first["pdfs"] == 1
            assert second["unchanged"] == 1
            assert second["published"] == 1
            state["attachments"] = False
            third = run()
            assert third["retirement"]["ids"] == [pdf_id]
            assert third["retirement"]["count"] == 1

    def test_embedding_and_reconciliation_failures_block_retirement(self):
        inventory = Response({"total": 1, "results": [{"link": "/a"}]})
        self.pipeline.embedder.fail = True
        result = self.run_ingestion([inventory, Response(api_document()), inventory])
        assert not result["retirement"]["performed"]
        assert (self.old.id, "dbt") in self.repository.current
        self.pipeline.embedder.fail = False
        with patch.object(
            self.repository, "reconcile_collection", side_effect=RuntimeError("Audit failure")
        ):
            result = self.run_ingestion([inventory, Response(api_document()), inventory])
        assert result["status"] == "failed"
        assert not result["retirement"]["performed"]
        assert (self.old.id, "dbt") in self.repository.current


class TestPDF:
    def native_pdf(self):
        text = b"BT /F1 14 Tf 50 700 Td (Business and Trade export evidence and customs records for goods.) Tj ET"
        objects = [
            b"<< /Type /Catalog /Pages 2 0 R >>",
            b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 600 800] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
            b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
            b"<< /Length " + str(len(text)).encode() + b" >>\nstream\n" + text + b"\nendstream",
        ]
        data = bytearray(b"%PDF-1.4\n")
        offsets = [0]
        for number, obj in enumerate(objects, 1):
            offsets.append(len(data))
            data.extend(f"{number} 0 obj\n".encode() + obj + b"\nendobj\n")
        xref = len(data)
        data.extend(b"xref\n0 6\n0000000000 65535 f \n")
        for offset in offsets[1:]:
            data.extend(f"{offset:010} 00000 n \n".encode())
        data.extend(f"trailer\n<< /Size 6 /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode())
        return bytes(data)

    def fake_pdf(self, texts, size=(600, 800)):
        pdf = MagicMock()
        pdf.__enter__.return_value = pdf
        pdf.__len__.return_value = len(texts)
        pages = []
        for text in texts:
            page = MagicMock()
            page.__enter__.return_value = page
            page.get_size.return_value = size
            textpage = MagicMock()
            textpage.__enter__.return_value = textpage
            textpage.get_text_bounded.return_value = text
            page.get_textpage.return_value = textpage
            pages.append(page)
        pdf.__getitem__.side_effect = pages
        return SimpleNamespace(PdfDocument=MagicMock(return_value=pdf))

    def test_native_and_mixed_pdf_ocr_fallback(self):
        module = self.fake_pdf(["Readable native content " * 10, ""])
        with (
            patch.dict("sys.modules", {"pypdfium2": module}),
            patch(
                "knowledge.connectors.govuk.pdf.subprocess.run",
                return_value=SimpleNamespace(stdout=b"Scanned document text"),
            ) as ocr,
        ):
            result = pdf_payload(b"%PDF-test", {}, DEFAULT_LIMITS)
        assert [p["method"] for p in result["pages"]] == ["text", "ocr"]
        assert result["pages"][1]["text"] == "Scanned document text"
        assert base64.b64decode(result["bytes_base64"]) == b"%PDF-test"
        assert ocr.call_count == 1
        assert ocr.call_args.kwargs["timeout"] == 120

    @pytest.mark.parametrize(
        "texts,limits,response,message",
        [
            ([""], {**DEFAULT_LIMITS, "max_pages": 0}, None, "page limit"),
            ([""], {**DEFAULT_LIMITS, "max_pixels": 1}, None, "pixel limit"),
            ([""], DEFAULT_LIMITS, SimpleNamespace(stdout=b""), "no readable text"),
            ([""], DEFAULT_LIMITS, subprocess.TimeoutExpired("tesseract", 120), "timed out"),
            ([""], DEFAULT_LIMITS, FileNotFoundError(), "tesseract executable"),
        ],
    )
    def test_limits_blank_pdf_timeout_and_missing_tesseract(self, texts, limits, response, message):
        module = self.fake_pdf(texts)
        kwargs = (
            {"side_effect": response}
            if isinstance(response, Exception)
            else {"return_value": response}
        )
        with (
            patch.dict("sys.modules", {"pypdfium2": module}),
            patch("knowledge.connectors.govuk.pdf.subprocess.run", **kwargs),
            pytest.raises((RuntimeError, ValueError), match=message),
        ):
            pdf_payload(b"%PDF-test", {}, limits)

    @pytest.mark.skipif(
        not (importlib.util.find_spec("pypdfium2")), reason="Install PDF dependencies"
    )
    def test_real_native_pdf_and_malformed_file(self):
        import pypdfium2 as pdfium

        result = pdf_payload(self.native_pdf(), {}, DEFAULT_LIMITS)
        assert result["pages"][0]["method"] == "text"
        assert "export evidence" in result["pages"][0]["text"]
        with pytest.raises(pdfium.PdfiumError):
            pdf_payload(b"%PDF-invalid", {}, DEFAULT_LIMITS)

    @pytest.mark.skipif(
        not (
            importlib.util.find_spec("pypdfium2")
            and importlib.util.find_spec("PIL")
            and shutil.which("tesseract")
        ),
        reason="Install PDF dependencies and local Tesseract for the real OCR smoke test",
    )
    def test_real_scanned_pdf_ocr(self):
        from PIL import Image, ImageDraw, ImageFont

        image = Image.new("RGB", (1600, 600), "white")
        font = ImageFont.load_default(size=48)
        ImageDraw.Draw(image).text(
            (80, 100), "Business and Trade export evidence", font=font, fill="black"
        )
        data = io.BytesIO()
        image.save(data, format="PDF", resolution=150)
        image.close()
        result = pdf_payload(data.getvalue(), {}, DEFAULT_LIMITS)
        assert result["pages"][0]["method"] == "ocr"
        assert "export evidence" in result["pages"][0]["text"]
