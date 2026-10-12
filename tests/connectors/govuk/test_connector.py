"""GOV.UK discovery, extraction and content identity checks."""

from unittest.mock import patch

import pytest
from test_knowledge import Client, Response, api_document

from knowledge.connectors.govuk import GovUkConnector, GovUkExtractor, base_path
from knowledge.models import FetchedContent, SourceItem


class TestGovUk:
    @pytest.fixture(autouse=True)
    def setup(self, request):
        self.request = request
        sleeper = patch("knowledge.connectors.govuk.http.time.sleep")
        sleeper.start()
        self.request.addfinalizer(sleeper.stop)

    def test_both_apis_pagination_deduplication_and_filters(self):
        client = Client(
            [
                Response({"results": [{"link": "/eori"}, {"link": "/guidance/a"}]}),
                Response({"results": [{"link": "/guidance/b"}]}),
                Response(api_document()),
            ]
        )
        connector = GovUkConnector(client)
        items = list(
            connector.discover(
                {
                    "urls": ["https://www.gov.uk/eori"],
                    "search": {
                        "query": "exporting goods",
                        "page_size": 2,
                        "max_results": 4,
                        "filters": {"filter_organisations": ["hm-revenue-customs"]},
                    },
                }
            )
        )
        assert [i.reference for i in items] == ["/eori", "/guidance/a", "/guidance/b"]
        connector.fetch(items[1])
        assert client.calls[0][0] == "https://www.gov.uk/api/search.json"
        assert client.calls[1][1]["params"]["start"] == 2
        assert client.calls[2][0] == "https://www.gov.uk/api/content/guidance/a"

    def test_api_html_extracted_with_heading_and_anchor(self):
        payload = api_document()
        document = GovUkExtractor().extract(FetchedContent("/demo", payload))
        assert "Keep evidence." in document.sections[0].text
        assert "<p>" not in document.sections[0].text
        assert document.sections[0].heading_path == ("Declarations",)
        assert document.sections[0].locator == "declarations"
        assert document.publishers == ("HM Revenue & Customs",)
        assert document.source_id == payload["content_id"]
        assert document.source_updated_at == payload["public_updated_at"]

    def test_parts_and_step_by_step_structures(self):
        guide = api_document(
            details={
                "parts": [
                    {
                        "title": "Who needs an EORI",
                        "slug": "who-needs",
                        "body": "<p>You may need an EORI.</p>",
                    }
                ]
            }
        )
        document = GovUkExtractor().extract(FetchedContent("/demo", guide))
        assert document.sections[0].locator == "part:who-needs"
        step = api_document(
            details={
                "steps": [
                    {
                        "title": "Register",
                        "contents": [
                            {"type": "paragraph", "contents": "Get ready"},
                            {
                                "type": "list",
                                "contents": [{"text": "Get an EORI", "href": "/eori"}],
                            },
                        ],
                    }
                ]
            }
        )
        document = GovUkExtractor().extract(FetchedContent("/demo", step))
        assert document.sections[1].text == "Get an EORI (/eori)"

    def test_empty_unknown_format_not_silently_indexed(self):
        with pytest.raises(ValueError, match="Unsupported"):
            GovUkExtractor().extract(FetchedContent("/demo", api_document(details={})))

    @pytest.mark.parametrize(
        "url", ["https://example.com/a", "http://www.gov.uk/a", "//evil/a", "/a/../b", "/a?q=b"]
    )
    def test_off_origin_urls_and_redirects_rejected(self, url):
        with pytest.raises(ValueError):
            base_path(url)
        connector = GovUkConnector(Client([Response({}, 303, {"location": "https://evil.test/a"})]))
        with pytest.raises(ValueError, match="redirect"):
            connector.fetch(SourceItem("/eori"))

    def test_path_change_preserves_content_identity(self):
        extract = GovUkExtractor().extract
        a = extract(FetchedContent("/a", api_document(base_path="/a")))
        b = extract(FetchedContent("/b", api_document(base_path="/b")))
        assert a.id == b.id
        assert a.fingerprint != b.fingerprint
