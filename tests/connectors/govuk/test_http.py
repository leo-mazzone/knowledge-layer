"""Deterministic clocks verify request pacing, cooldowns and run-wide aborts."""

from contextlib import contextmanager
from datetime import UTC, datetime
from email.utils import format_datetime
from unittest.mock import patch

import pytest
from test_knowledge import Embedder, Response, api_document

from connectors.govuk.test_ingestion import FULL_CONFIG, ReconcileRepository
from knowledge.connectors.govuk import GovUkConnector, GovUkExtractor
from knowledge.connectors.govuk.http import GovUkRequests
from knowledge.errors import SourceRequestsStopped
from knowledge.models import SourceItem
from knowledge.pipeline import IngestionPipeline
from knowledge.processing import SectionChunker


class Clock:
    def __init__(self):
        self.value = 0.0
        self.pauses = []

    def monotonic(self):
        return self.value

    def wallclock(self):
        return 1_700_000_000.0 + self.value

    def sleep(self, seconds):
        self.pauses.append(seconds)
        self.value += seconds


class TrackingResponse(Response):
    def __init__(self, data=None, status=200, headers=None):
        super().__init__(data or {}, status, headers)
        self.closed = False

    def close(self):
        self.closed = True

    def iter_bytes(self):
        yield b"%PDF-test"


class Client:
    def __init__(self, responses, clock):
        self.responses = iter(responses)
        self.clock = clock
        self.calls = []
        self.last = None

    def get(self, url, **kwargs):
        if self.last is not None and self.last.status_code in (429, 503):
            assert self.last.closed, "Rejected response must close before another request"
        self.calls.append((url, self.clock.value, kwargs))
        self.last = next(self.responses)
        return self.last

    @contextmanager
    def stream(self, method, url, **kwargs):
        response = self.get(url, **kwargs)
        try:
            yield response
        finally:
            response.close()


class TestRequest:
    def requests(self, responses, policy=None):
        self.clock = Clock()
        self.client = Client(responses, self.clock)
        requests = GovUkRequests(
            self.client,
            clock=self.clock.monotonic,
            sleep=self.clock.sleep,
            wallclock=self.clock.wallclock,
            jitter=lambda low, high: high,
        )
        requests.configure(policy or {})
        return requests

    def test_pacing_across_search_content_streams_and_request_durations(self):
        requests = self.requests([TrackingResponse()] * 5)
        requests.get("https://www.gov.uk/api/search.json")
        requests.get("https://www.gov.uk/api/content/a")
        with requests.stream("https://assets.publishing.service.gov.uk/a.pdf"):
            self.clock.value += 0.25
        requests.get("https://www.gov.uk/api/content/b")
        self.clock.value += 5
        requests.get("https://www.gov.uk/api/content/c")
        assert [call[1] for call in self.client.calls] == [0, 1, 2, 3, 8]
        assert self.clock.pauses == [1, 1, 0.75]
        assert all(call[2]["follow_redirects"] is False for call in self.client.calls)

    def test_retry_after_seconds_on_429_and_date_on_503(self):
        date = format_datetime(datetime.fromtimestamp(1_700_000_017, UTC), usegmt=True)
        requests = self.requests(
            [
                TrackingResponse(status=429, headers={"Retry-After": "10"}),
                TrackingResponse(status=503, headers={"Retry-After": date}),
                TrackingResponse(),
            ]
        )
        requests.get("https://www.gov.uk/api/content/a")
        assert [call[1] for call in self.client.calls] == [0, 10, 17]
        assert requests.report["retries"] == 2
        assert requests.report["rate_limited"] == 1
        assert requests.report["service_unavailable"] == 1

    def test_malformed_retry_after_uses_exponential_backoff_and_jitter(self):
        requests = self.requests(
            [
                TrackingResponse(status=429, headers={"Retry-After": "not-a-date"}),
                TrackingResponse(status=503, headers={"Retry-After": "-1"}),
                TrackingResponse(),
            ]
        )
        requests.get("a")
        assert self.clock.pauses == [2.5, 5]
        assert [call[1] for call in self.client.calls] == [0, 2.5, 7.5]

    @pytest.mark.parametrize("header", ("0", "Tue, 14 Nov 2023 00:00:00 GMT"))
    def test_expired_or_zero_retry_after_still_respects_pacing(self, header):
        requests = self.requests(
            [
                TrackingResponse(status=429, headers={"Retry-After": header}),
                TrackingResponse(),
            ]
        )
        requests.get("a")
        assert self.clock.pauses == [1]

    def test_stream_retries_close_response_before_waiting_and_respect_headers(self):
        rejected = TrackingResponse(status=503, headers={"Retry-After": "130"})
        requests = self.requests([rejected, TrackingResponse()])
        with requests.stream("https://assets.publishing.service.gov.uk/a.pdf") as response:
            assert b"".join(response.iter_bytes()) == b"%PDF-test"
        assert rejected.closed
        assert self.clock.pauses == [60, 60, 10]
        assert self.client.calls[-1][1] == 130

    def test_exhaustion_stops_all_following_requests_and_closes_last_response(self):
        responses = [TrackingResponse(status=429)] * 3
        requests = self.requests(responses, {"max_retries": 2})
        with pytest.raises(SourceRequestsStopped, match="3 attempts"):
            requests.get("a")
        assert responses[-1].closed
        with pytest.raises(SourceRequestsStopped):
            requests.get("b")
        with pytest.raises(SourceRequestsStopped), requests.stream("c"):
            pass
        assert len(self.client.calls) == 3
        assert requests.report["stopped"]

    def test_long_retry_after_stops_instead_of_shortening_server_cooldown(self):
        requests = self.requests([TrackingResponse(status=429, headers={"Retry-After": "3600"})])
        with pytest.raises(SourceRequestsStopped, match="wait before restarting"):
            requests.get("a")
        assert len(self.client.calls) == 1
        assert self.clock.pauses == []
        assert requests.next_request_at == 3600

    def test_404_is_not_retried(self):
        requests = self.requests([TrackingResponse(status=404)])
        assert requests.get("a").status_code == 404
        assert requests.report["retries"] == 0
        assert len(self.client.calls) == 1

    @pytest.mark.parametrize(
        "options",
        [
            {"min_interval": 0},
            {"min_interval": float("nan")},
            {"min_interval": True},
            {"max_retries": -1},
            {"max_retries": 1.5},
            {"max_backoff": 1},
            {"unknown": 3},
            [],
        ],
    )
    def test_invalid_policy_is_rejected(self, options):
        requests = self.requests([])
        with pytest.raises(ValueError):
            requests.configure(options)

    def test_content_and_pdf_redirect_hops_use_same_pacing_policy(self):
        requests = self.requests(
            [
                TrackingResponse(status=302, headers={"location": "/api/content/b"}),
                TrackingResponse(api_document()),
                TrackingResponse(
                    status=302,
                    headers={"location": "https://assets.publishing.service.gov.uk/b.pdf"},
                ),
                TrackingResponse(),
            ]
        )
        connector = GovUkConnector(self.client)
        connector.requests = requests
        connector.limits = {"max_bytes": 100}
        connector.fetch(SourceItem("/a"))
        with patch("knowledge.connectors.govuk.connector.pdf_payload", return_value={"pages": []}):
            connector.fetch(SourceItem("https://www.gov.uk/a.pdf", {"kind": "pdf"}))
        assert [call[1] for call in self.client.calls] == [0, 1, 2, 3]

    def test_pipeline_stops_after_throttling_without_next_item_or_verification(self):
        requests = self.requests(
            [
                TrackingResponse({"total": 2, "results": [{"link": "/a"}, {"link": "/b"}]}),
                TrackingResponse(api_document()),
                TrackingResponse(status=429),
                TrackingResponse(status=429),
            ]
        )
        connector = GovUkConnector(self.client)
        connector.requests = requests
        repository = ReconcileRepository()
        repository.current[("old-document", "dbt")] = "previous"
        result = IngestionPipeline(repository, Embedder(), SectionChunker()).run(
            "govuk",
            connector,
            GovUkExtractor(),
            GovUkExtractor.signature,
            {**FULL_CONFIG, "http": {"max_retries": 1}},
            "dbt",
        )
        assert result["aborted"]
        assert result["published"] == 1
        assert result["status"] == "failed"
        assert result["coverage"] == "partial"
        assert not result["retirement"]["performed"]
        assert ("old-document", "dbt") in repository.current
        assert result["http"]["stopped"]
        assert len(self.client.calls) == 4
        assert sum("search.json" in call[0] for call in self.client.calls) == 1

    def test_discovery_throttling_aborts_before_ingesting_any_document(self):
        requests = self.requests([TrackingResponse(status=429)])
        connector = GovUkConnector(self.client)
        connector.requests = requests
        repository = ReconcileRepository()
        result = IngestionPipeline(repository, Embedder(), SectionChunker()).run(
            "govuk",
            connector,
            GovUkExtractor(),
            GovUkExtractor.signature,
            {**FULL_CONFIG, "http": {"max_retries": 0}},
            "dbt",
        )
        assert result["aborted"]
        assert result["published"] == 0
        assert not result["discovery"]["complete"]
        assert not result["retirement"]["performed"]
        assert len(self.client.calls) == 1

    def test_busy_source_is_rejected_before_discovery_or_starting_run(self):
        requests = self.requests([])
        connector = GovUkConnector(self.client)
        connector.requests = requests
        repository = ReconcileRepository()
        with (
            patch.object(repository, "source_lock", side_effect=RuntimeError("already running")),
            pytest.raises(RuntimeError, match="already running"),
        ):
            IngestionPipeline(repository, Embedder(), SectionChunker()).run(
                "govuk", connector, GovUkExtractor(), GovUkExtractor.signature, FULL_CONFIG, "dbt"
            )
        assert self.client.calls == []
        assert repository.runs == {}
