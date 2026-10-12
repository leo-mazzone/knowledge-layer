"""CLI output stays visible during ingestion without polluting the final JSON."""

import io
import json
import logging
import tempfile
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from connectors.govuk.test_http import Client, Clock, TrackingResponse
from test_knowledge import Embedder, MemoryRepository, MutableConnector

from knowledge.cli import main
from knowledge.connectors.govuk import GovUkConnector, GovUkExtractor
from knowledge.connectors.govuk.http import GovUkRequests


class TestLogging:
    def test_cli_reports_progress_to_stderr_and_final_json_to_stdout(self):
        stdout, stderr = io.StringIO(), io.StringIO()
        client = MagicMock()
        root = logging.getLogger()
        handlers, level = list(root.handlers), root.level
        httpx_logger = logging.getLogger("httpx")
        httpx_level = httpx_logger.level
        root.handlers = []
        adapter = SimpleNamespace(
            connector=MutableConnector(),
            extractor=GovUkExtractor(),
            extractor_signature=GovUkExtractor.signature,
        )
        try:
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "manifest.json"
                path.write_text(
                    json.dumps({"collection": "test", "connector": "govuk", "config": {}})
                )
                with (
                    redirect_stdout(stdout),
                    redirect_stderr(stderr),
                    patch("sys.argv", ["knowledge", "ingest", str(path)]),
                    patch(
                        "knowledge.cli.build_components",
                        return_value=(client, MemoryRepository(), Embedder()),
                    ),
                    patch("knowledge.cli.SourceRegistry.create", return_value=adapter),
                ):
                    main()
            assert json.loads(stdout.getvalue())["published"] == 1
            messages = stderr.getvalue()
            assert "INFO knowledge.cli Starting ingestion" in messages
            assert "Discovering documents" in messages
            assert "Fetching item=1" in messages
            assert "Embedding reference=" in messages
            assert "Item published" in messages
            assert "published=1 unchanged=0 errors=0" in messages
            assert "Ingestion finished" in messages
            assert "Keep evidence." not in messages
            client.close.assert_called_once()
        finally:
            for handler in root.handlers:
                handler.close()
            root.handlers = handlers
            root.setLevel(level)
            httpx_logger.setLevel(httpx_level)

    def test_discovery_and_verification_progress_have_distinct_phases(self, caplog):
        clock = Clock()
        inventory = {"total": 1, "results": [{"link": "/a"}]}
        client = Client([TrackingResponse(inventory), TrackingResponse(inventory)], clock)
        connector = GovUkConnector(client)
        connector.requests = GovUkRequests(client, clock=clock.monotonic, sleep=clock.sleep)
        with caplog.at_level(level="INFO", logger="knowledge.connectors.govuk"):
            list(
                connector.discover(
                    {
                        "organisations": ["department-for-business-and-trade"],
                        "http": {"min_interval": 0.1},
                    }
                )
            )
            connector.verify_discovery()
        messages = caplog.text
        assert "max_requests_per_second=10" in messages
        assert "Organisation discovery examined=1/1" in messages
        assert "Organisation verification examined=1/1" in messages
        assert "Organisation inventory verified" in messages

    def test_retry_wait_is_logged_before_sleeping(self, caplog):
        clock = Clock()
        client = Client(
            [TrackingResponse(status=429, headers={"Retry-After": "5"}), TrackingResponse()], clock
        )
        slept = []

        def sleep(seconds):
            assert "HTTP 429" in caplog.text
            slept.append(seconds)
            clock.sleep(seconds)

        requests = GovUkRequests(client, clock=clock.monotonic, sleep=sleep)
        with caplog.at_level(level="WARNING", logger="knowledge.connectors.govuk.http"):
            requests.get("https://www.gov.uk/api/content/a")
        assert slept == [5]
        assert "wait=5.0s reason=Retry-After" in caplog.text

    def test_ten_per_second_is_a_cap_and_retry_after_still_takes_precedence(self):
        clock = Clock()
        client = Client(
            [
                TrackingResponse(),
                TrackingResponse(status=429, headers={"Retry-After": "2"}),
                TrackingResponse(),
            ],
            clock,
        )
        requests = GovUkRequests(client, clock=clock.monotonic, sleep=clock.sleep)
        requests.configure({"min_interval": 0.1})
        requests.get("a")
        requests.get("b")
        assert [round(call[1], 2) for call in client.calls] == [0, 0.1, 2.1]
