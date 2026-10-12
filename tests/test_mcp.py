"""MCP schema and tool wiring checks when the SDK is installed."""

import importlib.util
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from knowledge.embeddings import CompatibleEmbedder
from knowledge.server import create_server, serve


class Service:
    def search(self, query, filters, limit):
        return {"query": query, "collection": filters.collection, "limit": limit, "results": []}

    def read(self, document_id, version_id, start_chunk, limit):
        return {
            "document_id": document_id,
            "version_id": version_id,
            "start_chunk": start_chunk,
            "limit": limit,
        }


@pytest.mark.skipif(
    not (importlib.util.find_spec("mcp")), reason="Install project dependencies to check MCP"
)
class TestMCP:
    @pytest.mark.asyncio
    async def test_two_read_only_tools_and_calls(self):
        server = create_server(Service(), lambda: None)
        tools = await server.list_tools()
        assert {tool.name for tool in tools} == {"search_knowledge", "read_document"}
        for tool in tools:
            assert tool.annotations.readOnlyHint
        search = await server.call_tool(
            "search_knowledge",
            {
                "query": "export goods",
                "collection": "uk-goods-exports",
                "limit": 2,
            },
        )
        assert "uk-goods-exports" in str(search)
        document_id = str(uuid4())
        read = await server.call_tool("read_document", {"document_id": document_id})
        assert document_id in str(read)


@pytest.mark.skipif(
    not (importlib.util.find_spec("mcp")), reason="Install project dependencies to check MCP"
)
class TestMCPHTTP:
    @pytest.fixture(autouse=True)
    def setup(self, request):
        self.request = request

    def test_shared_embedding_client_survives_handshake_and_repeated_http_tool_calls(self):
        import httpx
        from starlette.testclient import TestClient

        embedding_requests = []

        def embedding_response(request):
            embedding_requests.append(request)
            return httpx.Response(200, json={"data": [{"index": 0, "embedding": [1, 0, 0]}]})

        client = httpx.Client(transport=httpx.MockTransport(embedding_response))
        self.request.addfinalizer(client.close)
        embedder = CompatibleEmbedder(client, "http://embedding.test/v1", "test-model", 3)
        repository = MagicMock()
        repository.search.return_value = [{"text": "Stored export guidance"}]
        settings = SimpleNamespace(host="127.0.0.1", port=8080)

        def run_app(app, **kwargs):
            with TestClient(app, base_url="http://localhost:8080") as connection:
                headers = {"Accept": "application/json, text/event-stream"}

                def call(identifier, method, params):
                    response = connection.post(
                        "/mcp",
                        headers=headers,
                        json={
                            "jsonrpc": "2.0",
                            "id": identifier,
                            "method": method,
                            "params": params,
                        },
                    )
                    assert response.status_code == 200, response.text
                    assert "error" not in response.json()
                    return response.json()["result"]

                initialized = call(
                    1,
                    "initialize",
                    {
                        "protocolVersion": "2025-03-26",
                        "capabilities": {},
                        "clientInfo": {"name": "lifecycle-regression", "version": "1.0"},
                    },
                )
                headers["MCP-Protocol-Version"] = initialized["protocolVersion"]
                listed = call(2, "tools/list", {})
                assert {tool["name"] for tool in listed["tools"]} == {
                    "search_knowledge",
                    "read_document",
                }
                assert connection.get("/health").json() == {"status": "ok"}
                for identifier in range(3, 6):
                    result = call(
                        identifier,
                        "tools/call",
                        {
                            "name": "search_knowledge",
                            "arguments": {"query": "export goods", "collection": "dbt"},
                        },
                    )
                    assert not result.get("isError", False), result
                    assert "Stored export guidance" in str(result)
                    assert not client.is_closed
            # The process owner closes the client after Uvicorn returns, not per HTTP request.
            assert not client.is_closed

        with (
            patch("knowledge.config.build_components", return_value=(client, repository, embedder)),
            patch("uvicorn.run", side_effect=run_app),
            patch.object(client, "close", wraps=client.close) as close,
        ):
            serve(settings)
            close.assert_called_once()
        assert client.is_closed
        assert len(embedding_requests) == 3
        assert repository.search.call_count == 3

    def test_embedding_client_is_closed_when_startup_health_check_fails(self):
        client, repository, embedder = MagicMock(), MagicMock(), MagicMock()
        repository.health.side_effect = RuntimeError("Database unavailable")
        settings = SimpleNamespace(host="127.0.0.1", port=8080)
        with (
            patch("knowledge.config.build_components", return_value=(client, repository, embedder)),
            patch("uvicorn.run") as run,
            pytest.raises(RuntimeError, match="Database unavailable"),
        ):
            serve(settings)
        client.close.assert_called_once()
        run.assert_not_called()
