# Shared knowledge for LibreChat

Python document ingestion and retrieval service backed by PostgreSQL + pgvector. One MCP server exposes `search_knowledge` and `read_document`.

Source-specific configuration, extraction, metadata, and limitations are documented alongside each connector:

| Connector | Documentation | Implementation |
| --- | --- | --- |
| `govuk` | [GOV.UK ingestion](src/knowledge/connectors/govuk/README.md) | [govuk/](src/knowledge/connectors/govuk/) |

## Start the service

Prerequisites: Docker Compose and a running Ollama service on the host at port
11434. The default embedding model is `nomic-embed-text`, with 768 dimensions.

```sh
cd /Users/leo-mazzone/Documents/code/govchat/knowledge
ollama pull nomic-embed-text
docker compose up --build -d
curl --fail http://localhost:8091/health
```

Compose starts Postgres, runs schema initialization, then starts the MCP service. Initialization, serving and ingestion use the shared `govchat-knowledge:local` image, so rebuilding the application also updates the image used by the ingest profile. To explicitly rebuild it for ingestion, use `docker compose --profile ingest build ingest`. Ingestion is a job run manually. Choose or create a collection manifest using the connector documentation above. From the `knowledge` directory, replace `your-collection.json` with that manifest's filename:

```sh
docker compose --profile ingest run --rm ingest knowledge ingest /collections/your-collection.json
```

Compose mounts the `collections/` directory at `/collections`. Run the same command again to refresh the collection. Ingestion emits timestamped progress logs to stderr immediately, followed by a final JSON summary on stdout, and exits nonzero if anything failed; successfully processed documents remain available. Use `--log-level DEBUG` after the manifest for more detail or `--log-level WARNING` to reduce output. The DBT manifest caps GOV.UK requests at ten per second with retry backoff and run-wide throttling safeguards; see the connector documentation for the precise coverage and request policy.

Optional configuration: copy `.env.example` to `.env` and change the embedding endpoint, model, dimensions, API key, and input profile. The provider must support `POST /v1/embeddings` with batched `input` and indexed `data` results. The configured dimension is validated against every returned vector. Defaults keep embeddings local. The endpoint, model, dimensions, and effective document/query prefixes are part of the processing fingerprint; changing them requires re-ingestion before search will match stored vectors.

### Embedding input profiles

`EMBEDDING_PROFILE` selects the model's input conventions explicitly; changing `EMBEDDING_MODEL` does not automatically select a profile. The default `nomic` profile prepends `search_document: ` to document passages and `search_query: ` to search queries, following [Nomic's retrieval instructions](https://huggingface.co/nomic-ai/nomic-embed-text-v1.5#usage). Prefixes affect embedding requests only; stored content, keyword search, citations, and MCP results remain unchanged.

```dotenv
# Default local Nomic model
EMBEDDING_MODEL=nomic-embed-text
EMBEDDING_DIMENSIONS=768
EMBEDDING_PROFILE=nomic
```

For a model that expects plain text, select `plain` and configure the appropriate endpoint, API key, model, and output dimensions. For example:

```dotenv
EMBEDDING_BASE_URL=https://api.openai.com/v1
EMBEDDING_MODEL=text-embedding-3-small
EMBEDDING_DIMENSIONS=1536
EMBEDDING_API_KEY=your-api-key
EMBEDDING_PROFILE=plain
```

For other prefix conventions, use `custom` and define both variables. Values are preserved exactly, including trailing spaces; explicitly empty values are supported. Missing custom variables or an unknown profile fail configuration before the application starts.

```dotenv
EMBEDDING_PROFILE=custom
EMBEDDING_DOCUMENT_PREFIX="passage: "
EMBEDDING_QUERY_PREFIX="query: "
# Set either prefix to "" when that input should remain plain.
```

Ingestion and serving must use identical embedding configuration. When upgrading from the original unprefixed Nomic implementation, rebuild the image, re-ingest every collection, then recreate the serving container:

```sh
docker compose build
docker compose --profile ingest run --rm ingest knowledge ingest /collections/your-collection.json
# Repeat ingestion for every collection before switching the serving process.
docker compose up -d --no-deps --force-recreate knowledge
```

No schema migration or data deletion is needed. Changed prefixes produce new document versions; existing historical editions remain readable. Searches with the new configuration exclude editions using old embedding signatures, even from keyword results, until those documents are re-ingested. The `plain` profile preserves the original unprefixed signature and can retain the previous behavior. Equivalent effective prefixes share the same signature regardless of profile name.

Model input preparation is implemented separately from HTTP transport. Applications depend on the `Embedder` protocol's `embed_documents(texts)` and `embed_query(text)` methods; a provider requiring a different request format can implement that protocol and be wired in through `build_components`. Direct `CompatibleEmbedder` construction defaults to plain inputs for compatibility; application configuration explicitly selects Nomic by default. Its low-level `embed(texts)` method sends already prepared inputs unchanged and should not be used by ingestion or retrieval.

Postgres data persists in the `knowledge-db` volume. Service ports are published on loopback, and the MCP server accepts the Docker Desktop host name used below. The development database credentials are `knowledge` / `knowledge`.

## Connect LibreChat

Merge [librechat.yaml](librechat.yaml) into your existing LibreChat configuration. Preserve existing server and address entries. The snippet defines the shared knowledge server and its allowed address, `host.docker.internal:8091`.

After merging the config:

```sh
cd /Users/leo-mazzone/Documents/code/LibreChat
docker compose up -d --no-deps --force-recreate api
```

Enable both shared-knowledge tools in the chat or agent tool configuration. For your default agent, add the instructions in `knowledge/agent.md` and select that agent for conversations that should consult the knowledge layer. Add relevant source-specific instructions from the connector documentation. The collection is shared across chats; tool availability and whether the model calls a tool are controlled by LibreChat's chat/agent configuration.

To check connectivity from inside LibreChat:

```sh
docker compose exec api node -e 'fetch("http://host.docker.internal:8091/health").then(async r => { console.log(r.status, await r.text()); process.exit(r.ok ? 0 : 1); }).catch(e => { console.error(e.message); process.exit(1); })'
```

The supplied configuration is a merge snippet, not a replacement for an existing configuration. Running LibreChat images can differ in their supported settings; the repository's legislation README discusses the older `allowedDomains` option.

## Tools

| Tool | Inputs | Result |
| --- | --- | --- |
| `search_knowledge` | `query`; optional `collection`, `source`, `document_type`, `publisher`, `limit` (1–10) | Ranked passages, document/version IDs, chunk numbers, metadata and citations |
| `read_document` | `document_id`; optional `version_id`, `start_chunk`, `limit` (1–10) | Sequential passages and `next_chunk` for pagination |

Publisher filters use exact organisation names. Search combines Postgres full-text ranking with exact cosine vector retrieval using reciprocal rank fusion. Both candidate queries use the same database snapshot and apply the same filters. Scores are ranking signals, not calibrated confidence values.

Pass the search result's `version_id` when reading more context so a refresh cannot change the edition between calls. Omitting it selects the most recently created active edition. Raw source payloads and expanded link metadata are retained in storage; tool responses project the useful metadata to avoid repeating large linked payloads in model context.

The MCP server shares an embedding HTTP client across requests and closes it only when the serving process exits. Stateless MCP lifespans run per request and must not close process-owned resources. Transport regression tests cover initialization, tool listing and repeated searches through the HTTP endpoint, plus client cleanup on shutdown and startup failure.

## Architecture

```text
source connector → fetched content → source extractor → canonical document
                                                           ↓
                                                shared ingestion pipeline
                                                           ↓
                                            chunker → embedder → repository
                                                                     ↑
LibreChat → MCP tools → retrieval application service ─────────────────┘
```

`models.py` defines source-neutral dataclasses. `ports.py` defines the connector, extractor, chunker, embedder, and repository protocols. `pipeline.py` contains the ingestion use case; `retrieval.py` contains search/read use cases. Neither imports a source client, SQL driver, or MCP SDK. `registry.py` resolves source adapters. `server.py` and `cli.py` are application entry points.

The initial implementation uses synchronous adapters, with MCP calls dispatched to worker threads so database and HTTP calls do not block the event loop. There is no queue or scheduler; another worker can call the same pipeline later.

### Document model

| Record | Responsibility |
| --- | --- |
| `documents` | Stable identity, source and source-native identifier |
| `document_versions` | Metadata, source/processing fingerprint, raw payload, extracted content, embedding configuration |
| `collection_documents` | Active version for each document in each collection |
| `chunks` | Passage order, structured heading path, location, full-text index and embedding |
| `ingestion_runs` | Connector, collection, timestamps, status, counts and errors |

Canonical metadata includes title, description, document type, language, publishing organisations, canonical URL, source publication/update dates, and retrieval time. Additional source metadata lives in a namespaced JSON object. Each connector documents its source-specific mappings and additions. Full source payloads are preserved separately. The document dataclass defines common fields; JSONB allows additional metadata without a database migration. No geographical applicability or legal effective date is inferred from publication time or URL.

Identity combines the source name and source-native identifier supplied by the extractor. Fingerprints cover body, metadata, extractor version, chunking configuration, embedding endpoint/model/dimensions, and effective document/query prefixes. Retrieval time is excluded, so unchanged fetches do not trigger re-embedding. The stored retrieval date records the captured version, rather than each subsequent unchanged poll.

Publication inserts the complete version and all chunks and switches the collection pointer in a single transaction. A per-document advisory lock serializes publication; failed fetching, extraction, embedding, or persistence leaves the previous version active. Historical editions remain readable. Runs may partially succeed across documents. A collection lock serializes refreshes. Missing discovery results are retained by default; GOV.UK organisation manifests can explicitly enable reconciliation after a verified clean run. Retirement removes collection memberships and records the audit atomically, preserving versions and other collections. Unavailable pages block retirement and remain reported errors.

### Add an ingestion source

Implement `discover(config)` and `fetch(item)` on a connector, and `extract(fetched_content)` on an extractor. Return the domain models from `models.py`. Give the extractor an explicit signature; increment it when its behaviour changes so unchanged source content is reprocessed when necessary.

Register a factory returning a `SourceAdapter` in `SourceRegistry`, or package it as a plugin:

```toml
[project.entry-points."knowledge.sources"]
sharepoint = "my_ingestion.sharepoint:create_adapter"
```

```python
from knowledge.registry import SourceAdapter

def create_adapter(http_client):
    return SourceAdapter(
        connector=SharePointConnector(http_client),
        extractor=SharePointExtractor(),
        extractor_signature="sharepoint-v1",
    )
```

Install that package in the service image/environment, then use a manifest:

```json
{
  "collection": "company-documents",
  "connector": "sharepoint",
  "config": {"site": "your-source-specific-settings"}
}
```

The connector owns discovery and authentication; the extractor owns format mapping. Connectors can optionally implement `expand(fetched_content)` to yield independently processed attachments and `verify_discovery()` with a `discovery_report` to establish reconciliation eligibility. `SourceItem` accepts optional opaque metadata and a deduplication key. A connector can declare `ingestion_lock_key` to require repository source locking across collections, and raise `SourceRequestsStopped` to abort the entire run while retaining previous memberships. The shared pipeline owns deduplication, chunking, embedding, error reporting, publication and guarded reconciliation. New connectors do not write directly to SQL. Repositories implement source and collection locking and atomic reconciliation; the PostgreSQL implementation needs no schema migration. Keep each source's configuration, metadata mapping, identity rules, extraction behaviour, and limitations in a document alongside its implementation. Add its documentation link to the connector table above.

### Local Python development and checks

Python 3.13 or later is required. The `.python-version` file selects 3.13 for local uv commands; both Docker stages also use Python 3.13.

```sh
cd /Users/leo-mazzone/Documents/code/govchat/knowledge
uv sync --extra dev
uv run knowledge init-db
uv run knowledge ingest collections/your-collection.json
uv run knowledge serve
```

Replace `your-collection.json` with your chosen manifest. The default local database URL uses port 5433 from Compose. Local Python uses `http://localhost:11434/v1` for embeddings; Docker uses `http://host.docker.internal:11434/v1`. Use the same configured embedding endpoint for ingestion and serving because the endpoint is part of the signature. Local serving defaults to port 8080; set `PORT=8091` to use the supplied LibreChat configuration. `.env` is read by Compose; for local Python export environment variables explicitly.

```sh
uv run ruff check src tests
uv run ruff format --check src tests
uv run --extra dev pytest -v
```

With pytest installed, run the core behavioural tests directly:

```sh
python3.13 -m pytest -v
```

MCP wiring checks run when the SDK is installed. To run PostgreSQL checks, point `KNOWLEDGE_TEST_DATABASE_URL` at a disposable pgvector database:

```sh
KNOWLEDGE_TEST_DATABASE_URL=postgresql://knowledge:knowledge@localhost:5433/knowledge \
  uv run --extra dev pytest -v
```

Database checks create and remove an isolated schema and install the vector extension in `public` if needed. They exercise SQL retrieval, filtering, pagination, version retention, and actual transaction rollback.

## Current scope and validation

The test suite covers exhaustive pagination, inventory instability, readable HTML, attachment isolation, PDF/OCR behaviour and guarded retirement. Opt-in PostgreSQL tests cover collection lock serialization, atomic retirement/audit rollback, source scoping, other collection preservation and historical reads. Real PDF and OCR smoke tests require the PDF dependencies and Tesseract; PostgreSQL tests require `KNOWLEDGE_TEST_DATABASE_URL`. The suite has passed after regenerating the dependency lockfile with uv. Full live DBT ingestion and LibreChat connectivity remain separate end-to-end checks. The image uses a separate build stage and includes local Tesseract in its runtime stage; build tools, duplicate source trees, tests, and connector documentation are excluded from the runtime image.

The service currently has no application authentication or document ACLs. Connector-specific scope and limitations are documented alongside each adapter. Automatic refresh, office-file extraction, ANN indexes, and an OpenSearch adapter are deferred. GOV.UK organisation ingestion includes opt-in membership reconciliation. The initial schema is version 1; future schema changes should be introduced as explicit migrations. Python dependencies update when the lockfile is refreshed; the Postgres image tag permits patch updates. Both local development and Docker installation use `uv.lock`; Docker builds fail if it disagrees with `pyproject.toml`.

Official references: [pgvector](https://github.com/pgvector/pgvector), [MCP Python SDK v1](https://github.com/modelcontextprotocol/python-sdk/tree/v1.x), [Ollama compatible API](https://docs.ollama.com/api/openai-compatibility), [LibreChat MCP configuration](https://www.librechat.ai/docs/configuration/librechat_yaml/object_structure/mcp_servers).
