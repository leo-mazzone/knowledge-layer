# GOV.UK ingestion

Implementation: [connector.py](connector.py). Registered connector name: `govuk`.

Discovery uses the public [Search API](https://docs.publishing.service.gov.uk/repos/search-api/using-the-search-api.html) at `https://www.gov.uk/api/search.json`. Retrieval uses the [Content API](https://content-api.publishing.service.gov.uk/getting-started.html) at `https://www.gov.uk/api/content/<path>`.

The extractor converts API HTML into readable text, preserving headings, known anchors, lists, table rows, multipart guides and steps. Raw JSON is retained separately. PDF sections follow page boundaries, and citations link to the PDF with `#page=N`. OCR output is searchable text, not a guarantee of transcription accuracy or faithful table reconstruction.

## Ingest an organisation

The supplied [Department for Business and Trade manifest](../../../collections/department-for-business-and-trade.json) configures a full organisation inventory, HTML/PDF attachments and retirement of missing documents:

```json
{
  "collection": "department-for-business-and-trade",
  "connector": "govuk",
  "config": {
    "organisations": ["department-for-business-and-trade"],
    "attachments": ["html", "pdf"],
    "http": {"min_interval": 0.1},
    "reconcile": true
  }
}
```

`organisations` is a nonempty list of GOV.UK organisation slugs. Multiple organisations match the union of their tags. Organisation mode cannot be combined with `urls` or `search`. It uses an empty search query, title ordering and 100-result pages, without the bounded mode's 1,000-result cap. API totals, examined results and unique paths are checked throughout pagination. Failures, changing totals, duplicate results and premature exhaustion leave discovery incomplete. API-imposed pagination limits are reported as failures rather than treated as complete coverage.

```sh
docker compose --profile ingest run --rm ingest knowledge ingest /collections/department-for-business-and-trade.json
```

For local Python, install Tesseract with English language data, then run:

```sh
# macOS
brew install tesseract
# Debian/Ubuntu alternative
sudo apt-get install tesseract-ocr tesseract-ocr-eng

uv sync --extra dev
uv run knowledge ingest collections/department-for-business-and-trade.json
```

The runtime Docker image includes Tesseract and English language data. OCR runs locally, without an external OCR service. `attachments` defaults to an empty list; enable `html` and `pdf` explicitly. Linked HTML documents are fetched through the Content API. PDFs are fetched from HTTPS on `www.gov.uk` or `assets.publishing.service.gov.uk`, including redirects restricted to those hosts. Structured attachment metadata is preferred, with publication document markup as a fallback. Attachments are followed recursively, with cycle detection and deduplication; general related links are not traversed. PDFs shared by different parent publications have separate parent-scoped identities.

Native PDF text is extracted per page. Pages with fewer than 40 alphanumeric characters are rendered at 300 DPI and passed to Tesseract. Sparse native text is retained when OCR returns no text; an entirely unreadable PDF fails ingestion. Corrupt or password-protected PDFs, OCR failures and configured resource limits are explicit errors. Original PDF bytes are retained as base64 in the existing raw JSON storage, alongside their SHA-256 digest and extracted page text. This increases storage use relative to metadata-only attachments.

Optional `config.pdf` settings override these defaults:

```json
{
  "max_bytes": 52428800,
  "max_pages": 1000,
  "max_pixels": 25000000,
  "ocr_timeout": 120,
  "ocr_language": "eng"
}
```

Size limits apply while streaming downloads; pixel limits apply per rendered page; OCR timeouts are in seconds per page. Additional OCR languages require installed Tesseract language data. PDF parsing dependencies are included in `pyproject.toml` and `uv.lock`.

## Coverage and retirement

### Request pacing and throttling

Every GOV.UK request uses a shared pacing policy within the ingestion, including Search API pages, verification, Content API requests, attachment downloads and redirect hops. The provider default minimum interval between request starts is one second across both GOV.UK hosts. The DBT manifest overrides this to 0.1 seconds, allowing up to ten request starts per second. These are application settings, not published GOV.UK API quotas. Fetching and processing remain sequential: response latency, embedding and OCR can make actual throughput substantially lower. Embedding requests use their separate adapter and are not included in GOV.UK pacing.

HTTP `429` and `503` responses are retried up to four times after the initial attempt. Both seconds and HTTP-date forms of `Retry-After` are honoured; missing or malformed values use exponential backoff starting at two seconds, with positive jitter and a 60-second ceiling. Rejected responses and streams are closed before waiting. HTTP `404` and other non-retry statuses retain normal ingestion error handling.

Repeated `429` or `503` responses exhaust the retry budget and stop all source requests immediately, including remaining documents and verification. A server-requested delay above the default 300-second wait limit also stops the run instead of retrying sooner than allowed. Previously published updates and memberships remain available, retirement is skipped, and the run reports `aborted: true` with the reason. Respect any reported server cooldown before restarting a stopped run.

Override the defaults through `config.http`; the DBT manifest supplies these values explicitly:

```json
{
  "min_interval": 0.1,
  "max_retries": 4,
  "initial_backoff": 2.0,
  "max_backoff": 60.0,
  "max_retry_after": 300.0
}
```

Intervals and delays must be positive and finite; `max_retries` must be an integer from zero to ten, and `max_backoff` must be at least `initial_backoff`. The run's `http` summary records its policy, request attempts, retries, `429`/`503` counts, wait time and stop reason. The request policy does not alter document fingerprints or require re-embedding unchanged documents.

A PostgreSQL advisory lock permits only one GOV.UK ingestion at a time across all collections using the same knowledge database. A second invocation fails immediately with an “already running” message. Separate deployments using different databases or sharing an outbound IP must coordinate their ingestion schedules independently. This implements GOV.UK's guidance to slow down after rate-limit responses; see the [CDN guidance](https://docs.publishing.service.gov.uk/manual/cdn.html) and [Content API caching guidance](https://content-api.publishing.service.gov.uk/getting-started.html).

Run summaries report `published`, `unchanged`, `discovered`, successfully extracted `pages` and `pdfs`, `ocr_pages`, `errors`, `exclusions`, discovery counts, `coverage`, and retirement results. Successfully extracted counts can include items whose later embedding or publication failed. An operational failure makes the run `failed` and the CLI exits nonzero; successfully processed updates remain available. Explicit exclusions, such as office files or external websites, make coverage `partial` without making the run fail. Bounded manifests also report partial coverage because they do not establish an organisation inventory.

Completeness is relative to the public Search API inventory and enabled attachments. Search-index omissions cannot be detected, and predecessor departments are not implicitly included. Withdrawn pages remain eligible when discoverable and readable; their withdrawal metadata is preserved. Historical GOV.UK editions, general related pages, external websites and office-file extraction are outside this version's scope.

`reconcile` defaults to `false` and requires organisation mode. Runs are serialized per collection. Before retirement, a second discovery pass must return the identical page set with consistent totals, and every discovered page and enabled attachment must have fetched, extracted, embedded and published successfully or remained unchanged. Any operational failure blocks all retirement. Explicit attachment exclusions do not block retirement. Two matching passes reduce inventory instability but do not establish an atomic source snapshot.

An eligible reconciliation removes only active GOV.UK memberships absent from the processed identity set in the selected collection, including removed attachments. A verified empty inventory retires all GOV.UK memberships in that collection. Retirement IDs and counts are recorded atomically with the completed run. Versions, chunks and other collection memberships are retained. Retired content is excluded from that collection's search; explicit historical `version_id` reads remain available. No database schema migration is required.

## Progress logs

The ingestion CLI emits timestamped `INFO` progress logs immediately to stderr, which Docker includes in container logs and the attached terminal. Logs identify startup and lock acquisition, discovery and verification counts, the document being fetched or embedded, large embedding batches, PDF extraction and OCR pages, published or unchanged documents, errors and elapsed time. Retry warnings identify the HTTP status, attempt, wait and reason; long cooldowns report remaining time. Normal pacing does not produce a line for every sleep. The final JSON run summary remains on stdout.

Use `--log-level DEBUG` after the manifest for extra discovery detail, or `--log-level WARNING` to show only warnings and errors. Log handlers flush each record, so progress does not wait for the final JSON summary. A single network or embedding request can still take time; the last stage message identifies the operation in progress.

```sh
docker compose --profile ingest run --rm ingest knowledge ingest /collections/department-for-business-and-trade.json --log-level INFO
```

Changes to the image and manifest apply to newly started ingestions. To use them during a running ingestion, stop the attached command with Ctrl+C, rebuild with `docker compose --profile ingest build ingest`, and rerun it. All application services share the same image tag; the explicit build command ensures the ingest image is rebuilt even though ingestion is a Compose profile. Successful stored editions are retained; unchanged documents are fetched again but do not require re-embedding. A rebuild cannot add logging or change the pacing of an existing container.

## Configure discovery

A manifest can contain explicit URLs, a Search API query, or both:

```json
{
  "collection": "uk-goods-exports",
  "connector": "govuk",
  "config": {
    "urls": ["https://www.gov.uk/export-goods", "https://www.gov.uk/eori"],
    "search": {
      "query": "exporting goods",
      "max_results": 20,
      "page_size": 10,
      "filters": {
        "filter_content_store_document_type": ["detailed_guide", "guide", "guidance", "manual_section"]
      }
    }
  }
}
```

URLs must use `https://www.gov.uk`, or be absolute content paths such as `/eori`. Queries and fragments are rejected. Discovery deduplicates paths across seeds and search results. Search pages use `start` and `count`, with `max_results` limiting the number of search results examined before deduplication. Supported limits are 1–1000 results and 1–100 results per page. Filters pass through the API's `filter_*` and `reject_*` parameters; list values are supported.

Content API redirects are followed only within the GOV.UK Content API origin and path. Response payloads over 8 MiB are rejected. Raw JSON is retained with each stored document version. Existing bounded manifests retain their URL validation and discovery limits, do not follow attachments unless explicitly enabled, and cannot enable reconciliation. The v2 extractor processes readable HTML; rerunning an existing collection creates new versions where the changed extraction signature requires reprocessing.

## Exporting goods collection

The supplied [uk-goods-exports manifest](../../../collections/uk-goods-exports.json) combines five curated seeds with up to twenty Search API results for exporting goods. Edit the query, filters, result limit, page size, or URLs to adjust scope. Bounded discovery does not establish exhaustive export coverage.

From the `knowledge` directory, after starting the shared service:

```sh
docker compose --profile ingest run --rm ingest knowledge ingest /collections/uk-goods-exports.json
```

For local Python:

```sh
uv run knowledge ingest collections/uk-goods-exports.json
```

Run the same command to refresh the collection. Failed items are reported in the run summary; their previous stored editions remain available. Pages missing from search results are not automatically removed.

## Identity and metadata

Document identity uses `content_id` where supplied, with a `base_path` fallback. Moving a page preserves identity when `content_id` stays the same. The canonical URL is formed from `base_path`.

The extractor maps title, description, document type, locale, linked publishing organisations, `first_published_at`, and `public_updated_at` into canonical metadata. Additional source metadata retains expanded links, attachment metadata, change history, schema name, content representation, and withdrawal notices where supplied. Full source JSON is retained separately. Publication and update dates are source dates, not evidence of legal effective dates or territorial applicability.

The extractor supports body strings, body content blocks, multipart guides, step-by-step navigation content, publication landing pages, structured document collections, and PDF pages. Unsupported or empty formats produce explicit ingestion errors. HTML and PDF attachments are fetched only when enabled. PDF metadata includes parent provenance, digest and extraction methods per page; tool responses expose parent provenance and extraction methods alongside normal metadata.

## Agent guidance

Use the shared instructions in [agent.md](../../../agent.md), adding these source-specific instructions for an agent working with government guidance:

> Use search_knowledge for government guidance and exporting goods, selecting collection uk-goods-exports when appropriate. For specific export requirements, establish where goods leave from, their destination, and what the goods are. Explain material source dates and withdrawal notices. A GOV.UK publication date is not a legal effective date. Distinguish guidance from legislation available through other tools.

Try: “I'm exporting manufactured goods from Great Britain. Which registrations and declaration guidance should I read?” Check that the agent searches, asks for missing shipment details, and cites GOV.UK URLs. Then ask it to read surrounding guidance using the same version ID.
