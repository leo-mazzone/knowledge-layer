"""Discover organisation content, fetch Content API pages and document attachments."""

import logging
from collections.abc import Iterable
from dataclasses import replace
from typing import Any
from urllib.parse import urljoin, urlsplit

from knowledge.models import Document, FetchedContent, Section, SourceItem

from .html import attachment_links, html_sections
from .http import GovUkRequests
from .pdf import DEFAULT_LIMITS, pdf_payload

logger = logging.getLogger(__name__)


def base_path(reference: str) -> str:
    parsed = urlsplit(reference)
    if (parsed.scheme or parsed.netloc) and (
        parsed.scheme != "https" or parsed.netloc != "www.gov.uk"
    ):
        raise ValueError("GOV.UK references must use https://www.gov.uk")
    path = parsed.path
    if not path.startswith("/") or path.startswith("//") or ".." in path.split("/"):
        raise ValueError("Expected an absolute GOV.UK content path")
    if parsed.query or parsed.fragment:
        raise ValueError("Content paths must not include queries or fragments")
    return path


class GovUkConnector:
    ingestion_lock_key = "govuk"

    def __init__(self, client):
        self.client = client
        self.requests = GovUkRequests(client)
        self.config = {}
        self.discovery_report = {"complete": False}
        self.exclusions = []
        self.inventory = set()
        self.statistics = {"ocr_pages": 0}

    @property
    def request_report(self):
        return dict(self.requests.report)

    def _organisation_paths(self, organisations, *, phase="discovery"):
        paths = set()
        start = 0
        total = None
        while total is None or start < total:
            logger.debug("Organisation %s requesting start=%d count=100", phase, start)
            response = self.requests.get(
                "https://www.gov.uk/api/search.json",
                params={
                    "q": "",
                    "fields": "link,title",
                    "filter_organisations": organisations,
                    "order": "title",
                    "start": start,
                    "count": 100,
                },
            )
            response.raise_for_status()
            data = response.json()
            current_total = data["total"]
            if not isinstance(current_total, int) or current_total < 0:
                raise ValueError("Invalid Search API total")
            if total is not None and current_total != total:
                raise ValueError("Search API total changed during discovery")
            total = current_total
            results = data["results"]
            if len(results) > 100 or start + len(results) > total:
                raise ValueError("Search API returned inconsistent result counts")
            for result in results:
                paths.add(base_path(result["link"]))
            start += len(results)
            self.discovery_report.update(total=total, examined=start, unique_paths=len(paths))
            logger.info(
                "Organisation %s examined=%d/%d unique_paths=%d", phase, start, total, len(paths)
            )
            if len(results) < min(100, total - (start - len(results))):
                raise ValueError("Search API pagination ended before the reported total")
            if not results:
                break
        if len(paths) != total:
            raise ValueError("Duplicate search results prevented complete discovery")
        return paths

    def verify_discovery(self):
        if not self.config.get("organisations"):
            return False
        self.discovery_report["complete"] = False
        logger.info("Verifying organisation inventory expected_pages=%d", len(self.inventory))
        verification = self._organisation_paths(self.config["organisations"], phase="verification")
        if verification != self.inventory:
            raise ValueError("Organisation inventory changed between discovery passes")
        self.discovery_report.update(complete=True, verified=True)
        logger.info("Organisation inventory verified pages=%d", len(verification))
        return True

    def discover(self, config: dict[str, Any]) -> Iterable[SourceItem]:
        self.requests.configure(config.get("http", {}))
        logger.info(
            "GOV.UK request policy min_interval=%gs max_requests_per_second=%g max_retries=%d",
            self.requests.policy["min_interval"],
            1 / self.requests.policy["min_interval"],
            self.requests.policy["max_retries"],
        )
        self.config = config
        self.exclusions = []
        self.inventory = set()
        self.statistics = {"ocr_pages": 0}
        self.discovery_report = {"complete": False, "verified": False}
        organisations = config.get("organisations")
        attachments = config.get("attachments", [])
        if not isinstance(attachments, list) or any(x not in ("html", "pdf") for x in attachments):
            raise ValueError("attachments must be a list containing html and/or pdf")
        self.limits = {**DEFAULT_LIMITS, **config.get("pdf", {})}
        if set(self.limits) != set(DEFAULT_LIMITS):
            raise ValueError("Unknown PDF configuration option")
        for key in ("max_bytes", "max_pages", "max_pixels", "ocr_timeout"):
            if not isinstance(self.limits[key], (int, float)) or self.limits[key] <= 0:
                raise ValueError(f"PDF {key} must be positive")
        if (
            not isinstance(self.limits["ocr_language"], str)
            or not self.limits["ocr_language"].strip()
        ):
            raise ValueError("PDF ocr_language must not be empty")
        if config.get("reconcile") and not organisations:
            raise ValueError("Reconciliation requires full organisation discovery")
        if organisations is not None:
            if (
                not isinstance(organisations, list)
                or not organisations
                or any(
                    not isinstance(org, str)
                    or not org
                    or any(char not in "abcdefghijklmnopqrstuvwxyz0123456789-" for char in org)
                    for org in organisations
                )
            ):
                raise ValueError("organisations must be a nonempty list of organisation slugs")
            if config.get("urls") or config.get("search"):
                raise ValueError("Organisation discovery cannot be mixed with urls or search")
            self.inventory = self._organisation_paths(organisations)
            self.discovery_report["complete"] = True
            logger.info(
                "Organisation discovery complete pages=%d; processing documents",
                len(self.inventory),
            )
            for path in sorted(self.inventory):
                yield SourceItem(path)
            return
        references = config.get("urls", [])
        search = config.get("search")
        if not isinstance(references, list) or (not references and not search):
            raise ValueError("GOV.UK requires urls, search configuration, or both")
        seen = set()
        for reference in references:
            path = base_path(reference)
            if path not in seen:
                seen.add(path)
                yield SourceItem(path)
        if search:
            max_results = int(search.get("max_results", 20))
            page_size = int(search.get("page_size", 10))
            if not 1 <= max_results <= 1000 or not 1 <= page_size <= 100:
                raise ValueError("Require max_results 1–1000 and page_size 1–100")
            params = {"q": search.get("query", ""), "fields": "link,title"}
            for name, value in search.get("filters", {}).items():
                if not name.startswith(("filter_", "reject_")):
                    raise ValueError("Search filters must use filter_* or reject_* API parameters")
                params[name] = value
            start = 0
            while start < max_results:
                count = min(page_size, max_results - start)
                response = self.requests.get(
                    "https://www.gov.uk/api/search.json",
                    params={**params, "start": start, "count": count},
                )
                response.raise_for_status()
                results = response.json()["results"]
                logger.info(
                    "Search discovery examined=%d limit=%d", start + len(results), max_results
                )
                for result in results:
                    path = base_path(result["link"])
                    if path not in seen:
                        seen.add(path)
                        yield SourceItem(path)
                if len(results) < count:
                    break
                start += count

    def expand(self, content: FetchedContent) -> Iterable[SourceItem]:
        if content.payload.get("representation") == "pdf":
            return
        data = content.payload
        parent_id = data.get("content_id") or data["base_path"]
        parent = {
            "source_id": parent_id,
            "url": "https://www.gov.uk" + base_path(data["base_path"]),
            "title": data["title"],
            "publishers": [
                org["title"]
                for org in (data.get("links") or {}).get("organisations", [])
                if org.get("title")
            ],
            "published_at": data.get("first_published_at"),
            "source_updated_at": data.get("public_updated_at"),
            "locale": data.get("locale", "en"),
        }
        inherited = data.get("_parent")
        if inherited and not parent["publishers"]:
            parent["publishers"] = inherited["publishers"]
        enabled = self.config.get("attachments", [])
        # Legacy manifests intentionally do not traverse attachments.
        if not enabled and not self.config.get("organisations"):
            return
        seen = set()
        for attachment in attachment_links(data.get("details") or {}):
            url = urljoin(parent["url"], attachment.get("url", ""))
            if url in seen:
                continue
            seen.add(url)
            parsed = urlsplit(url)
            content_type = attachment.get("content_type", "").split(";")[0].lower()
            kind = "unsupported"
            if attachment.get("attachment_type") == "html" or (
                attachment.get("attachment_type") is None
                and parsed.netloc == "www.gov.uk"
                and parsed.path.startswith("/government/")
                and "." not in parsed.path.rsplit("/", 1)[-1]
            ):
                kind = "html"
            elif content_type == "application/pdf" or parsed.path.lower().endswith(".pdf"):
                kind = "pdf"
            if parsed.scheme != "https" or parsed.netloc not in (
                "www.gov.uk",
                "assets.publishing.service.gov.uk",
            ):
                kind = "external"
            metadata = {"kind": kind, "parent": parent, "attachment": {**attachment, "url": url}}
            if kind not in enabled:
                logger.debug("Attachment excluded reason=%s parent=%s", kind, parent["url"])
                self.exclusions.append({"reference": url, "parent": parent["url"], "reason": kind})
                continue
            key = f"attachment:{parent_id}:{attachment.get('id') or url}" if kind == "pdf" else None
            yield SourceItem(base_path(url) if kind == "html" else url, metadata, key)

    def fetch(self, item: SourceItem) -> FetchedContent:
        if item.metadata.get("kind") == "pdf":
            return self._fetch_pdf(item)
        path = base_path(item.reference)
        url = "https://www.gov.uk/api/content" + path
        for _ in range(5):
            response = self.requests.get(url)
            if response.status_code in (301, 302, 303, 307, 308):
                url = urljoin(url, response.headers["location"])
                parsed = urlsplit(url)
                if (
                    parsed.scheme != "https"
                    or parsed.netloc != "www.gov.uk"
                    or not parsed.path.startswith("/api/content/")
                ):
                    raise ValueError("Unexpected Content API redirect")
                continue
            response.raise_for_status()
            if len(response.content) > 8 * 1024 * 1024:
                raise ValueError("Content API response exceeds 8 MiB")
            payload = response.json()
            if item.metadata.get("parent"):
                payload = {**payload, "_parent": item.metadata["parent"]}
            return FetchedContent(item.reference, payload)
        raise ValueError("Too many Content API redirects")

    def _fetch_pdf(self, item):
        logger.info("Downloading PDF reference=%s", item.reference)
        url = item.reference
        for _ in range(5):
            parsed = urlsplit(url)
            if (
                parsed.scheme != "https"
                or parsed.netloc not in ("www.gov.uk", "assets.publishing.service.gov.uk")
                or parsed.username
                or parsed.password
            ):
                raise ValueError("Unexpected PDF attachment origin or redirect")
            with self.requests.stream(url) as response:
                if response.status_code in (301, 302, 303, 307, 308):
                    url = urljoin(url, response.headers["location"])
                    continue
                response.raise_for_status()
                data = bytearray()
                for block in response.iter_bytes():
                    data.extend(block)
                    if len(data) > self.limits["max_bytes"]:
                        raise ValueError("PDF attachment exceeds configured size limit")
            if not data.startswith(b"%PDF-"):
                raise ValueError("Attachment response is not a PDF")
            logger.info(
                "PDF downloaded reference=%s bytes=%d; extracting text", item.reference, len(data)
            )
            payload = pdf_payload(bytes(data), item.metadata, self.limits)
            self.statistics["ocr_pages"] += sum(
                page["method"] == "ocr" for page in payload["pages"]
            )
            logger.info(
                "PDF extracted reference=%s pages=%d", item.reference, len(payload["pages"])
            )
            return FetchedContent(item.reference, payload)
        raise ValueError("Too many PDF redirects")


def body_sections(body: Any, headings: tuple[str, ...] = (), locator: str | None = None):
    """Convert API HTML strings to readable sections with known anchors."""
    if isinstance(body, str) and body.strip():
        return html_sections(body, headings, locator)
    sections = []
    if isinstance(body, list):
        for block in body:
            if isinstance(block, dict) and isinstance(block.get("content"), str):
                sections.extend(body_sections(block["content"], headings, locator))
    return sections


def step_sections(steps: list[dict]) -> list[Section]:
    sections = []
    for step in steps:
        title = step.get("title", "")
        for block in step.get("contents", []):
            content = block.get("contents")
            if isinstance(content, str):
                sections.extend(body_sections(content, (title,)))
            elif isinstance(content, list):
                lines = []
                for item in content:
                    if isinstance(item, dict):
                        text = item.get("text", "")
                        href = item.get("href")
                        lines.append(f"{text} ({href})" if href else text)
                sections.extend(body_sections("\n".join(lines), (title,)))
    return sections


class GovUkExtractor:
    signature = "govuk-html-pdf-ocr-v2"

    def extract(self, content: FetchedContent) -> Document:
        data = content.payload
        if data.get("representation") == "pdf":
            return self._extract_pdf(content)
        if data.get("document_type") in ("redirect", "gone"):
            raise ValueError(
                "Content unavailable; inspect and retire the previous edition manually"
            )
        path = base_path(data["base_path"])
        details = data.get("details") or {}
        sections = body_sections(details.get("body"))
        for part in details.get("parts") or []:
            slug = part.get("slug")
            sections.extend(
                body_sections(
                    part.get("body"), (part.get("title", ""),), f"part:{slug}" if slug else None
                )
            )
        sections.extend(step_sections(details.get("steps") or []))
        if not sections and data.get("schema_name") in ("publication", "document_collection"):
            # Landing pages are genuine documents even when their body is empty.
            attachments = attachment_links(details)
            groups = details.get("groups") or []
            lines = [data["title"], data.get("description") or ""]
            lines.extend(
                f"{item.get('title') or 'Document'} ({item.get('url', '')})" for item in attachments
            )
            for group in groups:
                lines.append(group.get("title", ""))
                lines.extend(str(item) for item in group.get("documents") or [])
            if attachments or groups:
                sections = [Section("\n".join(line for line in lines if line))]
        if not sections:
            raise ValueError(f"Unsupported or empty GOV.UK format: {data.get('document_type')}")
        links = data.get("links") or {}
        publishers = tuple(
            org["title"] for org in links.get("organisations", []) if org.get("title")
        )
        document = Document(
            source="govuk",
            source_id=data.get("content_id") or path,
            canonical_url="https://www.gov.uk" + path,
            title=data["title"],
            sections=tuple(sections),
            description=data.get("description") or "",
            document_type=data.get("document_type", "document"),
            language=data.get("locale", "en"),
            publishers=publishers,
            published_at=data.get("first_published_at"),
            source_updated_at=data.get("public_updated_at"),
            retrieved_at=content.retrieved_at,
            source_metadata={
                "base_path": path,
                "schema_name": data.get("schema_name"),
                "content_representation": "content-api-html-text",
                "links": links,
                "withdrawn_notice": data.get("withdrawn_notice") or details.get("withdrawn_notice"),
                "attachments": details.get("attachments", []),
                "change_history": details.get("change_history", []),
            },
        )
        if data.get("_parent"):
            parent = data["_parent"]
            document = replace(
                document,
                publishers=document.publishers or tuple(parent["publishers"]),
                source_metadata={**document.source_metadata, "parent": parent},
            )
        return document

    def _extract_pdf(self, content):
        data = content.payload
        metadata = data["attachment"]
        parent = metadata["parent"]
        attachment = metadata["attachment"]
        identity = attachment.get("id") or attachment["url"]
        sections = tuple(
            Section(page["text"], (f"Page {page['number']}",), f"page={page['number']}")
            for page in data["pages"]
            if page["text"].strip()
        )
        if not sections:
            raise ValueError("PDF has no readable text")
        return Document(
            source="govuk",
            source_id=f"attachment:{parent['source_id']}:{identity}",
            canonical_url=attachment["url"],
            title=attachment.get("title") or parent["title"],
            sections=sections,
            document_type="pdf",
            language=attachment.get("locale") or parent["locale"],
            publishers=tuple(parent["publishers"]),
            published_at=parent["published_at"],
            source_updated_at=parent["source_updated_at"],
            retrieved_at=content.retrieved_at,
            source_metadata={
                "content_representation": "pdf-text-ocr",
                "parent": parent,
                "attachment": attachment,
                "sha256": data["sha256"],
                "page_methods": [
                    {"page": p["number"], "method": p["method"]} for p in data["pages"]
                ],
            },
        )
