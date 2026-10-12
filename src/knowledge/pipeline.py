"""Source-neutral ingestion orchestration. No source or SQL logic belongs here."""

import logging
import time
from collections import deque
from contextlib import ExitStack
from typing import Any

from knowledge.errors import SourceRequestsStopped
from knowledge.models import digest
from knowledge.ports import Chunker, Connector, Embedder, Extractor, Repository
from knowledge.processing import validate_vectors

logger = logging.getLogger(__name__)


class IngestionPipeline:
    def __init__(self, repository: Repository, embedder: Embedder, chunker: Chunker):
        self.repository = repository
        self.embedder = embedder
        self.chunker = chunker

    def run(
        self,
        name: str,
        connector: Connector,
        extractor: Extractor,
        extractor_signature: str,
        config: dict[str, Any],
        collection: str,
    ) -> dict[str, Any]:
        if not collection.strip():
            raise ValueError("Collection must not be empty")
        lock = getattr(self.repository, "collection_lock", None)
        if config.get("reconcile") and not callable(lock):
            raise ValueError("Reconciliation requires repository collection locking")
        logger.info("Acquiring ingestion locks source=%s collection=%s", name, collection)
        with ExitStack() as locks:
            source_key = getattr(connector, "ingestion_lock_key", None)
            if source_key:
                source_lock = getattr(self.repository, "source_lock", None)
                if not callable(source_lock):
                    raise ValueError("Connector requires repository source locking")
                locks.enter_context(source_lock(source_key))
            if callable(lock):
                locks.enter_context(lock(collection))
            return self._run(name, connector, extractor, extractor_signature, config, collection)

    def _run(self, name, connector, extractor, extractor_signature, config, collection):
        started = time.monotonic()
        run_id = self.repository.start_run(name, collection)
        logger.info("Ingestion started run=%s source=%s collection=%s", run_id, name, collection)
        summary: dict[str, Any] = {
            "run_id": run_id,
            "published": 0,
            "unchanged": 0,
            "discovered": 0,
            "pages": 0,
            "pdfs": 0,
            "ocr_pages": 0,
            "errors": [],
            "aborted": False,
            "exclusions": [],
            "retirement": {"performed": False, "reason": "not requested", "count": 0, "ids": []},
            "processing": {
                "pipeline": "ingestion-v2",
                "extractor": extractor_signature,
                "chunker": self.chunker.signature,
                "embedder": self.embedder.signature,
            },
        }
        seen = set()
        processed = set()

        def process(item, queue):
            key = item.key or item.reference
            if key in seen:
                return
            seen.add(key)
            summary["discovered"] += 1
            item_started = time.monotonic()
            stage = "fetch"
            logger.info("Fetching item=%d reference=%s", summary["discovered"], item.reference)
            try:
                raw = connector.fetch(item)
                expand = getattr(connector, "expand", None)
                if callable(expand):
                    try:
                        queue.extend(expand(raw))
                    except SourceRequestsStopped:
                        raise
                    except Exception as exc:  # noqa: BLE001
                        logger.error(
                            "Attachment discovery failed reference=%s error=%s", item.reference, exc
                        )
                        summary["errors"].append(
                            {"reference": item.reference, "stage": "attachments", "error": str(exc)}
                        )
                stage = "extract"
                document = extractor.extract(raw)
                logger.info(
                    "Extracted reference=%s type=%s sections=%d queued_attachments=%d",
                    item.reference,
                    document.document_type,
                    len(document.sections),
                    len(queue),
                )
                if document.document_type == "pdf":
                    summary["pdfs"] += 1
                else:
                    summary["pages"] += 1
                fingerprint = digest(
                    {
                        "document": document.fingerprint,
                        "extractor": extractor_signature,
                        "chunker": self.chunker.signature,
                        "embedder": self.embedder.signature,
                    }
                )
                if self.repository.current_fingerprint(document.id, collection) == fingerprint:
                    summary["unchanged"] += 1
                    processed.add(document.id)
                    log_item("unchanged", item, item_started)
                    return
                stage = "chunk"
                chunks = self.chunker.chunk(document)
                if not chunks:
                    raise ValueError("No indexable text extracted; previous version retained")
                stage = "embed"
                logger.info("Embedding reference=%s chunks=%d", item.reference, len(chunks))
                vectors = self.embedder.embed_documents(
                    ["\n".join([document.title, *c.heading_path, c.text]) for c in chunks]
                )
                validate_vectors(vectors, len(chunks), self.embedder.dimensions)
                stage = "publish"
                published = self.repository.publish(
                    document,
                    raw,
                    collection,
                    fingerprint,
                    chunks,
                    vectors,
                    self.embedder.signature,
                    processing=summary["processing"],
                )
                summary["published" if published else "unchanged"] += 1
                processed.add(document.id)
                log_item("published" if published else "unchanged", item, item_started)
            except SourceRequestsStopped:
                raise
            except Exception as exc:  # noqa: BLE001 -- isolate failures between source items
                summary["errors"].append({"reference": item.reference, "error": str(exc)})
                logger.error(
                    "Item failed reference=%s stage=%s error=%s", item.reference, stage, exc
                )

        def log_item(outcome, item, item_started):
            logger.info(
                "Item %s reference=%s published=%d unchanged=%d errors=%d item_elapsed=%.1fs run_elapsed=%.1fs",
                outcome,
                item.reference,
                summary["published"],
                summary["unchanged"],
                len(summary["errors"]),
                time.monotonic() - item_started,
                time.monotonic() - started,
            )

        def log_finished():
            logger.info(
                "Ingestion finished run=%s status=%s coverage=%s published=%d unchanged=%d errors=%d retired=%d elapsed=%.1fs",
                run_id,
                summary["status"],
                summary["coverage"],
                summary["published"],
                summary["unchanged"],
                len(summary["errors"]),
                summary["retirement"]["count"],
                time.monotonic() - started,
            )

        try:
            logger.info("Discovering documents source=%s", name)
            for item in connector.discover(config):
                queue = deque([item])
                while queue:
                    process(queue.popleft(), queue)
            verify = getattr(connector, "verify_discovery", None)
            if config.get("organisations") and callable(verify):
                verify()
            elif config.get("reconcile"):
                raise ValueError("Reconciliation requires verified discovery")
        except SourceRequestsStopped as exc:
            summary["aborted"] = True
            summary["errors"].append({"reference": "source", "error": str(exc)})
            logger.error("Ingestion aborted run=%s error=%s", run_id, exc)
        except Exception as exc:  # noqa: BLE001 -- record discovery failures in the run summary
            summary["errors"].append({"reference": "discovery", "error": str(exc)})
            logger.error("Discovery or verification failed run=%s error=%s", run_id, exc)

        summary["discovery"] = dict(getattr(connector, "discovery_report", {"complete": False}))
        summary["exclusions"] = list(getattr(connector, "exclusions", []))
        summary["ocr_pages"] = getattr(connector, "statistics", {}).get("ocr_pages", 0)
        if hasattr(connector, "request_report"):
            summary["http"] = connector.request_report
        complete = summary["discovery"].get("complete") and not summary["errors"]
        summary["coverage"] = "complete" if complete and not summary["exclusions"] else "partial"
        summary["status"] = "failed" if summary["errors"] else "succeeded"
        if config.get("reconcile"):
            eligible = complete and summary["discovery"].get("verified")
            summary["retirement"]["reason"] = "incomplete discovery or ingestion failures"
            if eligible:
                try:
                    logger.info(
                        "Reconciling collection=%s processed_documents=%d",
                        collection,
                        len(processed),
                    )
                    self.repository.reconcile_collection(
                        collection, name, processed, run_id, summary
                    )
                    log_finished()
                    return summary
                except Exception as exc:  # noqa: BLE001 -- rollback retains all memberships
                    summary["retirement"] = {
                        "performed": False,
                        "reason": "reconciliation failed",
                        "count": 0,
                        "ids": [],
                    }
                    summary["errors"].append({"reference": "reconciliation", "error": str(exc)})
                    summary["status"] = "failed"
                    summary["coverage"] = "partial"
                    logger.error("Reconciliation failed run=%s error=%s", run_id, exc)
            else:
                logger.warning(
                    "Retirement skipped run=%s reason=%s", run_id, summary["retirement"]["reason"]
                )
        self.repository.finish_run(run_id, summary)
        log_finished()
        return summary
