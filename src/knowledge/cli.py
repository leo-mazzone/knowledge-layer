"""Command-line entry point for schema setup, collection ingestion, and serving."""

import argparse
import json
import logging
from pathlib import Path

from knowledge.config import Settings, build_components
from knowledge.pipeline import IngestionPipeline
from knowledge.processing import SectionChunker
from knowledge.registry import SourceRegistry


def _build_parser() -> argparse.ArgumentParser:
    """Define the public commands and their command-specific options."""
    parser = argparse.ArgumentParser(description="Shared knowledge service")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init-db", help="Initialize the document schema")
    ingest = commands.add_parser("ingest", help="Ingest or refresh a configured collection")
    ingest.add_argument("manifest", type=Path)
    ingest.add_argument(
        "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO"
    )
    commands.add_parser("serve", help="Serve the two read-only MCP tools")
    return parser


def _configure_ingestion_logging(log_level: str) -> None:
    """Send progress to stderr so stdout remains a machine-readable JSON result."""
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    # Per-request HTTP messages obscure the pipeline's document-level progress.
    logging.getLogger("httpx").setLevel(logging.WARNING)


def _initialize_database(settings: Settings) -> None:
    """Create the document schema and release the shared component client."""
    client, repository, _ = build_components(settings)
    try:
        repository.initialize()
        print("Knowledge database initialized")
    finally:
        client.close()


def _ingest_collection(settings: Settings, manifest_path: Path) -> None:
    """Run the manifest's connector and report the ingestion result as JSON."""
    client, repository, embedder = build_components(settings)
    try:
        manifest = json.loads(manifest_path.read_text())
        adapter = SourceRegistry().create(manifest["connector"], client)
        pipeline = IngestionPipeline(repository, embedder, SectionChunker())
        result = pipeline.run(
            manifest["connector"],
            adapter.connector,
            adapter.extractor,
            adapter.extractor_signature,
            manifest["config"],
            manifest["collection"],
        )
        # Emit the summary even on partial failure, then signal failure to scripts.
        print(json.dumps(result, indent=2), flush=True)
        if result["errors"]:
            raise SystemExit(1)
    finally:
        # Manifest, connector, and pipeline failures must also release the client.
        client.close()


def main() -> None:
    """Parse arguments, load settings, and dispatch to the selected command."""
    args = _build_parser().parse_args()
    if args.command == "ingest":
        _configure_ingestion_logging(args.log_level)
        logging.getLogger(__name__).info("Starting ingestion manifest=%s", args.manifest)

    settings = Settings.from_env()
    if args.command == "init-db":
        _initialize_database(settings)
    elif args.command == "ingest":
        _ingest_collection(settings, args.manifest)
    elif args.command == "serve":
        # The server owns its components and lifecycle; CLI commands need no server import.
        from knowledge.server import serve

        serve(settings)


if __name__ == "__main__":
    main()
