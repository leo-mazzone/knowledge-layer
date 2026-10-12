"""Source-neutral errors that require stopping an ingestion run."""


class SourceRequestsStopped(RuntimeError):
    """The connector has stopped requests; do not continue with another item."""
