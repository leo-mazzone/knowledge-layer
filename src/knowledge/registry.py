"""Register built-ins or install packages exposing knowledge.sources entry points.

An entry point resolves to a factory(client) returning a SourceAdapter.
The shared pipeline does not need to change when a source is added.
"""

from collections.abc import Callable
from dataclasses import dataclass
from importlib.metadata import entry_points

from knowledge.connectors.govuk import GovUkConnector, GovUkExtractor
from knowledge.ports import Connector, Extractor


@dataclass(frozen=True)
class SourceAdapter:
    connector: Connector
    extractor: Extractor
    extractor_signature: str


class SourceRegistry:
    def __init__(self):
        self.factories: dict[str, Callable] = {}
        self.register(
            "govuk",
            lambda client: SourceAdapter(
                GovUkConnector(client),
                GovUkExtractor(),
                GovUkExtractor.signature,
            ),
        )
        for entry in entry_points(group="knowledge.sources"):
            self.register(entry.name, entry.load())

    def register(self, name: str, factory: Callable):
        if name in self.factories:
            raise ValueError(f"Duplicate source connector: {name}")
        self.factories[name] = factory

    def create(self, name: str, client) -> SourceAdapter:
        try:
            factory = self.factories[name]
        except KeyError:
            raise ValueError(
                f"Unknown connector {name!r}; available: {sorted(self.factories)}"
            ) from None
        return factory(client)
