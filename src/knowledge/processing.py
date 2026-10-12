import math
from collections.abc import Sequence
from dataclasses import dataclass

from knowledge.models import Chunk, Document


@dataclass(frozen=True)
class SectionChunker:
    """Preserve section boundaries; split long sections with word overlap."""

    max_words: int = 350
    overlap_words: int = 40
    max_chars: int = 6000

    def __post_init__(self):
        if (
            self.max_words < 1
            or not 0 <= self.overlap_words < self.max_words
            or self.max_chars < 100
        ):
            raise ValueError("Require max_words > overlap_words >= 0")

    @property
    def signature(self) -> str:
        return f"section-words-v1:{self.max_words}:{self.overlap_words}:{self.max_chars}"

    def chunk(self, document: Document) -> list[Chunk]:
        chunks = []
        for section in document.sections:
            words = section.text.split()
            start = 0
            while start < len(words):
                stop = min(start + self.max_words, len(words))
                text = " ".join(words[start:stop])
                for offset in range(0, len(text), self.max_chars):
                    chunks.append(
                        Chunk(
                            len(chunks),
                            text[offset : offset + self.max_chars],
                            section.heading_path,
                            section.locator,
                        )
                    )
                if stop == len(words):
                    break
                start = stop - self.overlap_words
        return chunks


def validate_vectors(vectors: Sequence[Sequence[float]], count: int, dimensions: int) -> None:
    if len(vectors) != count:
        raise ValueError("Embedding provider returned the wrong number of vectors")
    for vector in vectors:
        if len(vector) != dimensions or not all(math.isfinite(x) for x in vector):
            raise ValueError("Embedding vector has invalid dimensions or non-finite values")
        if not any(x != 0 for x in vector):
            raise ValueError("Embedding provider returned a zero vector")
