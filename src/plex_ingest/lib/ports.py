"""Ports for the external systems the ingest pipeline depends on.

Defined outside `defs/` per CLAUDE.md: these are plain Python contracts with no
Dagster or vendor-SDK coupling. Each `ConfigurableResource` under
`defs/resources/` holds config only and delegates to a concrete adapter (see
`lib/adapters/`) implementing the matching port here — that split is what lets
an adapter be swapped (e.g. the still-open LangChain/LlamaIndex choice in
docs/pipeline-design.md) or faked in tests without touching the
resource or any asset.
"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Protocol


class SynopsisScraper(Protocol):
    def fetch_synopsis(self, imdb_id: str, title: str, year: int) -> str | None: ...


@dataclass(frozen=True)
class SynopsisMatchResult:
    matches: bool
    reason: str


class SynopsisMatchJudge(Protocol):
    def check(self, *, title: str, year: int, synopsis: str) -> SynopsisMatchResult: ...


class EnrichmentGenerator(Protocol):
    @property
    def sections(self) -> tuple[str, ...]: ...

    def generate_section(
        self,
        *,
        title: str,
        year: int,
        genres: list[str],
        imdb_rating: float | None,
        content_rating: str | None,
        synopsis: str,
        section: str,
    ) -> str | None: ...


class EmbeddingClient(Protocol):
    def embed_query(self, text: str) -> list[float]: ...


class VectorStore(Protocol):
    def recreate_collection(self) -> None: ...

    def upsert_points(
        self, points: list[tuple[str, list[float], str, dict[str, Any]]]
    ) -> None: ...

    def point_count(self) -> int: ...


class MovieCatalog(Protocol):
    def fetch_raw_movies(self) -> list[dict[str, Any]]: ...


class RuntimeLookup(Protocol):
    def fetch_runtime_minutes(self, imdb_id: str) -> int | None: ...


@dataclass(frozen=True)
class WatchHistoryEntry:
    title: str
    originally_available_at: date
    viewed_at: datetime
    rating_key: str | None


class WatchHistorySource(Protocol):
    def fetch_history(self) -> list[WatchHistoryEntry]: ...


@dataclass(frozen=True)
class ResolvedWatchedMovie:
    imdb_id: str
    title: str
    year: int
    genres: list[str]
    imdb_rating: float | None
    summary: str


class WatchedMovieResolver(Protocol):
    def resolve(
        self, title: str, originally_available_at: date
    ) -> ResolvedWatchedMovie | None: ...
