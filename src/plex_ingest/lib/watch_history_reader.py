"""Shared reader for `stg_watch_history`, queried by the watch-history
embeddings/qdrant_collection assets and the partition-sync sensor. Defined
outside `defs/` per CLAUDE.md: plain Python logic (a query + its result
shape), not a Dagster construct — takes a raw `duckdb` connection, not a
`DuckDBResource`. Centralizing the column list here means a schema change is
a one-place edit. Mirrors `stg_movies_reader.py`; unlike `stg_movies`,
`stg_watch_history` is written directly by a Python asset rather than a dbt
model — see docs/pipeline-design.md's "Watch-history diversity-recommender
pipeline" for why, and the still-open question of whether a dbt layer gets
introduced later.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

_COLUMNS = "tmdb_id, imdb_id, title, year, genres, imdb_rating, summary, last_viewed_at"


@dataclass(frozen=True)
class WatchHistoryRow:
    tmdb_id: str
    imdb_id: str
    title: str
    year: int
    genres: list[str]
    imdb_rating: float | None
    summary: str
    last_viewed_at: datetime


def _row_to_watch_history(tmdb_id: str, row: tuple[Any, ...]) -> WatchHistoryRow:
    imdb_id, title, year, genres, imdb_rating, summary, last_viewed_at = row[1:]
    return WatchHistoryRow(
        tmdb_id=tmdb_id,
        imdb_id=imdb_id,
        title=title,
        year=year,
        genres=genres,
        imdb_rating=imdb_rating,
        summary=summary,
        last_viewed_at=last_viewed_at,
    )


def fetch_watch_history_movie(
    conn: DuckDBPyConnection, tmdb_id: str
) -> WatchHistoryRow:
    """The `stg_watch_history` row for `tmdb_id`. Raises if it's missing — every asset
    that reads this table by partition key needs the same "must exist" guarantee."""
    row = conn.execute(
        f"SELECT {_COLUMNS} FROM stg_watch_history WHERE tmdb_id = ?",  # noqa: S608 — _COLUMNS is a module constant, not user input
        [tmdb_id],
    ).fetchone()
    if row is None:
        msg = f"No stg_watch_history row for tmdb_id={tmdb_id!r}"
        raise ValueError(msg)
    return _row_to_watch_history(tmdb_id, row)


def fetch_all_watch_history(conn: DuckDBPyConnection) -> dict[str, WatchHistoryRow]:
    """Every `stg_watch_history` row, keyed by tmdb_id — unbounded, since
    `stg_watch_history` itself is an upsert never pruned by age (see that asset's
    docstring). `watch_history_qdrant_collection` is where the read-side relevance
    window actually gets enforced, by filtering the tmdb_ids it builds points for —
    not by filtering the rows read here, which would make a since-aged-out embedding
    file look indistinguishable from a genuine partition-sync bug (no row at all)."""
    return {
        row[0]: _row_to_watch_history(row[0], row)
        for row in conn.execute(f"SELECT {_COLUMNS} FROM stg_watch_history").fetchall()  # noqa: S608
    }
