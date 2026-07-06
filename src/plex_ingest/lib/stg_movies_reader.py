"""Shared reader for `stg_movies`, the dbt staging table that synopsis/enrichment/
embeddings/qdrant_collection all query. Defined outside `defs/` per CLAUDE.md: this is
plain Python logic (a query + its result shape), not a Dagster construct — it takes a
raw `duckdb` connection, not a `DuckDBResource`. Centralizing the column list here means
a `stg_movies` schema change is a one-place edit instead of four independent SQL
strings.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

_COLUMNS = "imdb_id, title, year, genres, imdb_rating, content_rating, thumb_url"


@dataclass(frozen=True)
class MovieCatalogRow:
    imdb_id: str
    title: str
    year: int
    genres: list[str]
    imdb_rating: float | None
    content_rating: str | None
    thumb_url: str | None


def fetch_movie(conn: DuckDBPyConnection, imdb_id: str) -> MovieCatalogRow:
    """The `stg_movies` row for `imdb_id`. Raises if it's missing — every asset that
    reads this table by partition key needs the same "must exist" guarantee."""
    row = conn.execute(
        f"SELECT {_COLUMNS} FROM stg_movies WHERE imdb_id = ?",  # noqa: S608 — _COLUMNS is a module constant, not user input
        [imdb_id],
    ).fetchone()
    if row is None:
        msg = f"No stg_movies row for imdb_id={imdb_id!r}"
        raise ValueError(msg)
    return MovieCatalogRow(*row)


def fetch_all_movies(conn: DuckDBPyConnection) -> dict[str, MovieCatalogRow]:
    """Every `stg_movies` row, keyed by imdb_id — used by qdrant_collection's full
    rebuild, which needs the whole catalog rather than one imdb_id at a time."""
    return {
        row[0]: MovieCatalogRow(*row)
        for row in conn.execute(f"SELECT {_COLUMNS} FROM stg_movies").fetchall()  # noqa: S608
    }
