"""Shared reader for `stg_movies`, the dbt staging table that synopsis/enrichment/
embeddings/qdrant_collection all query. Defined outside `defs/` per CLAUDE.md: this is
plain Python logic (a query + its result shape), not a Dagster construct — it takes a
raw `duckdb` connection, not a `DuckDBResource`. Centralizing the column list here means
a `stg_movies` schema change is a one-place edit instead of four independent SQL
strings.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from plex_ingest.lib.media_source import StreamingSource, VideoResolution

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

_COLUMNS = (
    "imdb_id, title, year, genres, imdb_rating, content_rating, thumb_url, "
    "video_resolution, source_platform"
)


@dataclass(frozen=True)
class MovieCatalogRow:
    imdb_id: str
    title: str
    year: int
    genres: list[str]
    imdb_rating: float | None
    content_rating: str | None
    thumb_url: str | None
    video_resolution: VideoResolution | None
    source_platform: StreamingSource | None


def _row_to_movie(imdb_id: str, row: tuple[Any, ...]) -> MovieCatalogRow:
    (
        title,
        year,
        genres,
        imdb_rating,
        content_rating,
        thumb_url,
        video_resolution_raw,
        source_platform_raw,
    ) = row[1:]
    try:
        video_resolution = (
            VideoResolution(video_resolution_raw) if video_resolution_raw else None
        )
        source_platform = (
            StreamingSource(source_platform_raw) if source_platform_raw else None
        )
    except ValueError as e:
        msg = f"stg_movies row for imdb_id={imdb_id!r} has an unrecognized value: {e}"
        raise ValueError(msg) from e
    return MovieCatalogRow(
        imdb_id=imdb_id,
        title=title,
        year=year,
        genres=genres,
        imdb_rating=imdb_rating,
        content_rating=content_rating,
        thumb_url=thumb_url,
        video_resolution=video_resolution,
        source_platform=source_platform,
    )


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
    return _row_to_movie(imdb_id, row)


def fetch_all_movies(conn: DuckDBPyConnection) -> dict[str, MovieCatalogRow]:
    """Every `stg_movies` row, keyed by imdb_id — used by qdrant_collection's full
    rebuild, which needs the whole catalog rather than one imdb_id at a time."""
    return {
        row[0]: _row_to_movie(row[0], row)
        for row in conn.execute(f"SELECT {_COLUMNS} FROM stg_movies").fetchall()  # noqa: S608
    }
