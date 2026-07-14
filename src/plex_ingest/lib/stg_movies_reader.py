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

from plex_ingest.lib.media_source import HdrFormat, StreamingSource, VideoResolution
from plex_ingest.lib.streaming_runtime_store import (
    CREATE_TABLE_SQL as _CREATE_STREAMING_RUNTIME_TABLE_SQL,
)
from plex_ingest.lib.streaming_runtime_store import (
    TABLE_NAME as _STREAMING_RUNTIME_TABLE_NAME,
)

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

# LEFT JOINed against stg_streaming_runtime for runtime_minutes: m.runtime_minutes is
# NULL for streaming-platform placeholder rows (see stg_movies.sql), and the real
# value for those, if resolved, lives only in stg_streaming_runtime (see
# defs/assets/streaming_runtime.py) — COALESCE picks whichever side actually has a
# value, and a real download's row simply has no stg_streaming_runtime match, so
# COALESCE just passes m.runtime_minutes through unchanged.
_COLUMNS = (
    "m.imdb_id, m.title, m.year, m.genres, m.imdb_rating, m.content_rating, "
    "m.description, m.thumb_url, m.video_resolution, m.hdr_formats, "
    "m.source_platform, "
    "COALESCE(m.runtime_minutes, sr.runtime_minutes) AS runtime_minutes"
)
_FROM = (
    f"FROM stg_movies m LEFT JOIN {_STREAMING_RUNTIME_TABLE_NAME} sr USING (imdb_id)"
)


@dataclass(frozen=True)
class MovieCatalogRow:
    imdb_id: str
    title: str
    year: int
    genres: list[str]
    imdb_rating: float | None
    content_rating: str | None
    description: str | None
    thumb_url: str | None
    video_resolution: VideoResolution | None
    hdr_formats: list[HdrFormat]
    source_platform: StreamingSource | None
    runtime_minutes: int | None


def _row_to_movie(imdb_id: str, row: tuple[Any, ...]) -> MovieCatalogRow:
    (
        title,
        year,
        genres,
        imdb_rating,
        content_rating,
        description,
        thumb_url,
        video_resolution_raw,
        hdr_formats_raw,
        source_platform_raw,
        runtime_minutes,
    ) = row[1:]
    try:
        video_resolution = (
            VideoResolution(video_resolution_raw) if video_resolution_raw else None
        )
        hdr_formats = [HdrFormat(raw) for raw in hdr_formats_raw]
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
        description=description,
        thumb_url=thumb_url,
        video_resolution=video_resolution,
        hdr_formats=hdr_formats,
        source_platform=source_platform,
        runtime_minutes=runtime_minutes,
    )


def _ensure_streaming_runtime_table(conn: DuckDBPyConnection) -> None:
    """stg_movies_reader can run before `streaming_runtime` ever has (e.g. a fresh
    DuckDB, or synopsis/enrichment/embeddings racing ahead of it in the pipeline) —
    the JOIN below needs the table to exist even with zero rows in it. Idempotent DDL,
    cheap to run on every read."""
    conn.execute(_CREATE_STREAMING_RUNTIME_TABLE_SQL)


def fetch_movie(conn: DuckDBPyConnection, imdb_id: str) -> MovieCatalogRow:
    """The `stg_movies` row for `imdb_id`. Raises if it's missing — every asset that
    reads this table by partition key needs the same "must exist" guarantee."""
    _ensure_streaming_runtime_table(conn)
    row = conn.execute(
        f"SELECT {_COLUMNS} {_FROM} WHERE m.imdb_id = ?",  # noqa: S608 — _COLUMNS/_FROM are module constants, not user input
        [imdb_id],
    ).fetchone()
    if row is None:
        msg = f"No stg_movies row for imdb_id={imdb_id!r}"
        raise ValueError(msg)
    return _row_to_movie(imdb_id, row)


def fetch_all_movies(conn: DuckDBPyConnection) -> dict[str, MovieCatalogRow]:
    """Every `stg_movies` row, keyed by imdb_id — used by qdrant_collection's full
    rebuild, which needs the whole catalog rather than one imdb_id at a time."""
    _ensure_streaming_runtime_table(conn)
    return {
        row[0]: _row_to_movie(row[0], row)
        for row in conn.execute(f"SELECT {_COLUMNS} {_FROM}").fetchall()  # noqa: S608
    }
