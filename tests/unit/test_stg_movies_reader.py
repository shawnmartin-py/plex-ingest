from pathlib import Path

from dagster_duckdb import DuckDBResource
from duckdb import DuckDBPyConnection

from plex_ingest.lib.stg_movies_reader import fetch_all_movies, fetch_movie
from plex_ingest.lib.streaming_runtime_store import (
    CREATE_TABLE_SQL as _CREATE_STREAMING_RUNTIME_TABLE_SQL,
)

_CREATE_STG_MOVIES_SQL = """
CREATE TABLE stg_movies (
    tmdb_id VARCHAR,
    imdb_id VARCHAR,
    title VARCHAR,
    year INTEGER,
    genres VARCHAR[],
    imdb_rating DOUBLE,
    content_rating VARCHAR,
    description VARCHAR,
    thumb_url VARCHAR,
    video_resolution VARCHAR,
    hdr_formats VARCHAR[],
    source_platform VARCHAR,
    runtime_minutes INTEGER
)
"""

_INSERT_STG_MOVIES_SQL = (
    "INSERT INTO stg_movies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"  # noqa: S608, E501
)


def _duckdb(tmp_path: Path) -> DuckDBResource:
    return DuckDBResource(database=str(tmp_path / "test.duckdb"))


def _insert_movie(
    conn: DuckDBPyConnection,
    tmdb_id: str,
    *,
    imdb_id: str = "tt0001",
    source_platform: str | None = None,
    runtime_minutes: int | None = None,
) -> None:
    conn.execute(
        _INSERT_STG_MOVIES_SQL,
        [
            tmdb_id,
            imdb_id,
            "Test Film",
            2020,
            ["Drama"],
            7.5,
            "PG-13",
            "A great film.",
            "http://example.com/thumb.jpg",
            None,
            [],
            source_platform,
            runtime_minutes,
        ],
    )


def test_fetch_all_movies_works_when_streaming_runtime_table_does_not_exist_yet(
    tmp_path: Path,
) -> None:
    """stg_movies_reader can run before the streaming_runtime asset ever has (a fresh
    DuckDB, or synopsis/enrichment/embeddings racing ahead of it) — the join must not
    blow up just because stg_streaming_runtime has never been created."""
    duckdb_resource = _duckdb(tmp_path)
    with duckdb_resource.get_connection() as conn:
        conn.execute(_CREATE_STG_MOVIES_SQL)
        _insert_movie(conn, "101", runtime_minutes=104)

        movies = fetch_all_movies(conn)

    assert movies["101"].runtime_minutes == 104


def test_fetch_all_movies_uses_stg_movies_runtime_for_a_real_download(
    tmp_path: Path,
) -> None:
    duckdb_resource = _duckdb(tmp_path)
    with duckdb_resource.get_connection() as conn:
        conn.execute(_CREATE_STG_MOVIES_SQL)
        _insert_movie(conn, "101", runtime_minutes=104)

        movies = fetch_all_movies(conn)

    assert movies["101"].runtime_minutes == 104


def test_fetch_all_movies_keyed_by_tmdb_id_with_imdb_id_as_attribute(
    tmp_path: Path,
) -> None:
    duckdb_resource = _duckdb(tmp_path)
    with duckdb_resource.get_connection() as conn:
        conn.execute(_CREATE_STG_MOVIES_SQL)
        _insert_movie(conn, "101", imdb_id="tt0001")

        movies = fetch_all_movies(conn)

    assert movies["101"].tmdb_id == "101"
    assert movies["101"].imdb_id == "tt0001"


def test_fetch_all_movies_coalesces_streaming_runtime_for_a_placeholder(
    tmp_path: Path,
) -> None:
    """stg_movies.runtime_minutes is NULL for a streaming placeholder (source_platform
    set) — the real value, if OMDb resolved one, only lives in stg_streaming_runtime,
    which stays keyed by imdb_id (it caches an imdb-keyed API — see
    defs/assets/streaming_runtime.py)."""
    duckdb_resource = _duckdb(tmp_path)
    with duckdb_resource.get_connection() as conn:
        conn.execute(_CREATE_STG_MOVIES_SQL)
        _insert_movie(
            conn,
            "101",
            imdb_id="tt0001",
            source_platform="Netflix",
            runtime_minutes=None,
        )
        conn.execute(_CREATE_STREAMING_RUNTIME_TABLE_SQL)
        conn.execute(
            "INSERT INTO stg_streaming_runtime VALUES (?, ?, ?)",
            ["tt0001", 96, "2026-07-14 00:00:00"],
        )

        movies = fetch_all_movies(conn)

    assert movies["101"].runtime_minutes == 96
    assert movies["101"].source_platform is not None


def test_fetch_all_movies_null_runtime_for_an_unresolved_placeholder(
    tmp_path: Path,
) -> None:
    duckdb_resource = _duckdb(tmp_path)
    with duckdb_resource.get_connection() as conn:
        conn.execute(_CREATE_STG_MOVIES_SQL)
        _insert_movie(conn, "101", source_platform="Netflix", runtime_minutes=None)

        movies = fetch_all_movies(conn)

    assert movies["101"].runtime_minutes is None


def test_fetch_movie_coalesces_streaming_runtime_for_a_placeholder(
    tmp_path: Path,
) -> None:
    duckdb_resource = _duckdb(tmp_path)
    with duckdb_resource.get_connection() as conn:
        conn.execute(_CREATE_STG_MOVIES_SQL)
        _insert_movie(
            conn,
            "101",
            imdb_id="tt0001",
            source_platform="Disney+",
            runtime_minutes=None,
        )
        conn.execute(_CREATE_STREAMING_RUNTIME_TABLE_SQL)
        conn.execute(
            "INSERT INTO stg_streaming_runtime VALUES (?, ?, ?)",
            ["tt0001", 121, "2026-07-14 00:00:00"],
        )

        movie = fetch_movie(conn, "101")

    assert movie.runtime_minutes == 121
