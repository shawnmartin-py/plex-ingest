import dagster as dg
from dagster_duckdb import DuckDBResource

from plex_ingest.defs.resources.plex import PlexResource

TABLE_NAME = "raw_movies"

_CREATE_TABLE_SQL = f"""
CREATE OR REPLACE TABLE {TABLE_NAME} (
    rating_key BIGINT PRIMARY KEY,
    title VARCHAR NOT NULL,
    year INTEGER,
    content_rating VARCHAR,
    description VARCHAR,
    thumb_url VARCHAR,
    guids VARCHAR[],
    genres VARCHAR[],
    imdb_rating DOUBLE,
    view_count BIGINT NOT NULL,
    video_resolution VARCHAR,
    duration_ms BIGINT,
    file_path VARCHAR,
    hdr_formats VARCHAR[] NOT NULL,
    synced_at TIMESTAMP NOT NULL
)
"""

_INSERT_SQL = (
    f"INSERT INTO {TABLE_NAME} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"  # noqa: S608 — TABLE_NAME is a module constant, not user input
)


@dg.asset(group_name="raw", kinds={"plex", "duckdb"})
def raw_movies(
    context: dg.AssetExecutionContext, plex: PlexResource, duckdb: DuckDBResource
) -> dg.MaterializeResult:
    """Full overwrite of the Plex movie library into DuckDB on every run. The library is
    small enough (~3s end to end) that a full re-fetch is cheaper and simpler than
    incremental sync — see docs/pipeline-design.md for the larger partitioning
    discussion (this asset is deliberately unpartitioned)."""
    rows = plex.fetch_raw_movies()

    with duckdb.get_connection() as conn:
        conn.execute(_CREATE_TABLE_SQL)
        conn.executemany(
            _INSERT_SQL,
            [
                (
                    row["rating_key"],
                    row["title"],
                    row["year"],
                    row["content_rating"],
                    row["description"],
                    row["thumb_url"],
                    row["guids"],
                    row["genres"],
                    row["imdb_rating"],
                    row["view_count"],
                    row["video_resolution"],
                    row["duration_ms"],
                    row["file_path"],
                    row["hdr_formats"],
                    row["synced_at"],
                )
                for row in rows
            ],
        )
        count_row = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()  # noqa: S608
        if count_row is None:
            raise RuntimeError(f"COUNT(*) on {TABLE_NAME!r} returned no row")
        row_count = count_row[0]

    context.log.info(f"Wrote {row_count} row(s) to {TABLE_NAME!r} in {duckdb.database}")

    return dg.MaterializeResult(
        metadata={
            "table": TABLE_NAME,
            "path": duckdb.database,
            "row_count": row_count,
        }
    )


defs = dg.Definitions(assets=[raw_movies])
