"""Schema for `stg_streaming_runtime`, the small cache table holding OMDb-resolved
runtimes for streaming-platform placeholder movies (see docs/vector-store-contract.md's
"runtime_minutes" note). `stg_movies.runtime_minutes` is NULL for these rows — the
placeholder file's own duration is meaningless — so this table is the only source of
truth for their runtime. Defined outside `defs/` per CLAUDE.md: both the writer
(`defs/assets/streaming_runtime.py`) and the reader (`stg_movies_reader.py`, which joins
it in) need the same table name/DDL, so it lives here rather than being duplicated or
having the reader depend on the writer's asset module.

A resolved runtime never changes, so this is a fetch-once cache, not a refreshed
snapshot — the writer upserts with `ON CONFLICT DO NOTHING`, mirroring `synopsis`'s
"never re-scraped once materialized" philosophy rather than `stg_watch_history`'s
overwrite-on-newer-data upsert."""

TABLE_NAME = "stg_streaming_runtime"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    imdb_id VARCHAR PRIMARY KEY,
    runtime_minutes INTEGER,
    fetched_at TIMESTAMP NOT NULL
)
"""

UPSERT_SQL = f"""
INSERT INTO {TABLE_NAME} VALUES (?, ?, ?)
ON CONFLICT (imdb_id) DO NOTHING
"""  # noqa: S608 — TABLE_NAME is a module constant, not user input
