from datetime import UTC, datetime, timedelta

import dagster as dg
from dagster_duckdb import DuckDBResource

from plex_ingest.defs.resources.plex_watch_history import PlexWatchHistoryResource

_Row = tuple[str, str, int, list[str], float | None, str, datetime]

TABLE_NAME = "stg_watch_history"

# How far back to pull/resolve watch history each run. Matches the read-side
# relevance window (see docs/diversity-recommender.md in plex-rag) — as long
# as this pipeline runs more often than the window is wide, every watched
# title gets captured (and cached — see the partition-sync sensor) before it
# would otherwise age out of view. Widening this only affects how much gets
# re-resolved per run, not what's already cached: once an imdb_id has a
# partition, the sync sensor never re-embeds it regardless of whether a later
# run's window still includes it.
_WINDOW_DAYS = 60

_CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    imdb_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    year INTEGER NOT NULL,
    genres VARCHAR[] NOT NULL,
    imdb_rating DOUBLE,
    summary VARCHAR NOT NULL,
    last_viewed_at TIMESTAMP NOT NULL
)
"""

# Upsert, not overwrite: a row already in the table for an imdb_id must survive a run
# whose Plex fetch window no longer includes it (see _WINDOW_DAYS) -- the
# watch_history_embeddings/watch_history_qdrant_collection assets need this table to
# still have the row for any imdb_id that ever got a partition, even long after it aged
# out of the fetch window, since partitions are add-only (see the sync sensor) and never
# force a re-embed. The WHERE guards against an out-of-order/stale re-resolution
# clobbering a newer last_viewed_at with an older one.
_UPSERT_SQL = f"""
INSERT INTO {TABLE_NAME} VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (imdb_id) DO UPDATE SET
    title = excluded.title,
    year = excluded.year,
    genres = excluded.genres,
    imdb_rating = excluded.imdb_rating,
    summary = excluded.summary,
    last_viewed_at = excluded.last_viewed_at
WHERE excluded.last_viewed_at > {TABLE_NAME}.last_viewed_at
"""  # noqa: S608 — TABLE_NAME is a module constant, not user input


@dg.asset(group_name="watch_history", kinds={"plex", "duckdb"})
def stg_watch_history(
    context: dg.AssetExecutionContext,
    plex_watch_history: PlexWatchHistoryResource,
    duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    """Fetches and resolves the last `_WINDOW_DAYS` of Plex watch history on every run
    — deliberately unpartitioned, mirroring `raw_movies`'s "cheap enough to refetch"
    philosophy for the *fetch* step, bounded to a small rolling window rather than
    all-time history (see `_WINDOW_DAYS`) to keep each run's Discover API cost small.

    Unlike `raw_movies`, this is an **upsert into an accumulating table, not a full
    overwrite** — a row already present for an imdb_id must survive a later run whose
    fetch window no longer includes it. Partitions here are add-only (see the sync
    sensor) precisely so a cached embedding never needs to be recomputed just because
    its title aged out of the window; that only holds if this table keeps the row
    around for `watch_history_embeddings`/`watch_history_qdrant_collection` to still
    read. See docs/pipeline-design.md's "Watch-history diversity-recommender pipeline".

    Resolution (title+date -> imdb_id/genres/summary/rating, via
    `PlexWatchHistoryResource`) happens here rather than in a downstream SQL
    transform, since it needs a live Plex Discover API call per title — not
    SQL-shaped, unlike `stg_movies`'s dbt-based `imdb_id` resolution from raw
    `guids`. Whether a dbt layer belongs on top of this table for further
    transformation is still open — see docs/pipeline-design.md.

    Deduplicates within this run's fetch by `imdb_id`, keeping the most recent
    `viewed_at` — Plex history contains one row per playback, so a rewatched movie
    appears multiple times; the upsert's `WHERE` clause applies the same
    keep-the-newest rule across runs. A title that fails to resolve (see
    `PlexWatchHistoryResource.resolve`) is logged and skipped, not fatal to the run."""
    cutoff = datetime.now(UTC) - timedelta(days=_WINDOW_DAYS)
    history = [
        entry
        for entry in plex_watch_history.fetch_history()
        if entry.viewed_at >= cutoff.replace(tzinfo=None)
    ]

    latest_by_imdb_id: dict[str, _Row] = {}
    skipped = 0
    for entry in history:
        resolved = plex_watch_history.resolve(
            entry.title, entry.originally_available_at
        )
        if resolved is None:
            context.log.warning(
                f"Could not resolve watch-history entry "
                f"{entry.title!r} ({entry.originally_available_at}) — skipping"
            )
            skipped += 1
            continue

        existing = latest_by_imdb_id.get(resolved.imdb_id)
        if existing is not None and existing[6] >= entry.viewed_at:
            continue
        latest_by_imdb_id[resolved.imdb_id] = (
            resolved.imdb_id,
            resolved.title,
            resolved.year,
            resolved.genres,
            resolved.imdb_rating,
            resolved.summary,
            entry.viewed_at,
        )

    with duckdb.get_connection() as conn:
        conn.execute(_CREATE_TABLE_SQL)
        if latest_by_imdb_id:
            conn.executemany(_UPSERT_SQL, list(latest_by_imdb_id.values()))
        count_row = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()  # noqa: S608
        if count_row is None:
            raise RuntimeError(f"COUNT(*) on {TABLE_NAME!r} returned no row")
        total_row_count = count_row[0]

    context.log.info(
        f"Upserted {len(latest_by_imdb_id)} row(s) from this run's window into "
        f"{TABLE_NAME!r} ({total_row_count} total) in {duckdb.database} "
        f"({skipped} unresolved entr{'y' if skipped == 1 else 'ies'} skipped)"
    )

    return dg.MaterializeResult(
        metadata={
            "table": TABLE_NAME,
            "path": duckdb.database,
            "upserted_row_count": len(latest_by_imdb_id),
            "total_row_count": total_row_count,
            "skipped": skipped,
        }
    )


defs = dg.Definitions(assets=[stg_watch_history])
