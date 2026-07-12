from datetime import UTC, datetime, timedelta
from pathlib import Path

import dagster as dg
from dagster_duckdb import DuckDBResource

from plex_ingest.defs.resources.partition_json_io_manager import PLEX_INGEST_DATA_DIR
from plex_ingest.defs.resources.qdrant import QdrantResource
from plex_ingest.lib.vector_store_contract import build_watch_history_points
from plex_ingest.lib.watch_history_reader import fetch_all_watch_history

# The read-side relevance window: how far back a watched title still counts toward
# the recency-weighted aversion vector (see plex-rag's docs/diversity-recommender.md).
# Independently tunable from stg_watch_history's own fetch window even though both are
# 60 days today -- one bounds Discover API cost per run, this one bounds what's
# queryable as "recent" -- see docs/pipeline-design.md.
_RELEVANCE_WINDOW_DAYS = 60


@dg.asset(
    automation_condition=dg.AutomationCondition.eager(),
    group_name="watch_history",
    kinds={"qdrant"},
    deps=["stg_watch_history", "watch_history_embeddings"],
)
def watch_history_qdrant_collection(
    context: dg.AssetExecutionContext,
    watch_history_qdrant: QdrantResource,
    duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    """Full rebuild of the `watch_history` Qdrant collection from every
    embeddings/watch_history/{imdb_id}.json currently on disk, joined against
    `stg_watch_history` rows filtered to the last `_RELEVANCE_WINDOW_DAYS` --
    mirrors `qdrant_collection`'s "delete+reinsert is cheap, so the simplest correct
    thing is also the self-correcting one" philosophy (see that asset's docstring).

    Two things make this rebuild self-correcting on different axes than
    `qdrant_collection`'s: (1) an imdb_id whose embedding exists but whose
    `stg_watch_history` row has aged past the window is simply excluded from this
    run's points -- no partition removal, no file deletion, the cached embedding
    stays on disk untouched for a future run where a rewatch brings it back into the
    window; (2) `deps=` on both `stg_watch_history` and `watch_history_embeddings`
    means `eager()` reacts to a `stg_watch_history`-only change too (e.g. a rewatch
    updating `last_viewed_at` with no new embedding needed), keeping recency
    weighting fresh on every run without needing the embeddings cache to change.

    `sync_watch_history_partitions` still requests this asset directly whenever a
    new embedding gets backfilled, the same cold-start gap `sync_imdb_id_partitions`
    covers for `qdrant_collection` -- see that sensor's docstring."""
    cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(
        days=_RELEVANCE_WINDOW_DAYS
    )
    with duckdb.get_connection() as conn:
        watch_history = fetch_all_watch_history(conn)
    in_window_ids = {
        imdb_id
        for imdb_id, row in watch_history.items()
        if row.last_viewed_at >= cutoff
    }

    embeddings_dir = Path(PLEX_INGEST_DATA_DIR) / "embeddings" / "watch_history"
    points = build_watch_history_points(watch_history, embeddings_dir, in_window_ids)

    watch_history_qdrant.recreate_collection()
    if points:
        watch_history_qdrant.upsert_points(points)

    point_count = watch_history_qdrant.point_count()
    context.log.info(
        f"Rebuilt {watch_history_qdrant.collection!r}: {len(points)} point(s) from "
        f"{embeddings_dir} (window: last {_RELEVANCE_WINDOW_DAYS} days, "
        f"{len(in_window_ids)} of {len(watch_history)} cached movies in window)"
    )

    return dg.MaterializeResult(
        metadata={
            "collection": watch_history_qdrant.collection,
            "point_count": point_count,
            "in_window_movies": len(in_window_ids),
            "total_cached_movies": len(watch_history),
        }
    )


defs = dg.Definitions(assets=[watch_history_qdrant_collection])
