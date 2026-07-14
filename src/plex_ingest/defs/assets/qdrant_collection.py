from pathlib import Path

import dagster as dg
from dagster_duckdb import DuckDBResource

from plex_ingest.defs.resources.partition_json_io_manager import PLEX_INGEST_DATA_DIR
from plex_ingest.defs.resources.qdrant import QdrantResource
from plex_ingest.lib.stg_movies_reader import fetch_all_movies
from plex_ingest.lib.vector_store_contract import build_points

_ONLY_EMBEDDINGS = dg.AssetSelection.assets("embeddings")

# eager(), but broadened past its own `~any_deps_in_progress()` guard: that guard only
# inspects qdrant_collection's *direct* deps (embeddings), so during a large backfill it
# still fires a full rebuild after every single embeddings partition, each one stale
# within moments as more partitions land — a real, not hypothetical, problem, since
# sync_imdb_id_partitions drives synopsis/enrichment/embeddings across potentially
# hundreds of imdb_id partitions at very different pipeline stages simultaneously.
# synopsis/enrichment are added as structural `deps=` purely so any_deps_in_progress()
# can see them (they're not read by this asset — embeddings_dir and duckdb are the
# actual inputs). The trigger event and the missing-dep guard stay scoped to embeddings
# only via .allow(_ONLY_EMBEDDINGS): synopsis/enrichment will almost always have some
# partition "missing" simply because the library is still being worked through, and
# that must never block a rebuild the way an actually-missing embeddings would.
_WAIT_FOR_PIPELINE_TO_SETTLE = (
    dg.AutomationCondition.any_deps_updated()
    .allow(_ONLY_EMBEDDINGS)
    .since_last_handled()
    & ~dg.AutomationCondition.any_deps_missing().allow(_ONLY_EMBEDDINGS)
    & ~dg.AutomationCondition.any_deps_in_progress()
    & ~dg.AutomationCondition.in_progress()
).with_label("eager_wait_for_pipeline_to_settle")


@dg.asset(
    automation_condition=_WAIT_FOR_PIPELINE_TO_SETTLE,
    group_name="enrichment",
    kinds={"qdrant"},
    deps=["embeddings", "synopsis", "enrichment", "streaming_runtime"],
)
def qdrant_collection(
    context: dg.AssetExecutionContext, qdrant: QdrantResource, duckdb: DuckDBResource
) -> dg.MaterializeResult:
    """Full rebuild of the Qdrant collection from every embeddings/{imdb_id}.json
    currently on disk. Deliberately unpartitioned and delete+reinsert rather than
    incremental per-movie upserts: loading already-computed data into Qdrant is cheap,
    so the simplest correct thing is also the self-correcting one — a movie pruned
    from embeddings/ (see the partition-sync sensor) is absent from the next rebuild
    *whenever a rebuild actually runs*. Gated by _WAIT_FOR_PIPELINE_TO_SETTLE (eager(),
    widened to treat synopsis/enrichment being in-progress on *any* partition as
    blocking too, not just embeddings — see that condition's comment): must never be
    allowed to drift from whatever embeddings/ currently holds, but also must not
    thrash with a redundant rebuild mid-backfill while the rest of the pipeline is
    still catching up. See docs/pipeline-design.md's "Asset boundary".

    A pure removal (no accompanying addition) has no tracked embeddings update for
    eager() to react to, since the sensor's file deletion is a direct filesystem write,
    invisible to Dagster's materialization tracking — so sync_imdb_id_partitions
    requests a run of this asset directly whenever it removes a partition, rather than
    relying on eager() alone. See docs/pipeline-design.md's "Known gaps found during
    dev-subset verification".

    Catalog fields (title/year/rating/genres/thumb_url) and `embedding_type` are read
    fresh from stg_movies and attached here, not cached in embeddings/*.json — matches
    vector-store-contract.md's payload shape exactly, and re-reading from the source of
    truth at rebuild time means a catalog-only change (e.g. a corrected title) can never
    go stale in Qdrant without needing to re-embed anything."""
    with duckdb.get_connection() as conn:
        catalog = fetch_all_movies(conn)

    embeddings_dir = Path(PLEX_INGEST_DATA_DIR) / "embeddings"
    points = build_points(catalog, embeddings_dir)

    qdrant.recreate_collection()
    if points:
        qdrant.upsert_points(points)

    point_count = qdrant.point_count()
    context.log.info(
        f"Rebuilt {qdrant.collection!r}: {len(points)} point(s) from {embeddings_dir}"
    )

    return dg.MaterializeResult(
        metadata={
            "collection": qdrant.collection,
            "point_count": point_count,
            "movies": len(list(embeddings_dir.glob("*.json"))),
        }
    )


defs = dg.Definitions(assets=[qdrant_collection])
