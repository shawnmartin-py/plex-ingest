from pathlib import Path

import dagster as dg
from dagster_duckdb import DuckDBResource

from plex_ingest.defs.resources.partition_json_io_manager import PLEX_INGEST_DATA_DIR
from plex_ingest.defs.resources.qdrant import QdrantResource
from plex_ingest.lib.stg_movies_reader import fetch_all_movies
from plex_ingest.lib.vector_store_contract import build_points


@dg.asset(
    automation_condition=dg.AutomationCondition.eager(),
    group_name="enrichment",
    kinds={"qdrant"},
    deps=["embeddings"],
)
def qdrant_collection(
    context: dg.AssetExecutionContext, qdrant: QdrantResource, duckdb: DuckDBResource
) -> dg.MaterializeResult:
    """Full rebuild of the Qdrant collection from every embeddings/{imdb_id}.json
    currently on disk. Deliberately unpartitioned and delete+reinsert rather than
    incremental per-movie upserts: loading already-computed data into Qdrant is cheap,
    so the simplest correct thing is also the self-correcting one — a movie pruned
    from embeddings/ (see the partition-sync sensor) is absent from the next rebuild
    *whenever a rebuild actually runs*. Gated by eager(): must never be allowed to
    drift from whatever embeddings/ currently holds. See docs/pipeline-design.md's
    "Asset boundary".

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
