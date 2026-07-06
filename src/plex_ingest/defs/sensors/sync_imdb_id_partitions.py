import os

import dagster as dg
from dagster_duckdb import DuckDBResource

from plex_ingest.defs.partitions import imdb_id_partitions
from plex_ingest.defs.resources.partition_json_io_manager import (
    EMBEDDINGS_IO_MANAGER,
    ENRICHMENT_IO_MANAGER,
    SYNOPSIS_IO_MANAGER,
)

# Deliberate safety rail while this pipeline is being built and tested: caps how many
# imdb_ids are ever registered as partitions, regardless of how many are in stg_movies.
# Unset (or remove this check) once the pipeline is proven end to end and covered by
# tests — see docs/epics/plex-ingest-extraction/phase-2-pipeline-design.md in plex-rag.
PLEX_INGEST_PARTITION_LIMIT = os.environ.get("PLEX_INGEST_PARTITION_LIMIT")

_STAGE_IO_MANAGERS = (SYNOPSIS_IO_MANAGER, ENRICHMENT_IO_MANAGER, EMBEDDINGS_IO_MANAGER)
_STAGE_ASSET_KEYS = (
    dg.AssetKey("synopsis"),
    dg.AssetKey("enrichment"),
    dg.AssetKey("embeddings"),
)


def _delete_partition_files(imdb_id: str) -> None:
    for io_manager in _STAGE_IO_MANAGERS:
        io_manager.path_for(imdb_id).unlink(missing_ok=True)


def _missing_stage_assets(imdb_id: str) -> list[dg.AssetKey]:
    """Which of synopsis/enrichment/embeddings have never been materialized for this
    partition, checked directly against on-disk state. This is what actually triggers
    their first materialization now — synopsis/enrichment carry no automation_condition
    of their own — rather than AutomationCondition.on_missing(), whose
    missing().newly_true() event is a one-shot transient that never re-fires: a
    partition already missing at the automation-condition cursor's very first
    evaluation (evaluation_id 0) got stuck forever (see phase-2-pipeline-design.md's
    "Known gaps found during dev-subset verification", item 2, in plex-rag). Checking
    disk state on every tick sidesteps that cursor entirely."""
    return [
        asset_key
        for asset_key, io_manager in zip(
            _STAGE_ASSET_KEYS, _STAGE_IO_MANAGERS, strict=True
        )
        if not io_manager.path_for(imdb_id).exists()
    ]


def compute_desired_ids(current_ids: set[str], limit: int | None) -> set[str]:
    """The imdb_ids that should be registered as partitions, given what's currently in
    stg_movies and an optional dev-only cap. Sorted before truncating so the same subset
    is chosen deterministically across runs, rather than an arbitrary set ordering."""
    if limit is None:
        return current_ids
    return set(sorted(current_ids)[:limit])


def compute_partition_diff(
    desired_ids: set[str], registered_ids: set[str]
) -> tuple[set[str], set[str]]:
    """(new_ids, removed_ids) needed to bring the registered dynamic partition set in
    line with desired_ids."""
    new_ids = desired_ids - registered_ids
    removed_ids = registered_ids - desired_ids
    return new_ids, removed_ids


@dg.sensor(
    minimum_interval_seconds=60,
    default_status=dg.DefaultSensorStatus.RUNNING,
    asset_selection=[*_STAGE_ASSET_KEYS, dg.AssetKey("qdrant_collection")],
)
def sync_imdb_id_partitions(
    context: dg.SensorEvaluationContext, duckdb: DuckDBResource
) -> dg.SensorResult:
    """Keeps the imdb_id dynamic partition set (shared by synopsis/enrichment/
    embeddings) in sync with stg_movies, and is the sole trigger for
    synopsis/enrichment's first materialization (see `_missing_stage_assets`) — they
    carry no automation_condition of their own, since AutomationCondition.on_missing()
    (and eager()) cannot be relied on for this: a partition already missing at the
    automation-condition cursor's very first evaluation never becomes "newly missing"
    again and is stuck forever (see phase-2-pipeline-design.md's "Known gaps found
    during dev-subset verification", item 2, in plex-rag). A new imdb_id gets a
    partition added and, like every other desired partition, is checked against on-disk
    state and backfilled directly if anything is missing. An imdb_id no longer in
    stg_movies gets its partition removed *and* its on-disk synopsis/enrichment/
    embeddings files deleted, so the next qdrant_collection rebuild naturally excludes
    it. qdrant_collection is requested directly whenever a removal happened or a
    partition's embeddings needed backfilling: both are invisible or unreliable for
    qdrant_collection's own eager() condition to react to on its own (file deletion
    isn't tracked at all; a cold-started embeddings materialization is exactly the same
    missing()-cursor pitfall this sensor exists to avoid)."""
    with duckdb.get_connection() as conn:
        current_ids = {
            row[0] for row in conn.execute("SELECT imdb_id FROM stg_movies").fetchall()
        }

    limit = (
        int(PLEX_INGEST_PARTITION_LIMIT)
        if PLEX_INGEST_PARTITION_LIMIT is not None
        else None
    )
    desired_ids = compute_desired_ids(current_ids, limit)

    registered_ids = set(
        imdb_id_partitions.get_partition_keys(dynamic_partitions_store=context.instance)
    )
    new_ids, removed_ids = compute_partition_diff(desired_ids, registered_ids)

    for imdb_id in removed_ids:
        _delete_partition_files(imdb_id)

    dynamic_partitions_requests: list[
        dg.AddDynamicPartitionsRequest | dg.DeleteDynamicPartitionsRequest
    ] = []
    if new_ids:
        dynamic_partitions_requests.append(
            imdb_id_partitions.build_add_request(sorted(new_ids))
        )
    if removed_ids:
        dynamic_partitions_requests.append(
            imdb_id_partitions.build_delete_request(sorted(removed_ids))
        )

    if new_ids or removed_ids:
        context.log.info(
            f"imdb_id partitions: +{len(new_ids)} -{len(removed_ids)} "
            f"(desired total: {len(desired_ids)})"
        )

    run_requests: list[dg.RunRequest] = []
    embeddings_backfilled = False
    for imdb_id in sorted(desired_ids):
        missing_assets = _missing_stage_assets(imdb_id)
        if not missing_assets:
            continue
        run_requests.append(
            dg.RunRequest(asset_selection=missing_assets, partition_key=imdb_id)
        )
        if dg.AssetKey("embeddings") in missing_assets:
            embeddings_backfilled = True

    if removed_ids or embeddings_backfilled:
        run_requests.append(
            dg.RunRequest(asset_selection=[dg.AssetKey("qdrant_collection")])
        )

    return dg.SensorResult(
        run_requests=run_requests,
        dynamic_partitions_requests=dynamic_partitions_requests,
    )


default_automation_condition_sensor = dg.AutomationConditionSensorDefinition(
    name="default_automation_condition_sensor",
    target=dg.AssetSelection.all(),
    default_status=dg.DefaultSensorStatus.RUNNING,
)

defs = dg.Definitions(
    sensors=[sync_imdb_id_partitions, default_automation_condition_sensor]
)
