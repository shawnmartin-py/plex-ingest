import hashlib
import uuid

import dagster as dg
from dagster_duckdb import DuckDBResource

from plex_ingest.defs.partitions import imdb_id_partitions
from plex_ingest.defs.resources.partition_json_io_manager import (
    EMBEDDINGS_IO_MANAGER,
    ENRICHMENT_IO_MANAGER,
    SYNOPSIS_IO_MANAGER,
)
from plex_ingest.defs.sensors.run_dedup import in_flight_signatures

_STAGE_IO_MANAGERS = (SYNOPSIS_IO_MANAGER, ENRICHMENT_IO_MANAGER, EMBEDDINGS_IO_MANAGER)
_STAGE_ASSET_KEYS = (
    dg.AssetKey("synopsis"),
    dg.AssetKey("enrichment"),
    dg.AssetKey("embeddings"),
)

_SENSOR_NAME = "sync_imdb_id_partitions"

# A custom tag carrying the *logical* identity of a backfill request (partition +
# missing-asset signature, or the qdrant rebuild's own signature) -- deliberately
# separate from Dagster's `run_key`. Dagster's RunRequest.run_key dedup is permanent
# and status-agnostic: once a run with a given key exists, Dagster will never launch
# another one with that key again, even if that run FAILED. Relying on it here would
# mean any run failure (a crash, a killed daemon, a hard-failed daily quota) silently
# and permanently strands that partition, since its missing-asset state never changes
# on its own -- confirmed in production 2026-07-06. So `run_key` below is minted
# uniquely every tick (just enough to satisfy Dagster's API), and actual duplicate
# prevention is done ourselves via `_in_flight_signatures`, which only considers
# *non-terminal* runs -- a terminal FAILURE is invisible to it and can't block a
# future legitimate attempt.
_BACKFILL_SIGNATURE_TAG_KEY = "plex_ingest/backfill_signature"


def _in_flight_signatures(instance: dg.DagsterInstance) -> set[str]:
    """The `_BACKFILL_SIGNATURE_TAG_KEY` values that already have a non-terminal run
    in flight for this sensor -- the actual duplicate-prevention check (see the
    module-level comment above `_BACKFILL_SIGNATURE_TAG_KEY` for why this replaces
    relying on `run_key` for that purpose). See `run_dedup.py` for the shared
    implementation (also used by `sync_watch_history_partitions`)."""
    return in_flight_signatures(instance, _SENSOR_NAME, _BACKFILL_SIGNATURE_TAG_KEY)


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
    evaluation (evaluation_id 0) got stuck forever (see docs/pipeline-design.md's
    "Known gaps found during dev-subset verification", item 2). Checking
    disk state on every tick sidesteps that cursor entirely."""
    return [
        asset_key
        for asset_key, io_manager in zip(
            _STAGE_ASSET_KEYS, _STAGE_IO_MANAGERS, strict=True
        )
        if not io_manager.path_for(imdb_id).exists()
    ]


def compute_partition_diff(
    desired_ids: set[str], registered_ids: set[str]
) -> tuple[set[str], set[str]]:
    """(new_ids, removed_ids) needed to bring the registered dynamic partition set in
    line with desired_ids."""
    new_ids = desired_ids - registered_ids
    removed_ids = registered_ids - desired_ids
    return new_ids, removed_ids


@dg.sensor(
    minimum_interval_seconds=600,
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
    again and is stuck forever (see docs/pipeline-design.md's "Known gaps found
    during dev-subset verification", item 2). A new imdb_id gets a
    partition added and, like every other desired partition, is checked against on-disk
    state and backfilled directly if anything is missing. An imdb_id no longer in
    stg_movies gets its partition removed *and* its on-disk synopsis/enrichment/
    embeddings files deleted, so the next qdrant_collection rebuild naturally excludes
    it. qdrant_collection is requested directly only when a removal happened: file
    deletion is a raw filesystem write, invisible to Dagster's materialization
    tracking, so it's the one case qdrant_collection's own eager()-derived condition
    truly cannot react to on its own. A freshly-backfilled embeddings partition needs
    no equivalent direct request here — that's an ordinary `any_deps_updated()` event
    (see qdrant_collection.py's automation_condition), unaffected by the
    missing()-cursor pitfall above: that bug is specific to the automation-condition
    cursor's literal evaluation_id 0, a one-time historical event for a running
    instance, not to a partition's own first materialization, which happens at
    whatever evaluation_id the cursor has reached by then. A prior version of this
    sensor requested qdrant_collection directly here too, which caused a redundant
    premature rebuild before embeddings had actually finished for the partition —
    removed 2026-07-14. Duplicate-request prevention is done via
    `_in_flight_signatures`, not Dagster's own `run_key` dedup — see the comment above
    `_BACKFILL_SIGNATURE_TAG_KEY` for why."""
    with duckdb.get_connection() as conn:
        current_ids = {
            row[0] for row in conn.execute("SELECT imdb_id FROM stg_movies").fetchall()
        }

    desired_ids = current_ids

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

    # Dedup against duplicate *in-flight* requests only (see _in_flight_signatures) --
    # not against Dagster's own run_key mechanism, which is permanent and
    # status-agnostic and would silently starve retries after any run failure.
    in_flight = _in_flight_signatures(context.instance)

    run_requests: list[dg.RunRequest] = []
    for imdb_id in sorted(desired_ids):
        missing_assets = _missing_stage_assets(imdb_id)
        if not missing_assets:
            continue
        missing_signature = "-".join(sorted(k.to_user_string() for k in missing_assets))
        backfill_signature = f"{imdb_id}:{missing_signature}"
        if backfill_signature in in_flight:
            continue
        run_requests.append(
            dg.RunRequest(
                run_key=f"{backfill_signature}:{uuid.uuid4().hex[:8]}",
                asset_selection=missing_assets,
                # Explicitly excludes synopsis_matches_movie, which would otherwise
                # tag along automatically whenever `synopsis` is in asset_selection.
                # Disabled 2026-07-06: a full-catalog verification run showed the
                # Groq/qwen3-32b judge is unreliable at scale (~85% false-mismatch
                # rate against known-correct synopses, including inconsistent
                # verdicts for the same partition across separate runs) -- see
                # docs/pipeline-design.md's "Data-quality checks" for the full
                # writeup. The check's code is left in place; re-enable by removing
                # this once a search-capable judge model replaces qwen3-32b.
                asset_check_keys=[],
                partition_key=imdb_id,
                tags={_BACKFILL_SIGNATURE_TAG_KEY: backfill_signature},
            )
        )

    if removed_ids:
        # Signature of *why* a qdrant_collection rebuild is needed right now, used
        # the same way as backfill_signature above (not as run_key).
        qdrant_signature = (
            "qdrant:"
            + hashlib.sha256(f"{sorted(removed_ids)}".encode()).hexdigest()[:16]
        )
        if qdrant_signature not in in_flight:
            run_requests.append(
                dg.RunRequest(
                    run_key=f"{qdrant_signature}:{uuid.uuid4().hex[:8]}",
                    asset_selection=[dg.AssetKey("qdrant_collection")],
                    tags={_BACKFILL_SIGNATURE_TAG_KEY: qdrant_signature},
                )
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
