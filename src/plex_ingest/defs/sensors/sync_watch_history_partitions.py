import uuid

import dagster as dg
from dagster_duckdb import DuckDBResource

from plex_ingest.defs.partitions import watch_history_imdb_id_partitions
from plex_ingest.defs.resources.partition_json_io_manager import (
    WATCH_HISTORY_EMBEDDINGS_IO_MANAGER,
)
from plex_ingest.defs.sensors.run_dedup import in_flight_signatures

_SENSOR_NAME = "sync_watch_history_partitions"

_BACKFILL_SIGNATURE_TAG_KEY = "plex_ingest/watch_history_backfill_signature"

# Matches sync_imdb_id_partitions's interval (see docs/pipeline-design.md's
# "Scheduling cadence" -- both sensors are pool-bound rather than tick-rate-bound,
# so ticking faster than this buys no real throughput, just wasted DB/file-stat
# overhead; see that doc for the full reasoning).
_MINIMUM_INTERVAL_SECONDS = 600


def _missing_embeddings(imdb_id: str) -> bool:
    return not WATCH_HISTORY_EMBEDDINGS_IO_MANAGER.path_for(imdb_id).exists()


def _qdrant_collection_needs_cold_start(instance: dg.DagsterInstance) -> bool:
    """True until `watch_history_qdrant_collection` has materialized at least once.
    Deliberately independent of this tick's `backfilled_any` -- that flag is derived
    from on-disk embeddings state and goes false again the moment an embedding lands,
    even if the *qdrant* RunRequest that was supposed to consume it failed to submit
    (e.g. a transient resource-config error). Without this check, that failure
    silently and permanently strands the cold start: no future tick would see
    anything "newly missing" to re-trigger it, since eager() can't catch this cold
    start either (see this sensor's docstring). Checked directly against the
    materialization event log, not a cursor, for the same reason `_missing_embeddings`
    checks on-disk state directly."""
    return (
        instance.get_latest_materialization_event(
            dg.AssetKey("watch_history_qdrant_collection")
        )
        is None
    )


def compute_new_partition_ids(
    desired_ids: set[str], registered_ids: set[str]
) -> set[str]:
    """Add-only, deliberately -- unlike sync_imdb_id_partitions's
    compute_partition_diff, this never computes (or acts on) a removed set. See this
    module's docstring for why."""
    return desired_ids - registered_ids


@dg.sensor(
    minimum_interval_seconds=_MINIMUM_INTERVAL_SECONDS,
    default_status=dg.DefaultSensorStatus.RUNNING,
    asset_selection=[
        dg.AssetKey("watch_history_embeddings"),
        dg.AssetKey("watch_history_qdrant_collection"),
    ],
)
def sync_watch_history_partitions(
    context: dg.SensorEvaluationContext, duckdb: DuckDBResource
) -> dg.SensorResult:
    """Keeps the `watch_history_imdb_id` dynamic partition set in sync with
    `stg_watch_history`, and is the sole trigger for `watch_history_embeddings`'s
    first materialization (mirroring `sync_imdb_id_partitions`'s reasoning for
    `synopsis`/`enrichment` -- `AutomationCondition.on_missing()`/`eager()` can't be
    relied on for an asset's own cold start; see that sensor's docstring for the full
    mechanism). Checked directly against on-disk state every tick, not against a
    one-shot automation-condition event.

    **Add-only**, unlike `sync_imdb_id_partitions`: an imdb_id already partitioned
    here is never removed just because a later run's `stg_watch_history` fetch window
    no longer includes it (`stg_watch_history` itself upserts rather than overwrites,
    specifically so this holds -- see that asset's docstring). This is what
    guarantees a given imdb_id is only ever embedded once, matching the requirement
    driving this whole pipeline's design -- see docs/pipeline-design.md's
    "Watch-history diversity-recommender pipeline".

    `watch_history_qdrant_collection` is requested directly whenever anything gets
    backfilled *or* it has never materialized (`_qdrant_collection_needs_cold_start`),
    the same cold-start gap `sync_imdb_id_partitions` covers for `qdrant_collection`: a
    freshly-backfilled `watch_history_embeddings` partition is exactly the case
    `eager()` can't reliably react to on its own. The cold-start check is deliberately
    separate from `backfilled_any` -- confirmed 2026-07-12: an embeddings RunRequest
    can succeed while the paired qdrant RunRequest in the same tick fails to submit
    (e.g. a resource-config error), after which `backfilled_any` alone would never be
    true again for that imdb_id since its embedding already exists on disk, silently
    and permanently stranding `watch_history_qdrant_collection` at "never
    materialized" with nothing left to re-trigger it. Its own steady-state reaction to
    a `stg_watch_history`-only change (e.g. a rewatch updating `last_viewed_at` with no
    new embedding needed) is left to `eager()`, which is the "ordinary steady-state
    cascade" case already proven to work elsewhere in this pipeline -- once
    `watch_history_qdrant_collection` has materialized once, cold-start requests stop
    and `eager()` takes over. Duplicate-request prevention uses the shared
    `run_dedup.in_flight_signatures`, not Dagster's own `run_key` dedup -- see
    `run_dedup.py`'s module docstring for why."""
    with duckdb.get_connection() as conn:
        current_ids = {
            row[0]
            for row in conn.execute("SELECT imdb_id FROM stg_watch_history").fetchall()
        }

    registered_ids = set(
        watch_history_imdb_id_partitions.get_partition_keys(
            dynamic_partitions_store=context.instance
        )
    )
    new_ids = compute_new_partition_ids(current_ids, registered_ids)

    dynamic_partitions_requests: list[dg.AddDynamicPartitionsRequest] = []
    if new_ids:
        dynamic_partitions_requests.append(
            watch_history_imdb_id_partitions.build_add_request(sorted(new_ids))
        )
        context.log.info(f"watch_history_imdb_id partitions: +{len(new_ids)}")

    in_flight = in_flight_signatures(
        context.instance, _SENSOR_NAME, _BACKFILL_SIGNATURE_TAG_KEY
    )

    run_requests: list[dg.RunRequest] = []
    backfilled_any = False
    for imdb_id in sorted(current_ids | registered_ids):
        if not _missing_embeddings(imdb_id):
            continue
        backfilled_any = True
        signature = f"{imdb_id}:embeddings"
        if signature in in_flight:
            continue
        run_requests.append(
            dg.RunRequest(
                run_key=f"{signature}:{uuid.uuid4().hex[:8]}",
                asset_selection=[dg.AssetKey("watch_history_embeddings")],
                partition_key=imdb_id,
                tags={_BACKFILL_SIGNATURE_TAG_KEY: signature},
            )
        )

    if backfilled_any or _qdrant_collection_needs_cold_start(context.instance):
        qdrant_signature = "watch_history_qdrant_rebuild"
        if qdrant_signature not in in_flight:
            run_requests.append(
                dg.RunRequest(
                    run_key=f"{qdrant_signature}:{uuid.uuid4().hex[:8]}",
                    asset_selection=[dg.AssetKey("watch_history_qdrant_collection")],
                    tags={_BACKFILL_SIGNATURE_TAG_KEY: qdrant_signature},
                )
            )

    return dg.SensorResult(
        run_requests=run_requests,
        dynamic_partitions_requests=dynamic_partitions_requests,
    )


defs = dg.Definitions(sensors=[sync_watch_history_partitions])
