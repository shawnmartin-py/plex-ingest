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
    asset_selection=[dg.AssetKey("watch_history_embeddings")],
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

    Unlike `sync_imdb_id_partitions`, this sensor never requests
    `watch_history_qdrant_collection` directly -- it's left entirely to that asset's
    own `eager()` condition. A prior version requested it directly whenever anything
    got backfilled, or via a `_qdrant_collection_needs_cold_start` check for the case
    where it had never materialized at all; both were removed 2026-07-14 (see
    `sync_imdb_id_partitions`'s docstring and docs/pipeline-design.md's "Known gaps",
    item 2 correction for the full reasoning this mirrors). `any_deps_updated()` --
    what `eager()` actually reacts to -- is a plain materialization event read off the
    instance's event log; it fires correctly the first time a `watch_history_embeddings`
    partition materializes just as reliably as the hundredth time, regardless of
    whether *this* sensor's own RunRequest for it succeeds, fails to submit, or
    doesn't exist at all. There is no add-only equivalent of `sync_imdb_id_partitions`'s
    `removed_ids` case here (this sensor never deletes anything), so no scenario
    remains where a direct trigger is actually necessary. Duplicate-request
    prevention uses the shared `run_dedup.in_flight_signatures`, not Dagster's own
    `run_key` dedup -- see `run_dedup.py`'s module docstring for why."""
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
    for imdb_id in sorted(current_ids | registered_ids):
        if not _missing_embeddings(imdb_id):
            continue
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

    return dg.SensorResult(
        run_requests=run_requests,
        dynamic_partitions_requests=dynamic_partitions_requests,
    )


defs = dg.Definitions(sensors=[sync_watch_history_partitions])
