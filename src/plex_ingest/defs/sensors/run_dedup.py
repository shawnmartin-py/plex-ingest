"""Shared duplicate-request-prevention helper for partition-sync sensors.

Extracted from `sync_tmdb_id_partitions.py`, which is the sensor with the full
history of *why* this exists: Dagster's own `RunRequest.run_key` dedup is permanent
and status-agnostic, so relying on it means any run failure (a crash, a killed
daemon, a hard-failed daily quota) silently and permanently strands whatever that
run was for, since the condition that triggered it never changes on its own
(confirmed in production 2026-07-06 — see that module's docstring for the full
writeup). Sensors here mint `run_key` uniquely every tick and instead track their
own in-flight state via a signature tag, considering only *non-terminal* runs.
"""

import dagster as dg

TERMINAL_RUN_STATUSES = frozenset(
    {
        dg.DagsterRunStatus.SUCCESS,
        dg.DagsterRunStatus.FAILURE,
        dg.DagsterRunStatus.CANCELED,
    }
)
NON_TERMINAL_RUN_STATUSES = [
    status for status in dg.DagsterRunStatus if status not in TERMINAL_RUN_STATUSES
]


def in_flight_signatures(
    instance: dg.DagsterInstance, sensor_name: str, signature_tag_key: str
) -> set[str]:
    """The `signature_tag_key` values that already have a non-terminal run in flight
    for `sensor_name`."""
    records = instance.get_run_records(
        filters=dg.RunsFilter(
            tags={"dagster/sensor_name": sensor_name},
            statuses=NON_TERMINAL_RUN_STATUSES,
        )
    )
    return {
        r.dagster_run.tags[signature_tag_key]
        for r in records
        if signature_tag_key in r.dagster_run.tags
    }
