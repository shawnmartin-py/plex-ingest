import dagster as dg
from dagster._core.test_utils import create_run_for_test

from plex_ingest.defs.sensors.run_dedup import in_flight_signatures

_TAG_KEY = "plex_ingest/test_signature"


def test_includes_non_terminal_runs() -> None:
    instance = dg.DagsterInstance.ephemeral()
    create_run_for_test(
        instance,
        status=dg.DagsterRunStatus.STARTED,
        tags={"dagster/sensor_name": "some_sensor", _TAG_KEY: "tt0001:x"},
    )
    assert in_flight_signatures(instance, "some_sensor", _TAG_KEY) == {"tt0001:x"}


def test_excludes_terminal_runs() -> None:
    instance = dg.DagsterInstance.ephemeral()
    for status in (
        dg.DagsterRunStatus.SUCCESS,
        dg.DagsterRunStatus.FAILURE,
        dg.DagsterRunStatus.CANCELED,
    ):
        create_run_for_test(
            instance,
            status=status,
            tags={
                "dagster/sensor_name": "some_sensor",
                _TAG_KEY: f"tt0001:{status.value}",
            },
        )
    assert in_flight_signatures(instance, "some_sensor", _TAG_KEY) == set()


def test_ignores_runs_from_other_sensors() -> None:
    instance = dg.DagsterInstance.ephemeral()
    create_run_for_test(
        instance,
        status=dg.DagsterRunStatus.STARTED,
        tags={"dagster/sensor_name": "a_different_sensor", _TAG_KEY: "tt0001:x"},
    )
    assert in_flight_signatures(instance, "some_sensor", _TAG_KEY) == set()


def test_ignores_runs_with_a_different_tag_key() -> None:
    instance = dg.DagsterInstance.ephemeral()
    create_run_for_test(
        instance,
        status=dg.DagsterRunStatus.STARTED,
        tags={"dagster/sensor_name": "some_sensor", "some/other_tag": "tt0001:x"},
    )
    assert in_flight_signatures(instance, "some_sensor", _TAG_KEY) == set()
