from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import dagster as dg
from dagster._core.test_utils import create_run_for_test
from pytest_mock import MockerFixture

from plex_ingest.defs.resources.partition_json_io_manager import JsonPartitionIOManager
from plex_ingest.defs.sensors.sync_watch_history_partitions import (
    _BACKFILL_SIGNATURE_TAG_KEY,
    _SENSOR_NAME,
    compute_new_partition_ids,
    sync_watch_history_partitions,
)

_PARTITIONS_DEF_NAME = "watch_history_tmdb_id"


def _patch_embeddings_io_manager(mocker: MockerFixture, base_dir: Path) -> None:
    import plex_ingest.defs.sensors.sync_watch_history_partitions as sensor_module

    mocker.patch.object(
        sensor_module,
        "WATCH_HISTORY_EMBEDDINGS_IO_MANAGER",
        JsonPartitionIOManager(base_dir=str(base_dir)),
    )


def _write_embeddings_file(base_dir: Path, tmdb_id: str) -> None:
    base_dir.mkdir(exist_ok=True, parents=True)
    (base_dir / f"{tmdb_id}.json").write_text("{}")


def _report_qdrant_collection_materialized(instance: dg.DagsterInstance) -> None:
    instance.report_runless_asset_event(
        dg.AssetMaterialization(
            asset_key=dg.AssetKey("watch_history_qdrant_collection")
        )
    )


def _mock_duckdb(mocker: MockerFixture, current_ids: set[str]) -> MagicMock:
    mock_duckdb = cast(MagicMock, mocker.MagicMock())
    mock_conn = mock_duckdb.get_connection.return_value.__enter__.return_value
    mock_conn.execute.return_value.fetchall.return_value = [
        (tmdb_id,) for tmdb_id in current_ids
    ]
    return mock_duckdb


# --- compute_new_partition_ids ---


def test_compute_new_partition_ids_returns_only_unregistered_ids() -> None:
    assert compute_new_partition_ids(desired_ids={"1", "2"}, registered_ids={"1"}) == {
        "2"
    }


def test_compute_new_partition_ids_ignores_ids_no_longer_desired() -> None:
    """The add-only property at the unit level: a registered id absent from
    desired_ids produces no signal at all here -- there's no removed-set concept."""
    assert (
        compute_new_partition_ids(desired_ids={"1"}, registered_ids={"1", "2"}) == set()
    )


# --- sync_watch_history_partitions (full sensor) ---


def test_new_id_is_added_and_backfilled(tmp_path: Path, mocker: MockerFixture) -> None:
    _patch_embeddings_io_manager(mocker, tmp_path)
    instance = dg.DagsterInstance.ephemeral()
    context = dg.build_sensor_context(instance=instance)

    result = sync_watch_history_partitions(
        context, duckdb=_mock_duckdb(mocker, {"0001"})
    )

    assert isinstance(result, dg.SensorResult)
    assert result.dynamic_partitions_requests
    assert result.dynamic_partitions_requests[0].partition_keys == ["0001"]
    run_requests = result.run_requests
    assert run_requests is not None
    # No direct watch_history_qdrant_collection request accompanies this -- that's
    # left entirely to its own eager() condition (see the sensor's docstring).
    assert len(run_requests) == 1
    backfill = run_requests[0]
    assert backfill.partition_key == "0001"
    assert backfill.asset_selection == [dg.AssetKey("watch_history_embeddings")]


def test_no_changes_requests_nothing_once_fully_materialized(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    _patch_embeddings_io_manager(mocker, tmp_path)
    _write_embeddings_file(tmp_path, "0001")
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, ["0001"])
    _report_qdrant_collection_materialized(instance)
    context = dg.build_sensor_context(instance=instance)

    result = sync_watch_history_partitions(
        context, duckdb=_mock_duckdb(mocker, {"0001"})
    )

    assert isinstance(result, dg.SensorResult)
    assert result.run_requests == []
    assert result.dynamic_partitions_requests == []


def test_id_no_longer_in_stg_watch_history_is_not_removed(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """The core add-only property this sensor exists for: 0001 aged out of
    stg_watch_history's current fetch window (absent from current_ids) but must
    remain a registered partition -- no removal request, no deleted embeddings file."""
    _patch_embeddings_io_manager(mocker, tmp_path)
    _write_embeddings_file(tmp_path, "0001")
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, ["0001"])
    _report_qdrant_collection_materialized(instance)
    context = dg.build_sensor_context(instance=instance)

    # 0001 is no longer in stg_watch_history's current window at all.
    result = sync_watch_history_partitions(context, duckdb=_mock_duckdb(mocker, set()))

    assert isinstance(result, dg.SensorResult)
    assert result.dynamic_partitions_requests == []
    assert result.run_requests == []
    assert (tmp_path / "0001.json").exists()
    assert "0001" in set(
        dg.DynamicPartitionsDefinition(name=_PARTITIONS_DEF_NAME).get_partition_keys(
            dynamic_partitions_store=instance
        )
    )


def test_previously_registered_partition_missing_embeddings_still_gets_backfilled(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """A registered partition with no on-disk embeddings file gets backfilled even
    when it's no longer in stg_watch_history's current window -- add-only means it's
    never re-derived from current_ids alone, so the missing-embeddings check must
    still cover it."""
    _patch_embeddings_io_manager(mocker, tmp_path)
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, ["0001"])
    context = dg.build_sensor_context(instance=instance)

    result = sync_watch_history_partitions(context, duckdb=_mock_duckdb(mocker, set()))

    assert isinstance(result, dg.SensorResult)
    run_requests = result.run_requests
    assert run_requests is not None
    backfill = next(r for r in run_requests if r.partition_key == "0001")
    assert backfill.asset_selection == [dg.AssetKey("watch_history_embeddings")]


def test_run_requests_carry_a_run_key(tmp_path: Path, mocker: MockerFixture) -> None:
    _patch_embeddings_io_manager(mocker, tmp_path)
    instance = dg.DagsterInstance.ephemeral()
    context = dg.build_sensor_context(instance=instance)

    result = sync_watch_history_partitions(
        context, duckdb=_mock_duckdb(mocker, {"0001"})
    )

    assert isinstance(result, dg.SensorResult)
    run_requests = result.run_requests
    assert run_requests
    assert all(r.run_key is not None for r in run_requests)


def test_no_duplicate_backfill_while_one_is_in_flight(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    _patch_embeddings_io_manager(mocker, tmp_path)
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, ["0001"])

    context = dg.build_sensor_context(instance=instance)
    first = sync_watch_history_partitions(
        context, duckdb=_mock_duckdb(mocker, {"0001"})
    )
    assert isinstance(first, dg.SensorResult)
    backfill = next(r for r in (first.run_requests or []) if r.partition_key == "0001")
    signature = backfill.tags[_BACKFILL_SIGNATURE_TAG_KEY]

    create_run_for_test(
        instance,
        status=dg.DagsterRunStatus.STARTED,
        tags={
            "dagster/sensor_name": _SENSOR_NAME,
            _BACKFILL_SIGNATURE_TAG_KEY: signature,
        },
    )

    context = dg.build_sensor_context(instance=instance)
    second = sync_watch_history_partitions(
        context, duckdb=_mock_duckdb(mocker, {"0001"})
    )
    assert isinstance(second, dg.SensorResult)
    assert all(r.partition_key != "0001" for r in (second.run_requests or []))


def test_backfill_is_retried_after_a_terminal_failure(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    _patch_embeddings_io_manager(mocker, tmp_path)
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, ["0001"])

    context = dg.build_sensor_context(instance=instance)
    first = sync_watch_history_partitions(
        context, duckdb=_mock_duckdb(mocker, {"0001"})
    )
    assert isinstance(first, dg.SensorResult)
    backfill = next(r for r in (first.run_requests or []) if r.partition_key == "0001")
    signature = backfill.tags[_BACKFILL_SIGNATURE_TAG_KEY]

    create_run_for_test(
        instance,
        status=dg.DagsterRunStatus.FAILURE,
        tags={
            "dagster/sensor_name": _SENSOR_NAME,
            _BACKFILL_SIGNATURE_TAG_KEY: signature,
        },
    )

    context = dg.build_sensor_context(instance=instance)
    second = sync_watch_history_partitions(
        context, duckdb=_mock_duckdb(mocker, {"0001"})
    )
    assert isinstance(second, dg.SensorResult)
    retried = next(r for r in (second.run_requests or []) if r.partition_key == "0001")
    assert retried.tags[_BACKFILL_SIGNATURE_TAG_KEY] == signature
    assert retried.run_key != backfill.run_key


def test_never_requests_watch_history_qdrant_collection_directly(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """This sensor no longer requests watch_history_qdrant_collection under any
    circumstance -- not on backfill, not on cold start (both removed 2026-07-14, see
    the sensor's docstring). It's left entirely to that asset's own eager()
    condition, which reacts correctly to any_deps_updated() regardless of whether
    watch_history_qdrant_collection has ever materialized before."""
    _patch_embeddings_io_manager(mocker, tmp_path)
    # No embeddings file on disk and watch_history_qdrant_collection has never
    # materialized -- the exact scenario the old cold-start check existed for.
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, ["0001"])
    context = dg.build_sensor_context(instance=instance)

    result = sync_watch_history_partitions(
        context, duckdb=_mock_duckdb(mocker, {"0001"})
    )

    assert isinstance(result, dg.SensorResult)
    run_requests = result.run_requests
    assert run_requests is not None
    assert all(
        dg.AssetKey("watch_history_qdrant_collection") not in (r.asset_selection or [])
        for r in run_requests
    )
