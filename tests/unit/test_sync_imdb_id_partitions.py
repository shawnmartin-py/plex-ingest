from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import dagster as dg
from dagster._core.test_utils import create_run_for_test
from pytest_mock import MockerFixture

from plex_ingest.defs.resources.partition_json_io_manager import JsonPartitionIOManager
from plex_ingest.defs.sensors.sync_imdb_id_partitions import (
    _BACKFILL_SIGNATURE_TAG_KEY,
    _SENSOR_NAME,
    _delete_partition_files,
    _in_flight_signatures,
    _missing_stage_assets,
    compute_partition_diff,
    sync_imdb_id_partitions,
)

_STAGES = ("synopsis", "enrichment", "embeddings")


def _patch_stage_io_managers(mocker: MockerFixture, base_dir: Path) -> None:
    import plex_ingest.defs.sensors.sync_imdb_id_partitions as sensor_module

    managers = tuple(
        JsonPartitionIOManager(base_dir=str(base_dir / stage)) for stage in _STAGES
    )
    mocker.patch.object(sensor_module, "_STAGE_IO_MANAGERS", managers)


def _write_stage_files(
    base_dir: Path, imdb_id: str, stages: tuple[str, ...] = _STAGES
) -> None:
    """Marks a partition as already materialized for the given stages, so it's not
    picked up by `_missing_stage_assets`'s backfill check."""
    for stage in stages:
        stage_dir = base_dir / stage
        stage_dir.mkdir(exist_ok=True)
        (stage_dir / f"{imdb_id}.json").write_text("{}")


# --- compute_partition_diff ---


def test_diff_adds_new_ids_not_yet_registered() -> None:
    new_ids, removed_ids = compute_partition_diff(
        desired_ids={"tt1", "tt2"}, registered_ids={"tt1"}
    )
    assert new_ids == {"tt2"}
    assert removed_ids == set()


def test_diff_removes_registered_ids_no_longer_desired() -> None:
    new_ids, removed_ids = compute_partition_diff(
        desired_ids={"tt1"}, registered_ids={"tt1", "tt2"}
    )
    assert new_ids == set()
    assert removed_ids == {"tt2"}


def test_diff_is_empty_when_already_in_sync() -> None:
    new_ids, removed_ids = compute_partition_diff(
        desired_ids={"tt1"}, registered_ids={"tt1"}
    )
    assert new_ids == set()
    assert removed_ids == set()


def test_diff_handles_simultaneous_additions_and_removals() -> None:
    new_ids, removed_ids = compute_partition_diff(
        desired_ids={"tt1", "tt3"}, registered_ids={"tt1", "tt2"}
    )
    assert new_ids == {"tt3"}
    assert removed_ids == {"tt2"}


# --- _delete_partition_files ---


def test_delete_partition_files_removes_files_for_all_three_stages(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    _patch_stage_io_managers(mocker, tmp_path)
    for stage in ("synopsis", "enrichment", "embeddings"):
        stage_dir = tmp_path / stage
        stage_dir.mkdir()
        (stage_dir / "tt0242888.json").write_text("{}")

    _delete_partition_files("tt0242888")

    for stage in ("synopsis", "enrichment", "embeddings"):
        assert not (tmp_path / stage / "tt0242888.json").exists()


def test_delete_partition_files_is_a_noop_when_files_dont_exist(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    _patch_stage_io_managers(mocker, tmp_path)
    # Should not raise even though none of the files exist.
    _delete_partition_files("tt9999999")


def test_delete_partition_files_does_not_touch_other_movies(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    _patch_stage_io_managers(mocker, tmp_path)
    stage_dir = tmp_path / "synopsis"
    stage_dir.mkdir()
    (stage_dir / "tt0001.json").write_text("{}")
    (stage_dir / "tt0002.json").write_text("{}")

    _delete_partition_files("tt0001")

    assert not (stage_dir / "tt0001.json").exists()
    assert (stage_dir / "tt0002.json").exists()


# --- _missing_stage_assets ---


def test_missing_stage_assets_returns_all_three_when_nothing_materialized(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    _patch_stage_io_managers(mocker, tmp_path)
    assert set(_missing_stage_assets("tt0001")) == {
        dg.AssetKey("synopsis"),
        dg.AssetKey("enrichment"),
        dg.AssetKey("embeddings"),
    }


def test_missing_stage_assets_returns_empty_when_all_materialized(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    _patch_stage_io_managers(mocker, tmp_path)
    _write_stage_files(tmp_path, "tt0001")
    assert _missing_stage_assets("tt0001") == []


def test_missing_stage_assets_returns_only_the_missing_subset(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    _patch_stage_io_managers(mocker, tmp_path)
    _write_stage_files(tmp_path, "tt0001", stages=("synopsis", "enrichment"))
    assert _missing_stage_assets("tt0001") == [dg.AssetKey("embeddings")]


# --- sync_imdb_id_partitions (full sensor, run-request behavior on removal) ---

_PARTITIONS_DEF_NAME = "imdb_id"


def _mock_duckdb(mocker: MockerFixture, current_ids: set[str]) -> MagicMock:
    mock_duckdb = cast(MagicMock, mocker.MagicMock())
    mock_conn = mock_duckdb.get_connection.return_value.__enter__.return_value
    mock_conn.execute.return_value.fetchall.return_value = [
        (imdb_id,) for imdb_id in current_ids
    ]
    return mock_duckdb


def test_removal_requests_a_qdrant_collection_rebuild(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    _patch_stage_io_managers(mocker, tmp_path)
    # tt0001 is already fully materialized, isolating the removal behavior from any
    # backfill request that an un-materialized surviving partition would also trigger.
    _write_stage_files(tmp_path, "tt0001")
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, ["tt0001", "tt0002"])
    context = dg.build_sensor_context(instance=instance)

    # tt0002 no longer in stg_movies -> should be removed and trigger a rebuild.
    result = sync_imdb_id_partitions(context, duckdb=_mock_duckdb(mocker, {"tt0001"}))

    assert isinstance(result, dg.SensorResult)
    run_requests = result.run_requests
    assert run_requests is not None
    assert len(run_requests) == 1
    assert run_requests[0].asset_selection == [dg.AssetKey("qdrant_collection")]


def test_addition_backfills_the_new_partition_without_a_direct_qdrant_rebuild(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    _patch_stage_io_managers(mocker, tmp_path)
    _write_stage_files(tmp_path, "tt0001")
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, ["tt0001"])
    context = dg.build_sensor_context(instance=instance)

    # tt0002 is new and has no on-disk files -> gets backfilled directly. No direct
    # qdrant_collection request accompanies it: that would fire before embeddings has
    # actually finished for tt0002, a redundant premature rebuild (removed 2026-07-14
    # -- see sync_imdb_id_partitions's docstring). qdrant_collection's own
    # eager()-derived condition reacts once embeddings genuinely updates.
    result = sync_imdb_id_partitions(
        context, duckdb=_mock_duckdb(mocker, {"tt0001", "tt0002"})
    )

    assert isinstance(result, dg.SensorResult)
    run_requests = result.run_requests
    assert run_requests is not None
    assert len(run_requests) == 1
    backfill = run_requests[0]
    assert backfill.partition_key == "tt0002"
    assert set(backfill.asset_selection or []) == {
        dg.AssetKey("synopsis"),
        dg.AssetKey("enrichment"),
        dg.AssetKey("embeddings"),
    }


def test_no_changes_requests_nothing_once_fully_materialized(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    _patch_stage_io_managers(mocker, tmp_path)
    _write_stage_files(tmp_path, "tt0001")
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, ["tt0001"])
    context = dg.build_sensor_context(instance=instance)

    result = sync_imdb_id_partitions(context, duckdb=_mock_duckdb(mocker, {"tt0001"}))

    assert isinstance(result, dg.SensorResult)
    assert result.run_requests == []
    assert result.dynamic_partitions_requests == []


def test_run_requests_carry_a_run_key(tmp_path: Path, mocker: MockerFixture) -> None:
    """Regression test for the unbounded-queue-growth bug: every RunRequest must
    carry a run_key so Dagster dedupes repeat requests for the same still-pending
    partition across ticks, instead of queueing a fresh duplicate run every tick."""
    _patch_stage_io_managers(mocker, tmp_path)
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, ["tt0001", "tt0002"])
    context = dg.build_sensor_context(instance=instance)
    _write_stage_files(tmp_path, "tt0001")

    result = sync_imdb_id_partitions(
        context, duckdb=_mock_duckdb(mocker, {"tt0001", "tt0002"})
    )

    assert isinstance(result, dg.SensorResult)
    run_requests = result.run_requests
    assert run_requests is not None
    assert run_requests  # sanity: this scenario does produce requests
    assert all(r.run_key is not None for r in run_requests)


def test_backfill_signature_changes_once_the_missing_asset_set_changes(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """Once a partition's missing-asset signature genuinely changes (here: synopsis
    finishes materializing between ticks), the backfill_signature tag must change
    too -- that's what _in_flight_signatures keys on, not run_key (which is now
    minted uniquely every tick regardless -- see _BACKFILL_SIGNATURE_TAG_KEY's
    module-level comment for why)."""
    _patch_stage_io_managers(mocker, tmp_path)
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, ["tt0001"])

    context = dg.build_sensor_context(instance=instance)
    first = sync_imdb_id_partitions(context, duckdb=_mock_duckdb(mocker, {"tt0001"}))
    assert isinstance(first, dg.SensorResult)
    first_signature = next(
        r.tags[_BACKFILL_SIGNATURE_TAG_KEY]
        for r in (first.run_requests or [])
        if r.partition_key == "tt0001"
    )

    _write_stage_files(tmp_path, "tt0001", stages=("synopsis",))
    context = dg.build_sensor_context(instance=instance)
    second = sync_imdb_id_partitions(context, duckdb=_mock_duckdb(mocker, {"tt0001"}))
    assert isinstance(second, dg.SensorResult)
    second_signature = next(
        r.tags[_BACKFILL_SIGNATURE_TAG_KEY]
        for r in (second.run_requests or [])
        if r.partition_key == "tt0001"
    )

    assert first_signature != second_signature


def test_no_duplicate_backfill_while_one_is_in_flight(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """The actual duplicate-prevention guarantee now: a second tick must not
    re-request a partition whose prior request is still non-terminal (queued/
    started/etc.) -- replacing reliance on Dagster's own run_key dedup, which
    turned out to also silently block legitimate retries after a failure (see
    test_backfill_is_retried_after_a_terminal_failure)."""
    _patch_stage_io_managers(mocker, tmp_path)
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, ["tt0001"])

    context = dg.build_sensor_context(instance=instance)
    first = sync_imdb_id_partitions(context, duckdb=_mock_duckdb(mocker, {"tt0001"}))
    assert isinstance(first, dg.SensorResult)
    backfill = next(
        r for r in (first.run_requests or []) if r.partition_key == "tt0001"
    )
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
    second = sync_imdb_id_partitions(context, duckdb=_mock_duckdb(mocker, {"tt0001"}))
    assert isinstance(second, dg.SensorResult)
    # tt0001 itself must not be re-requested; no other requests are expected here
    # (no removal happened, so nothing else would trigger a qdrant_collection request).
    assert (second.run_requests or []) == []


def test_backfill_is_retried_after_a_terminal_failure(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """The core motivating fix: a partition whose prior attempt FAILED must still be
    retried. Dagster's own run_key dedup can't distinguish "failed" from "already
    successfully handled" -- confirmed in production 2026-07-06, where exactly this
    silently stranded a partition until it was manually relaunched (see CLAUDE.md's
    "Environment gotchas")."""
    _patch_stage_io_managers(mocker, tmp_path)
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, ["tt0001"])

    context = dg.build_sensor_context(instance=instance)
    first = sync_imdb_id_partitions(context, duckdb=_mock_duckdb(mocker, {"tt0001"}))
    assert isinstance(first, dg.SensorResult)
    backfill = next(
        r for r in (first.run_requests or []) if r.partition_key == "tt0001"
    )
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
    second = sync_imdb_id_partitions(context, duckdb=_mock_duckdb(mocker, {"tt0001"}))
    assert isinstance(second, dg.SensorResult)
    retried = next(
        r for r in (second.run_requests or []) if r.partition_key == "tt0001"
    )
    assert retried.tags[_BACKFILL_SIGNATURE_TAG_KEY] == signature
    assert retried.run_key != backfill.run_key


def test_qdrant_rebuild_not_duplicated_while_in_flight(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    _patch_stage_io_managers(mocker, tmp_path)
    _write_stage_files(tmp_path, "tt0001")
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, ["tt0001", "tt0002"])

    context = dg.build_sensor_context(instance=instance)
    first = sync_imdb_id_partitions(context, duckdb=_mock_duckdb(mocker, {"tt0001"}))
    assert isinstance(first, dg.SensorResult)
    rebuild = next(r for r in (first.run_requests or []) if r.partition_key is None)
    signature = rebuild.tags[_BACKFILL_SIGNATURE_TAG_KEY]

    create_run_for_test(
        instance,
        status=dg.DagsterRunStatus.STARTED,
        tags={
            "dagster/sensor_name": _SENSOR_NAME,
            _BACKFILL_SIGNATURE_TAG_KEY: signature,
        },
    )

    context = dg.build_sensor_context(instance=instance)
    second = sync_imdb_id_partitions(context, duckdb=_mock_duckdb(mocker, {"tt0001"}))
    assert isinstance(second, dg.SensorResult)
    assert all(r.partition_key is not None for r in (second.run_requests or []))


def test_qdrant_rebuild_retried_after_a_terminal_failure(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    _patch_stage_io_managers(mocker, tmp_path)
    _write_stage_files(tmp_path, "tt0001")
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, ["tt0001", "tt0002"])

    context = dg.build_sensor_context(instance=instance)
    first = sync_imdb_id_partitions(context, duckdb=_mock_duckdb(mocker, {"tt0001"}))
    assert isinstance(first, dg.SensorResult)
    rebuild = next(r for r in (first.run_requests or []) if r.partition_key is None)
    signature = rebuild.tags[_BACKFILL_SIGNATURE_TAG_KEY]

    create_run_for_test(
        instance,
        status=dg.DagsterRunStatus.FAILURE,
        tags={
            "dagster/sensor_name": _SENSOR_NAME,
            _BACKFILL_SIGNATURE_TAG_KEY: signature,
        },
    )

    context = dg.build_sensor_context(instance=instance)
    second = sync_imdb_id_partitions(context, duckdb=_mock_duckdb(mocker, {"tt0001"}))
    assert isinstance(second, dg.SensorResult)
    retried = next(r for r in (second.run_requests or []) if r.partition_key is None)
    assert retried.tags[_BACKFILL_SIGNATURE_TAG_KEY] == signature
    assert retried.run_key != rebuild.run_key


# --- _in_flight_signatures ---


def test_in_flight_signatures_includes_non_terminal_runs() -> None:
    instance = dg.DagsterInstance.ephemeral()
    create_run_for_test(
        instance,
        status=dg.DagsterRunStatus.STARTED,
        tags={
            "dagster/sensor_name": _SENSOR_NAME,
            _BACKFILL_SIGNATURE_TAG_KEY: "tt0001:x",
        },
    )
    assert _in_flight_signatures(instance) == {"tt0001:x"}


def test_in_flight_signatures_excludes_terminal_runs() -> None:
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
                "dagster/sensor_name": _SENSOR_NAME,
                _BACKFILL_SIGNATURE_TAG_KEY: f"tt0001:{status.value}",
            },
        )
    assert _in_flight_signatures(instance) == set()


def test_in_flight_signatures_ignores_runs_from_other_sensors() -> None:
    instance = dg.DagsterInstance.ephemeral()
    create_run_for_test(
        instance,
        status=dg.DagsterRunStatus.STARTED,
        tags={
            "dagster/sensor_name": "some_other_sensor",
            _BACKFILL_SIGNATURE_TAG_KEY: "tt0001:x",
        },
    )
    assert _in_flight_signatures(instance) == set()


def test_previously_registered_partition_missing_files_still_gets_backfilled(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """This is the bug #2 scenario directly: a partition that's neither newly added
    nor removed this tick (already registered, still desired) but has no on-disk files
    -- e.g. because it was stuck forever under AutomationCondition.on_missing()'s
    cold-start blind spot before this sensor took over as the sole trigger. Must still
    be backfilled, not just partitions that changed this tick."""
    _patch_stage_io_managers(mocker, tmp_path)
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, ["tt0001"])
    context = dg.build_sensor_context(instance=instance)

    # No addition, no removal: tt0001 is registered and still desired, unchanged --
    # but its files were never written.
    result = sync_imdb_id_partitions(context, duckdb=_mock_duckdb(mocker, {"tt0001"}))

    assert isinstance(result, dg.SensorResult)
    assert result.dynamic_partitions_requests == []
    run_requests = result.run_requests
    assert run_requests is not None
    backfill = next(r for r in run_requests if r.partition_key == "tt0001")
    assert set(backfill.asset_selection or []) == {
        dg.AssetKey("synopsis"),
        dg.AssetKey("enrichment"),
        dg.AssetKey("embeddings"),
    }
