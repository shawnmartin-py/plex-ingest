from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import dagster as dg
from pytest_mock import MockerFixture

from plex_ingest.defs.resources.partition_json_io_manager import JsonPartitionIOManager
from plex_ingest.defs.sensors.sync_imdb_id_partitions import (
    _delete_partition_files,
    _missing_stage_assets,
    compute_desired_ids,
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


# --- compute_desired_ids ---


def test_compute_desired_ids_returns_everything_when_no_limit() -> None:
    assert compute_desired_ids({"tt1", "tt2", "tt3"}, limit=None) == {
        "tt1",
        "tt2",
        "tt3",
    }


def test_compute_desired_ids_caps_at_limit() -> None:
    assert len(compute_desired_ids({"tt1", "tt2", "tt3"}, limit=2)) == 2


def test_compute_desired_ids_is_deterministic_across_calls() -> None:
    ids = {"tt3", "tt1", "tt2"}
    assert compute_desired_ids(ids, limit=2) == compute_desired_ids(ids, limit=2)


def test_compute_desired_ids_picks_lowest_sorted_ids() -> None:
    assert compute_desired_ids({"tt3", "tt1", "tt2"}, limit=2) == {"tt1", "tt2"}


def test_compute_desired_ids_limit_larger_than_available_returns_all() -> None:
    assert compute_desired_ids({"tt1", "tt2"}, limit=100) == {"tt1", "tt2"}


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


def test_shrinking_the_limit_removes_previously_registered_ids() -> None:
    # Simulates lowering PLEX_INGEST_PARTITION_LIMIT between sensor ticks.
    current_ids = {"tt1", "tt2", "tt3"}
    registered_ids = compute_desired_ids(current_ids, limit=3)
    new_ids, removed_ids = compute_partition_diff(
        compute_desired_ids(current_ids, limit=1), registered_ids
    )
    assert new_ids == set()
    assert len(removed_ids) == 2


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


def test_addition_backfills_the_new_partition_and_rebuilds_qdrant(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    _patch_stage_io_managers(mocker, tmp_path)
    _write_stage_files(tmp_path, "tt0001")
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, ["tt0001"])
    context = dg.build_sensor_context(instance=instance)

    # tt0002 is new and has no on-disk files -> gets backfilled directly; embeddings
    # is among the missing assets, so qdrant_collection is requested too rather than
    # relying solely on its own eager() (see sync_imdb_id_partitions's docstring).
    result = sync_imdb_id_partitions(
        context, duckdb=_mock_duckdb(mocker, {"tt0001", "tt0002"})
    )

    assert isinstance(result, dg.SensorResult)
    run_requests = result.run_requests
    assert run_requests is not None
    assert len(run_requests) == 2
    backfill = next(r for r in run_requests if r.partition_key == "tt0002")
    assert set(backfill.asset_selection or []) == {
        dg.AssetKey("synopsis"),
        dg.AssetKey("enrichment"),
        dg.AssetKey("embeddings"),
    }
    rebuild = next(r for r in run_requests if r.partition_key is None)
    assert rebuild.asset_selection == [dg.AssetKey("qdrant_collection")]


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
