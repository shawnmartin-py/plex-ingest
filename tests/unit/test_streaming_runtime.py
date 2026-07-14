from pathlib import Path
from typing import cast

import dagster as dg
import httpx
from dagster_duckdb import DuckDBResource
from pytest_mock import MockerFixture

from plex_ingest.defs.assets.streaming_runtime import streaming_runtime
from plex_ingest.lib.streaming_runtime_store import TABLE_NAME


def _duckdb_with_stg_movies(
    tmp_path: Path,
    placeholder_imdb_ids: list[str],
    real_imdb_ids: list[str] | None = None,
) -> DuckDBResource:
    """Real DuckDB, not mocked — this asset's correctness lives in its SQL (which
    imdb_ids count as "missing", the upsert's ON CONFLICT DO NOTHING), the same reason
    stg_watch_history's tests use real DuckDB rather than a mock."""
    resource = DuckDBResource(database=str(tmp_path / "test.duckdb"))
    with resource.get_connection() as conn:
        conn.execute(
            "CREATE TABLE stg_movies (imdb_id VARCHAR, source_platform VARCHAR)"
        )
        for imdb_id in placeholder_imdb_ids:
            conn.execute("INSERT INTO stg_movies VALUES (?, 'Netflix')", [imdb_id])
        for imdb_id in real_imdb_ids or []:
            conn.execute("INSERT INTO stg_movies VALUES (?, NULL)", [imdb_id])
    return resource


def test_no_op_when_omdb_not_configured(tmp_path: Path, mocker: MockerFixture) -> None:
    duckdb_resource = _duckdb_with_stg_movies(tmp_path, ["tt0001"])
    mock_omdb = mocker.MagicMock()
    mock_omdb.is_configured.return_value = False

    result = cast(
        dg.MaterializeResult,
        streaming_runtime(dg.build_asset_context(), mock_omdb, duckdb_resource),
    )

    mock_omdb.fetch_runtime_minutes.assert_not_called()
    assert result.metadata is not None
    assert result.metadata["omdb_configured"] is False
    with duckdb_resource.get_connection() as conn:
        row = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()  # noqa: S608
    assert row == (0,)


def test_resolves_runtime_only_for_streaming_placeholders(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    duckdb_resource = _duckdb_with_stg_movies(
        tmp_path, ["tt0001"], real_imdb_ids=["tt0002"]
    )
    mock_omdb = mocker.MagicMock()
    mock_omdb.is_configured.return_value = True
    mock_omdb.fetch_runtime_minutes.return_value = 104

    streaming_runtime(dg.build_asset_context(), mock_omdb, duckdb_resource)

    mock_omdb.fetch_runtime_minutes.assert_called_once_with("tt0001")
    with duckdb_resource.get_connection() as conn:
        row = conn.execute(
            f"SELECT runtime_minutes FROM {TABLE_NAME} WHERE imdb_id = 'tt0001'"  # noqa: S608
        ).fetchone()
    assert row == (104,)


def test_already_cached_imdb_id_is_not_refetched(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    duckdb_resource = _duckdb_with_stg_movies(tmp_path, ["tt0001"])
    mock_omdb = mocker.MagicMock()
    mock_omdb.is_configured.return_value = True
    mock_omdb.fetch_runtime_minutes.return_value = 104
    streaming_runtime(dg.build_asset_context(), mock_omdb, duckdb_resource)

    mock_omdb.reset_mock()
    streaming_runtime(dg.build_asset_context(), mock_omdb, duckdb_resource)

    mock_omdb.fetch_runtime_minutes.assert_not_called()


def test_a_definitive_not_found_result_is_cached(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """OMDb reaching a real "no runtime for this title" answer is a fact that never
    changes, so it's cached (unlike a transport error — see below)."""
    duckdb_resource = _duckdb_with_stg_movies(tmp_path, ["tt0001"])
    mock_omdb = mocker.MagicMock()
    mock_omdb.is_configured.return_value = True
    mock_omdb.fetch_runtime_minutes.return_value = None

    streaming_runtime(dg.build_asset_context(), mock_omdb, duckdb_resource)

    with duckdb_resource.get_connection() as conn:
        row = conn.execute(
            f"SELECT imdb_id, runtime_minutes FROM {TABLE_NAME}"  # noqa: S608
        ).fetchone()
    assert row == ("tt0001", None)


def test_a_transport_error_is_not_cached_and_is_retried_next_run(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    duckdb_resource = _duckdb_with_stg_movies(tmp_path, ["tt0001"])
    mock_omdb = mocker.MagicMock()
    mock_omdb.is_configured.return_value = True
    mock_omdb.fetch_runtime_minutes.side_effect = httpx.ConnectError("boom")

    streaming_runtime(dg.build_asset_context(), mock_omdb, duckdb_resource)

    with duckdb_resource.get_connection() as conn:
        count = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()  # noqa: S608
    assert count == (0,)

    mock_omdb.reset_mock()
    mock_omdb.fetch_runtime_minutes.side_effect = None
    mock_omdb.fetch_runtime_minutes.return_value = 90
    streaming_runtime(dg.build_asset_context(), mock_omdb, duckdb_resource)

    mock_omdb.fetch_runtime_minutes.assert_called_once_with("tt0001")
