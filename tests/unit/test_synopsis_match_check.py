from typing import cast
from unittest.mock import MagicMock

import dagster as dg
import pytest
from pytest_mock import MockerFixture

from plex_ingest.defs.checks.synopsis_match import synopsis_matches_movie
from plex_ingest.lib.ports import SynopsisMatchResult

CatalogRow = tuple[
    str,
    str,
    int,
    list[str],
    float,
    str | None,
    str | None,
    str | None,
    str | None,
    list[str],
    str | None,
    int | None,
]


def _check_context(partition_key: str) -> dg.AssetCheckExecutionContext:
    """dg.build_asset_check_context() doesn't take a partition_key (unlike
    dg.build_asset_context() for plain assets), so build the underlying op context
    with one directly and wrap it the same way build_asset_check_context() does."""
    op_context = dg.build_op_context(partition_key=partition_key)
    context_cls = type(dg.build_asset_check_context())
    return cast(
        dg.AssetCheckExecutionContext,
        context_cls(op_execution_context=op_context),
    )


def _catalog_row(
    imdb_id: str = "tt0001", title: str = "Test Film", year: int = 2020
) -> CatalogRow:
    return (
        imdb_id,
        title,
        year,
        ["Drama"],
        7.5,
        "PG-13",
        None,
        None,
        None,
        [],
        None,
        None,
    )


def _mock_duckdb(mocker: MockerFixture, row: CatalogRow | None) -> MagicMock:
    mock_duckdb = cast(MagicMock, mocker.MagicMock())
    mock_conn = mock_duckdb.get_connection.return_value.__enter__.return_value
    mock_conn.execute.return_value.fetchone.return_value = row
    return mock_duckdb


def test_passes_without_calling_judge_when_no_synopsis(mocker: MockerFixture) -> None:
    mock_duckdb = _mock_duckdb(mocker, _catalog_row())
    mock_judge = mocker.MagicMock()
    context = _check_context("tt0001")

    result = cast(
        dg.AssetCheckResult,
        synopsis_matches_movie(context, None, mock_judge, mock_duckdb),
    )

    assert result.passed is True
    mock_judge.check.assert_not_called()


def test_passes_when_judge_finds_a_match(mocker: MockerFixture) -> None:
    mock_duckdb = _mock_duckdb(mocker, _catalog_row(title="My Film", year=1999))
    mock_judge = mocker.MagicMock()
    mock_judge.check.return_value = SynopsisMatchResult(
        matches=True, reason="matches the film"
    )
    context = _check_context("tt0001")

    result = cast(
        dg.AssetCheckResult,
        synopsis_matches_movie(context, "A great film.", mock_judge, mock_duckdb),
    )

    assert result.passed is True
    assert result.severity == dg.AssetCheckSeverity.ERROR
    mock_judge.check.assert_called_once_with(
        title="My Film", year=1999, synopsis="A great film."
    )


def test_fails_when_judge_finds_a_mismatch(mocker: MockerFixture) -> None:
    mock_duckdb = _mock_duckdb(mocker, _catalog_row())
    mock_judge = mocker.MagicMock()
    mock_judge.check.return_value = SynopsisMatchResult(
        matches=False, reason="describes a different film"
    )
    context = _check_context("tt0001")

    result = cast(
        dg.AssetCheckResult,
        synopsis_matches_movie(
            context, "Wrong synopsis text.", mock_judge, mock_duckdb
        ),
    )

    assert result.passed is False
    assert result.metadata["reason"].value == "describes a different film"


def test_raises_when_no_stg_movies_row(mocker: MockerFixture) -> None:
    mock_duckdb = _mock_duckdb(mocker, None)
    mock_judge = mocker.MagicMock()
    context = _check_context("tt9999")

    with pytest.raises(ValueError, match="tt9999"):
        synopsis_matches_movie(
            context,
            "Some text.",
            mock_judge,
            mock_duckdb,
        )
