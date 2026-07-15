from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import dagster as dg
from dagster_duckdb import DuckDBResource
from pytest_mock import MockerFixture

from plex_ingest.defs.assets.stg_watch_history import stg_watch_history
from plex_ingest.lib.ports import ResolvedWatchedMovie, WatchHistoryEntry

_NOW = datetime(2026, 7, 12, tzinfo=UTC).replace(tzinfo=None)


def _history_entry(
    title: str, days_ago: int, rating_key: str | None = None
) -> WatchHistoryEntry:
    return WatchHistoryEntry(
        title=title,
        originally_available_at=(_NOW - timedelta(days=days_ago * 10)).date(),
        viewed_at=_NOW - timedelta(days=days_ago),
        rating_key=rating_key,
    )


def _resolved(tmdb_id: str, title: str = "Test Film") -> ResolvedWatchedMovie:
    return ResolvedWatchedMovie(
        tmdb_id=tmdb_id,
        imdb_id="tt0001",
        title=title,
        year=2020,
        genres=["Drama"],
        imdb_rating=7.5,
        summary="A summary.",
    )


def _mock_duckdb(mocker: MockerFixture) -> MagicMock:
    mock_duckdb = cast(MagicMock, mocker.MagicMock())
    mock_conn = mock_duckdb.get_connection.return_value.__enter__.return_value
    mock_conn.execute.return_value.fetchone.return_value = (1,)
    return mock_duckdb


def test_only_resolves_and_writes_entries_within_the_window(
    mocker: MockerFixture,
) -> None:
    mock_plex = mocker.MagicMock()
    mock_plex.fetch_history.return_value = [
        _history_entry("Recent", days_ago=10),
        _history_entry("Old", days_ago=90),
    ]
    mock_plex.resolve.return_value = _resolved("101", "Recent")
    mock_duckdb = _mock_duckdb(mocker)

    stg_watch_history(dg.build_asset_context(), mock_plex, mock_duckdb)

    mock_plex.resolve.assert_called_once()
    assert mock_plex.resolve.call_args[0][0] == "Recent"


def test_dedupes_by_tmdb_id_keeping_most_recent_viewed_at(
    mocker: MockerFixture,
) -> None:
    mock_plex = mocker.MagicMock()
    mock_plex.fetch_history.return_value = [
        _history_entry("Rewatched", days_ago=30),
        _history_entry("Rewatched", days_ago=5),
    ]
    mock_plex.resolve.return_value = _resolved("101", "Rewatched")
    mock_duckdb = _mock_duckdb(mocker)
    mock_conn = mock_duckdb.get_connection.return_value.__enter__.return_value

    stg_watch_history(dg.build_asset_context(), mock_plex, mock_duckdb)

    (_sql, rows), _ = mock_conn.executemany.call_args
    assert len(rows) == 1
    written_viewed_at = rows[0][7]
    assert written_viewed_at == _NOW - timedelta(days=5)


def test_skips_unresolvable_entries_without_failing(mocker: MockerFixture) -> None:
    mock_plex = mocker.MagicMock()
    mock_plex.fetch_history.return_value = [
        _history_entry("Resolvable", days_ago=10),
        _history_entry("Unresolvable", days_ago=10),
    ]
    mock_plex.resolve.side_effect = [_resolved("101", "Resolvable"), None]
    mock_duckdb = _mock_duckdb(mocker)
    mock_conn = mock_duckdb.get_connection.return_value.__enter__.return_value

    result = cast(
        dg.MaterializeResult,
        stg_watch_history(dg.build_asset_context(), mock_plex, mock_duckdb),
    )

    (_sql, rows), _ = mock_conn.executemany.call_args
    assert len(rows) == 1
    assert result.metadata is not None
    assert result.metadata["skipped"] == 1


def test_written_row_carries_resolved_fields_and_original_title_dropped(
    mocker: MockerFixture,
) -> None:
    mock_plex = mocker.MagicMock()
    mock_plex.fetch_history.return_value = [
        _history_entry("Original Title", days_ago=1)
    ]
    mock_plex.resolve.return_value = ResolvedWatchedMovie(
        tmdb_id="101",
        imdb_id="tt0001",
        title="Resolved Title",
        year=1999,
        genres=["Comedy", "Drama"],
        imdb_rating=None,
        summary="Resolved summary.",
    )
    mock_duckdb = _mock_duckdb(mocker)
    mock_conn = mock_duckdb.get_connection.return_value.__enter__.return_value

    stg_watch_history(dg.build_asset_context(), mock_plex, mock_duckdb)

    (_sql, rows), _ = mock_conn.executemany.call_args
    row = rows[0]
    assert row == (
        "101",
        "tt0001",
        "Resolved Title",
        1999,
        ["Comedy", "Drama"],
        None,
        "Resolved summary.",
        _NOW - timedelta(days=1),
    )


def test_row_survives_a_later_run_whose_window_no_longer_includes_it(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """Real DuckDB, not mocked -- this is the property the upsert SQL exists for: a
    tmdb_id already in the table must not be dropped just because a later run's fetch
    window doesn't include it anymore (partitions are add-only; the embeddings/
    qdrant_collection assets still need this row long after it ages out)."""
    duckdb_resource = DuckDBResource(database=str(tmp_path / "test.duckdb"))

    mock_plex = mocker.MagicMock()
    mock_plex.fetch_history.return_value = [_history_entry("Old Watch", days_ago=10)]
    mock_plex.resolve.return_value = _resolved("101", "Old Watch")
    stg_watch_history(dg.build_asset_context(), mock_plex, duckdb_resource)

    # Second run: history no longer includes 101 at all (aged out of the window).
    mock_plex.fetch_history.return_value = []
    stg_watch_history(dg.build_asset_context(), mock_plex, duckdb_resource)

    with duckdb_resource.get_connection() as conn:
        row = conn.execute(
            "SELECT tmdb_id, title FROM stg_watch_history WHERE tmdb_id = '101'"
        ).fetchone()
    assert row == ("101", "Old Watch")


def test_upsert_does_not_clobber_a_newer_row_with_an_older_one(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """Real DuckDB -- guards the upsert's WHERE clause against an out-of-order run
    (e.g. a retried/delayed materialization) overwriting a newer last_viewed_at."""
    duckdb_resource = DuckDBResource(database=str(tmp_path / "test.duckdb"))

    mock_plex = mocker.MagicMock()
    mock_plex.fetch_history.return_value = [_history_entry("Newer Watch", days_ago=1)]
    mock_plex.resolve.return_value = _resolved("101", "Newer Watch")
    stg_watch_history(dg.build_asset_context(), mock_plex, duckdb_resource)

    # A later run resolves the same tmdb_id but with an older viewed_at (e.g. a
    # delayed retry of a stale fetch) -- must not overwrite the newer row.
    mock_plex.fetch_history.return_value = [_history_entry("Older Watch", days_ago=30)]
    mock_plex.resolve.return_value = _resolved("101", "Older Watch")
    stg_watch_history(dg.build_asset_context(), mock_plex, duckdb_resource)

    with duckdb_resource.get_connection() as conn:
        row = conn.execute(
            "SELECT tmdb_id, title FROM stg_watch_history WHERE tmdb_id = '101'"
        ).fetchone()
    assert row == ("101", "Newer Watch")
