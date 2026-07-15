from datetime import datetime
from typing import cast
from unittest.mock import MagicMock

import dagster as dg
import pytest
from pytest_mock import MockerFixture

from plex_ingest.defs.assets.watch_history_embeddings import watch_history_embeddings

# Matches watch_history_reader._COLUMNS order: tmdb_id, imdb_id, title, year, genres,
# imdb_rating, summary, last_viewed_at.
WatchHistoryDbRow = tuple[str, str, str, int, list[str], float | None, str, datetime]


def _mock_duckdb(mocker: MockerFixture, row: WatchHistoryDbRow | None) -> MagicMock:
    mock_duckdb = cast(MagicMock, mocker.MagicMock())
    mock_conn = mock_duckdb.get_connection.return_value.__enter__.return_value
    mock_conn.execute.return_value.fetchone.return_value = row
    return mock_duckdb


def _watch_history_row(
    genres: list[str] | None = None, imdb_rating: float | None = 6.5
) -> WatchHistoryDbRow:
    return (
        "456",
        "tt0107315",
        "Kika",
        1993,
        genres or ["Comedy", "Drama"],
        imdb_rating,
        "Kika, a cosmetologist...",
        datetime(2026, 5, 11),
    )


def _mock_embeddings(mocker: MockerFixture) -> MagicMock:
    mock_embeddings = cast(MagicMock, mocker.MagicMock())
    mock_embeddings.embed_query.side_effect = lambda text: [len(text)]
    return mock_embeddings


def test_returns_text_and_vector_for_the_partition(mocker: MockerFixture) -> None:
    mock_duckdb = _mock_duckdb(mocker, _watch_history_row())
    mock_embeddings = _mock_embeddings(mocker)

    context = dg.build_asset_context(partition_key="456")
    result = cast(
        "dict[str, object]",
        watch_history_embeddings(context, mock_embeddings, mock_duckdb),
    )

    assert set(result.keys()) == {"text", "vector"}
    mock_embeddings.embed_query.assert_called_once_with(result["text"])


def test_document_text_matches_synopsis_contract_format(mocker: MockerFixture) -> None:
    mock_duckdb = _mock_duckdb(mocker, _watch_history_row(genres=["Comedy", "Drama"]))
    mock_embeddings = _mock_embeddings(mocker)

    context = dg.build_asset_context(partition_key="456")
    result = cast(
        "dict[str, object]",
        watch_history_embeddings(context, mock_embeddings, mock_duckdb),
    )

    assert result["text"] == (
        "Title: Kika\nYear: 1993\nIMDb Rating: 6.5\nGenres: Comedy, Drama\n"
        "Synopsis: Kika, a cosmetologist..."
    )


def test_raises_when_no_stg_watch_history_row(mocker: MockerFixture) -> None:
    mock_duckdb = _mock_duckdb(mocker, None)
    mock_embeddings = _mock_embeddings(mocker)

    context = dg.build_asset_context(partition_key="9999999")
    with pytest.raises(ValueError, match="9999999"):
        watch_history_embeddings(context, mock_embeddings, mock_duckdb)
