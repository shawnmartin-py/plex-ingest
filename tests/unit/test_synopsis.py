from typing import cast
from unittest.mock import MagicMock

import dagster as dg
import pytest
from pytest_mock import MockerFixture

from plex_ingest.defs.assets.synopsis import synopsis

# Matches stg_movies_reader._COLUMNS order: imdb_id, title, year, genres, imdb_rating,
# content_rating, thumb_url.
CatalogRow = tuple[str, str, int, list[str], float, str | None, str | None]


def _catalog_row(
    imdb_id: str = "tt0001", title: str = "Test Film", year: int = 2020
) -> CatalogRow:
    return (imdb_id, title, year, ["Drama"], 7.5, "PG-13", None)


def _mock_duckdb(mocker: MockerFixture, row: CatalogRow | None) -> MagicMock:
    mock_duckdb = cast(MagicMock, mocker.MagicMock())
    mock_conn = mock_duckdb.get_connection.return_value.__enter__.return_value
    mock_conn.execute.return_value.fetchone.return_value = row
    return mock_duckdb


def test_returns_scraped_text(mocker: MockerFixture) -> None:
    mock_duckdb = _mock_duckdb(mocker, _catalog_row())
    mock_scraper = mocker.MagicMock()
    mock_scraper.fetch_synopsis.return_value = "A great film."

    context = dg.build_asset_context(partition_key="tt0001")
    result = synopsis(context, mock_scraper, mock_duckdb)

    assert result == "A great film."


def test_returns_none_when_scraper_finds_nothing(mocker: MockerFixture) -> None:
    mock_duckdb = _mock_duckdb(mocker, _catalog_row())
    mock_scraper = mocker.MagicMock()
    mock_scraper.fetch_synopsis.return_value = None

    context = dg.build_asset_context(partition_key="tt0001")
    result = synopsis(context, mock_scraper, mock_duckdb)

    assert result is None


def test_scraper_called_with_catalog_title_and_year(mocker: MockerFixture) -> None:
    mock_duckdb = _mock_duckdb(mocker, _catalog_row(title="My Film", year=1999))
    mock_scraper = mocker.MagicMock()
    mock_scraper.fetch_synopsis.return_value = "text"

    context = dg.build_asset_context(partition_key="tt0001")
    synopsis(context, mock_scraper, mock_duckdb)

    mock_scraper.fetch_synopsis.assert_called_once_with("tt0001", "My Film", 1999)


def test_raises_when_no_stg_movies_row(mocker: MockerFixture) -> None:
    mock_duckdb = _mock_duckdb(mocker, None)
    mock_scraper = mocker.MagicMock()

    context = dg.build_asset_context(partition_key="tt9999")
    with pytest.raises(ValueError, match="tt9999"):
        synopsis(context, mock_scraper, mock_duckdb)
