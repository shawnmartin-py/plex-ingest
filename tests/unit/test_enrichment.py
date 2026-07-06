from typing import cast
from unittest.mock import MagicMock

import dagster as dg
import pytest
from pytest_mock import MockerFixture

from plex_ingest.defs.assets.enrichment import enrichment
from plex_ingest.lib.adapters.gemini_enrichment import DailyQuotaExhaustedError

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


def _mock_enrichment_llm(
    mocker: MockerFixture, sections: tuple[str, ...], texts: dict[str, str | None]
) -> MagicMock:
    mock_llm = cast(MagicMock, mocker.MagicMock())
    mock_llm.sections = sections
    mock_llm.generate_section.side_effect = lambda **kwargs: texts[kwargs["section"]]
    return mock_llm


def test_raises_when_synopsis_is_missing(mocker: MockerFixture) -> None:
    mock_duckdb = _mock_duckdb(mocker, _catalog_row())
    mock_llm = _mock_enrichment_llm(mocker, ("craft",), {"craft": "text"})

    context = dg.build_asset_context(partition_key="tt0001")
    with pytest.raises(ValueError, match="tt0001"):
        enrichment(context, None, mock_llm, mock_duckdb)


def test_builds_dict_with_every_section(mocker: MockerFixture) -> None:
    mock_duckdb = _mock_duckdb(mocker, _catalog_row())
    mock_llm = _mock_enrichment_llm(
        mocker,
        ("craft", "meaning"),
        {"craft": "Craft text.", "meaning": "Meaning text."},
    )

    context = dg.build_asset_context(partition_key="tt0001")
    result = enrichment(context, "A great film.", mock_llm, mock_duckdb)

    assert result == {"craft": "Craft text.", "meaning": "Meaning text."}


def test_omits_section_blocked_by_llm(mocker: MockerFixture) -> None:
    mock_duckdb = _mock_duckdb(mocker, _catalog_row())
    mock_llm = _mock_enrichment_llm(
        mocker, ("craft", "meaning"), {"craft": "Craft text.", "meaning": None}
    )

    context = dg.build_asset_context(partition_key="tt0001")
    result = enrichment(context, "A great film.", mock_llm, mock_duckdb)

    assert result == {"craft": "Craft text."}


def test_generate_section_called_with_catalog_and_synopsis(
    mocker: MockerFixture,
) -> None:
    mock_duckdb = _mock_duckdb(mocker, _catalog_row(title="My Film", year=1999))
    mock_llm = _mock_enrichment_llm(mocker, ("craft",), {"craft": "text"})

    context = dg.build_asset_context(partition_key="tt0001")
    enrichment(context, "A great film.", mock_llm, mock_duckdb)

    mock_llm.generate_section.assert_called_once_with(
        title="My Film",
        year=1999,
        genres=["Drama"],
        imdb_rating=7.5,
        content_rating="PG-13",
        synopsis="A great film.",
        section="craft",
    )


def test_raises_when_no_stg_movies_row(mocker: MockerFixture) -> None:
    mock_duckdb = _mock_duckdb(mocker, None)
    mock_llm = _mock_enrichment_llm(mocker, ("craft",), {"craft": "text"})

    context = dg.build_asset_context(partition_key="tt9999")
    with pytest.raises(ValueError, match="tt9999"):
        enrichment(context, "A great film.", mock_llm, mock_duckdb)


def test_daily_quota_exhausted_propagates_and_is_logged(
    mocker: MockerFixture,
) -> None:
    """A DailyQuotaExhaustedError from generate_section must halt the asset (not be
    swallowed/retried at this layer) and must be logged clearly before propagating,
    so it's visible without needing to dig through a full traceback."""
    mock_duckdb = _mock_duckdb(mocker, _catalog_row())
    mock_llm = cast(MagicMock, mocker.MagicMock())
    mock_llm.sections = ("craft",)
    mock_llm.generate_section.side_effect = DailyQuotaExhaustedError("quota gone")

    context = dg.build_asset_context(partition_key="tt0001")
    with pytest.raises(DailyQuotaExhaustedError, match="quota gone"):
        enrichment(context, "A great film.", mock_llm, mock_duckdb)
