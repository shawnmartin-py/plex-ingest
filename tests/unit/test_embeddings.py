from typing import cast
from unittest.mock import MagicMock

import dagster as dg
import pytest
from pytest_mock import MockerFixture

from plex_ingest.defs.assets.embeddings import embeddings

# Matches stg_movies_reader._COLUMNS order: imdb_id, title, year, genres, imdb_rating,
# content_rating, thumb_url.
CatalogRow = tuple[str, str, int, list[str], float, str | None, str | None]


def _mock_duckdb(mocker: MockerFixture, row: CatalogRow | None) -> MagicMock:
    mock_duckdb = cast(MagicMock, mocker.MagicMock())
    mock_conn = mock_duckdb.get_connection.return_value.__enter__.return_value
    mock_conn.execute.return_value.fetchone.return_value = row
    return mock_duckdb


def _catalog_row(
    genres: list[str] | None = None, imdb_rating: float = 7.5
) -> CatalogRow:
    return (
        "tt0001",
        "Test Film",
        2020,
        genres or ["Drama"],
        imdb_rating,
        "PG-13",
        None,
    )


def _mock_embeddings(mocker: MockerFixture) -> MagicMock:
    mock_embeddings = cast(MagicMock, mocker.MagicMock())
    mock_embeddings.embed_query.side_effect = lambda text: [len(text)]
    return mock_embeddings


def test_embeds_synopsis_document_and_every_enrichment_section(
    mocker: MockerFixture,
) -> None:
    mock_duckdb = _mock_duckdb(mocker, _catalog_row())
    mock_embeddings = _mock_embeddings(mocker)

    context = dg.build_asset_context(partition_key="tt0001")
    result = cast(
        "dict[str, dict[str, object]]",
        embeddings(
            context,
            "A great film.",
            {"craft": "Craft text.", "meaning": "Meaning text."},
            mock_embeddings,
            mock_duckdb,
        ),
    )

    assert set(result.keys()) == {"synopsis", "craft", "meaning"}


def test_synopsis_document_text_matches_contract_format(mocker: MockerFixture) -> None:
    mock_duckdb = _mock_duckdb(mocker, _catalog_row(genres=["Drama", "Sci-Fi"]))
    mock_embeddings = _mock_embeddings(mocker)

    context = dg.build_asset_context(partition_key="tt0001")
    result = cast(
        "dict[str, dict[str, object]]",
        embeddings(context, "A great film.", {}, mock_embeddings, mock_duckdb),
    )

    assert result["synopsis"]["text"] == (
        "Title: Test Film\nYear: 2020\nIMDb Rating: 7.5\nGenres: Drama, Sci-Fi\n"
        "Synopsis: A great film."
    )


def test_enrichment_section_text_is_embedded_unchanged(mocker: MockerFixture) -> None:
    mock_duckdb = _mock_duckdb(mocker, _catalog_row())
    mock_embeddings = _mock_embeddings(mocker)

    context = dg.build_asset_context(partition_key="tt0001")
    result = cast(
        "dict[str, dict[str, object]]",
        embeddings(
            context,
            "A great film.",
            {"craft": "Craft profile text."},
            mock_embeddings,
            mock_duckdb,
        ),
    )

    assert result["craft"]["text"] == "Craft profile text."


def test_raises_when_synopsis_is_missing(mocker: MockerFixture) -> None:
    mock_duckdb = _mock_duckdb(mocker, _catalog_row())
    mock_embeddings = _mock_embeddings(mocker)

    context = dg.build_asset_context(partition_key="tt0001")
    with pytest.raises(ValueError, match="tt0001"):
        embeddings(context, None, {"craft": "text"}, mock_embeddings, mock_duckdb)


def test_raises_when_no_stg_movies_row(mocker: MockerFixture) -> None:
    mock_duckdb = _mock_duckdb(mocker, None)
    mock_embeddings = _mock_embeddings(mocker)

    context = dg.build_asset_context(partition_key="tt9999")
    with pytest.raises(ValueError, match="tt9999"):
        embeddings(context, "synopsis", {}, mock_embeddings, mock_duckdb)
