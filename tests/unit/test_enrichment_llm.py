from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from plex_ingest.defs.resources.enrichment_llm import EnrichmentLLMResource


def test_generate_section_delegates_to_adapter(mocker: MockerFixture) -> None:
    resource = EnrichmentLLMResource()
    mock_adapter: MagicMock = mocker.MagicMock()
    mock_adapter.generate_section.return_value = "Craft profile text."
    resource._adapter = mocker.MagicMock(return_value=mock_adapter)  # type: ignore[method-assign]

    result = resource.generate_section(
        title="Test Film",
        year=2020,
        genres=["Drama"],
        imdb_rating=7.5,
        content_rating="PG-13",
        synopsis="A great film.",
        section="craft",
    )

    assert result == "Craft profile text."
    mock_adapter.generate_section.assert_called_once_with(
        title="Test Film",
        year=2020,
        genres=["Drama"],
        imdb_rating=7.5,
        content_rating="PG-13",
        synopsis="A great film.",
        section="craft",
    )
