from typing import TypedDict
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from plex_ingest.lib.adapters.gemini_enrichment import GeminiEnrichmentGenerator


class _GenerateSectionKwargs(TypedDict):
    title: str
    year: int
    genres: list[str]
    imdb_rating: float | None
    content_rating: str | None
    synopsis: str
    section: str


COMMON_KWARGS: _GenerateSectionKwargs = {
    "title": "Test Film",
    "year": 2020,
    "genres": ["Drama", "Sci-Fi"],
    "imdb_rating": 7.5,
    "content_rating": "PG-13",
    "synopsis": "A great film.",
    "section": "craft",
}


def make_generator_with_chain(
    mocker: MockerFixture, invoke_side_effect: list[object]
) -> tuple[GeminiEnrichmentGenerator, MagicMock]:
    generator = GeminiEnrichmentGenerator()
    mock_chain = mocker.MagicMock()
    mock_chain.invoke.side_effect = invoke_side_effect
    generator._chain = mocker.MagicMock(return_value=mock_chain)  # type: ignore[method-assign]
    return generator, mock_chain


def test_generate_section_returns_chain_output(mocker: MockerFixture) -> None:
    generator, _ = make_generator_with_chain(mocker, ["Craft profile text."])
    result = generator.generate_section(**COMMON_KWARGS)
    assert result == "Craft profile text."


def test_generate_section_joins_genres_as_string(mocker: MockerFixture) -> None:
    generator, mock_chain = make_generator_with_chain(mocker, ["text"])
    generator.generate_section(**COMMON_KWARGS)
    assert mock_chain.invoke.call_args[0][0]["genres"] == "Drama, Sci-Fi"


def test_generate_section_retries_on_429(mocker: MockerFixture) -> None:
    mock_sleep = mocker.patch("plex_ingest.lib.adapters.gemini_enrichment.time.sleep")
    generator, mock_chain = make_generator_with_chain(
        mocker, [Exception("429: quota exceeded"), "Success"]
    )
    result = generator.generate_section(**COMMON_KWARGS)
    assert result == "Success"
    mock_sleep.assert_called_once()
    assert mock_chain.invoke.call_count == 2


def test_generate_section_retries_on_resource_exhausted(mocker: MockerFixture) -> None:
    mock_sleep = mocker.patch("plex_ingest.lib.adapters.gemini_enrichment.time.sleep")
    generator, _ = make_generator_with_chain(
        mocker, [Exception("RESOURCE_EXHAUSTED"), "Success"]
    )
    result = generator.generate_section(**COMMON_KWARGS)
    assert result == "Success"
    mock_sleep.assert_called_once()


def test_generate_section_doubles_delay_on_successive_failures(
    mocker: MockerFixture,
) -> None:
    mock_sleep = mocker.patch("plex_ingest.lib.adapters.gemini_enrichment.time.sleep")
    generator, _ = make_generator_with_chain(
        mocker, [Exception("429"), Exception("429"), "Success"]
    )
    generator.generate_section(**COMMON_KWARGS)
    delays = [call.args[0] for call in mock_sleep.call_args_list]
    assert delays[1] > delays[0]


def test_generate_section_retries_without_synopsis_on_first_empty_response(
    mocker: MockerFixture,
) -> None:
    generator, mock_chain = make_generator_with_chain(
        mocker, ["", "Craft profile text."]
    )
    result = generator.generate_section(**COMMON_KWARGS)
    assert result == "Craft profile text."
    assert mock_chain.invoke.call_count == 2


def test_generate_section_second_call_uses_placeholder_synopsis(
    mocker: MockerFixture,
) -> None:
    generator, mock_chain = make_generator_with_chain(
        mocker, ["", "Craft profile text."]
    )
    generator.generate_section(**COMMON_KWARGS)
    second_call_input = mock_chain.invoke.call_args_list[1][0][0]
    assert second_call_input["synopsis"] == "(synopsis unavailable)"


def test_generate_section_returns_none_when_both_attempts_empty(
    mocker: MockerFixture,
) -> None:
    generator, mock_chain = make_generator_with_chain(mocker, ["", ""])
    assert generator.generate_section(**COMMON_KWARGS) is None
    assert mock_chain.invoke.call_count == 2


def test_generate_section_raises_immediately_on_other_errors(
    mocker: MockerFixture,
) -> None:
    generator, _ = make_generator_with_chain(mocker, [ValueError("Something broke")])
    with pytest.raises(ValueError, match="Something broke"):
        generator.generate_section(**COMMON_KWARGS)
