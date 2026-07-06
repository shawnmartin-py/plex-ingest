from typing import Any, TypedDict
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from plex_ingest.lib.adapters.gemini_enrichment import (
    KNOWN_RPM_LIMIT,
    DailyQuotaExhaustedError,
    GeminiEnrichmentGenerator,
    _quota_violation,
    _raise_if_daily_quota_exhausted,
)


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


class _FakeClientErrorCause(Exception):
    """Stands in for google.genai.errors.ClientError's `.details`, which
    `_quota_violation` reads off `exc.__cause__` -- must itself be an exception since
    Python requires `__cause__` to derive from BaseException."""

    def __init__(self, details: dict[str, Any]) -> None:
        super().__init__("fake client error")
        self.details = details


def _quota_exhausted_exception(
    quota_value: int,
    *,
    quota_id: str = "GenerateRequestsPerDayPerProjectPerModel-FreeTier",
    model: str = "gemini-3.1-flash-lite",
) -> Exception:
    """A RESOURCE_EXHAUSTED error shaped like the real Gemini API response captured
    in this session's investigation, with a QuotaFailure violation attached to
    __cause__ the way langchain_google_genai's wrapping does."""
    exc = Exception("429 RESOURCE_EXHAUSTED")
    exc.__cause__ = _FakeClientErrorCause(
        {
            "error": {
                "details": [
                    {
                        "@type": "type.googleapis.com/google.rpc.QuotaFailure",
                        "violations": [
                            {
                                "quotaId": quota_id,
                                "quotaValue": str(quota_value),
                                "quotaDimensions": {"model": model},
                            }
                        ],
                    }
                ]
            }
        }
    )
    return exc


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


# --- daily quota vs. per-minute burst ---


def test_generate_section_raises_daily_quota_exhausted_above_rpm_ceiling(
    mocker: MockerFixture,
) -> None:
    generator, _ = make_generator_with_chain(mocker, [_quota_exhausted_exception(500)])
    with pytest.raises(DailyQuotaExhaustedError, match="gemini-3.1-flash-lite"):
        generator.generate_section(**COMMON_KWARGS)


def test_generate_section_retries_when_quota_value_at_rpm_ceiling(
    mocker: MockerFixture,
) -> None:
    mock_sleep = mocker.patch("plex_ingest.lib.adapters.gemini_enrichment.time.sleep")
    generator, _ = make_generator_with_chain(
        mocker, [_quota_exhausted_exception(KNOWN_RPM_LIMIT), "Success"]
    )
    result = generator.generate_section(**COMMON_KWARGS)
    assert result == "Success"
    mock_sleep.assert_called_once()


def test_generate_section_retries_when_quota_value_below_rpm_ceiling(
    mocker: MockerFixture,
) -> None:
    mock_sleep = mocker.patch("plex_ingest.lib.adapters.gemini_enrichment.time.sleep")
    generator, _ = make_generator_with_chain(
        mocker, [_quota_exhausted_exception(10), "Success"]
    )
    result = generator.generate_section(**COMMON_KWARGS)
    assert result == "Success"
    mock_sleep.assert_called_once()


def test_generate_section_retries_when_violation_details_unavailable(
    mocker: MockerFixture,
) -> None:
    """A plain RESOURCE_EXHAUSTED with no __cause__/.details -- e.g. from a
    differently-shaped client error -- must fall back to retrying rather than
    erroring out trying to classify it."""
    mock_sleep = mocker.patch("plex_ingest.lib.adapters.gemini_enrichment.time.sleep")
    generator, _ = make_generator_with_chain(
        mocker, [Exception("429 RESOURCE_EXHAUSTED"), "Success"]
    )
    result = generator.generate_section(**COMMON_KWARGS)
    assert result == "Success"
    mock_sleep.assert_called_once()


def test_quota_violation_returns_none_without_a_cause() -> None:
    assert _quota_violation(Exception("429")) is None


def test_quota_violation_extracts_the_first_violation() -> None:
    exc = _quota_exhausted_exception(42, quota_id="SomeQuotaId", model="a-model")
    violation = _quota_violation(exc)
    assert violation == {
        "quotaId": "SomeQuotaId",
        "quotaValue": "42",
        "quotaDimensions": {"model": "a-model"},
    }


def test_raise_if_daily_quota_exhausted_is_a_noop_on_malformed_quota_value() -> None:
    exc = _quota_exhausted_exception(500)
    assert isinstance(exc.__cause__, _FakeClientErrorCause)
    exc.__cause__.details["error"]["details"][0]["violations"][0]["quotaValue"] = (
        "not-a-number"
    )
    _raise_if_daily_quota_exhausted(exc)  # must not raise
