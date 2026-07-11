from typing import Any
from unittest.mock import MagicMock

import groq
import httpx
import pytest
from pytest_mock import MockerFixture

from plex_ingest.lib.adapters.groq_synopsis_judge import (
    SYNOPSIS_EXCERPT_CHARS,
    GroqSynopsisJudge,
    _parse_verdict,
)
from plex_ingest.lib.ports import SynopsisMatchResult


def _rate_limit_error(retry_after: str | None = None) -> groq.RateLimitError:
    request = httpx.Request("POST", "https://api.groq.com/openai/v1/chat/completions")
    headers = {"retry-after": retry_after} if retry_after else {}
    response = httpx.Response(429, request=request, headers=headers)
    return groq.RateLimitError("rate limited", response=response, body=None)


def make_judge_with_chain(
    mocker: MockerFixture, invoke_side_effect: list[object]
) -> tuple[GroqSynopsisJudge, MagicMock]:
    judge = GroqSynopsisJudge()
    mock_chain = mocker.MagicMock()
    mock_chain.invoke.side_effect = invoke_side_effect
    judge._chain = mocker.MagicMock(return_value=mock_chain)  # type: ignore[method-assign]
    return judge, mock_chain


# --- _parse_verdict ---


def test_parse_verdict_match() -> None:
    result = _parse_verdict("MATCH: plot lines up with the film.", "Test Film", 2020)
    assert result == SynopsisMatchResult(
        matches=True, reason="plot lines up with the film."
    )


def test_parse_verdict_mismatch() -> None:
    result = _parse_verdict("MISMATCH: this is about a different sequel.", "X", 2020)
    assert result == SynopsisMatchResult(
        matches=False, reason="this is about a different sequel."
    )


def test_parse_verdict_is_case_insensitive() -> None:
    result = _parse_verdict("mismatch: wrong film", "X", 2020)
    assert result.matches is False


def test_parse_verdict_raises_on_unparseable_response() -> None:
    with pytest.raises(ValueError, match="Could not parse"):
        _parse_verdict("I'm not sure about this one.", "Test Film", 2020)


# --- GroqSynopsisJudge.check ---


def test_check_returns_match_result(mocker: MockerFixture) -> None:
    judge, _ = make_judge_with_chain(mocker, ["MATCH: consistent plot."])
    result = judge.check(title="Test Film", year=2020, synopsis="A great film.")
    assert result == SynopsisMatchResult(matches=True, reason="consistent plot.")


def test_check_returns_mismatch_result(mocker: MockerFixture) -> None:
    judge, _ = make_judge_with_chain(mocker, ["MISMATCH: wrong entry in franchise."])
    result = judge.check(title="Test Film", year=2020, synopsis="A great film.")
    assert result.matches is False


def test_check_truncates_synopsis_to_excerpt_length(mocker: MockerFixture) -> None:
    judge, mock_chain = make_judge_with_chain(mocker, ["MATCH: ok"])
    long_synopsis = "x" * (SYNOPSIS_EXCERPT_CHARS * 2)
    judge.check(title="Test Film", year=2020, synopsis=long_synopsis)
    sent_excerpt = mock_chain.invoke.call_args[0][0]["excerpt"]
    assert len(sent_excerpt) == SYNOPSIS_EXCERPT_CHARS


def test_check_retries_on_rate_limit_error(mocker: MockerFixture) -> None:
    mock_sleep = mocker.patch("plex_ingest.lib.adapters.groq_synopsis_judge.time.sleep")
    judge, mock_chain = make_judge_with_chain(
        mocker, [_rate_limit_error(), "MATCH: ok"]
    )
    result = judge.check(title="Test Film", year=2020, synopsis="A great film.")
    assert result.matches is True
    mock_sleep.assert_called_once()
    assert mock_chain.invoke.call_count == 2


def test_check_honors_retry_after_header(mocker: MockerFixture) -> None:
    mock_sleep = mocker.patch("plex_ingest.lib.adapters.groq_synopsis_judge.time.sleep")
    judge, _ = make_judge_with_chain(mocker, [_rate_limit_error("7"), "MATCH: ok"])
    judge.check(title="Test Film", year=2020, synopsis="A great film.")
    mock_sleep.assert_called_once_with(7.0)


def test_check_raises_after_max_attempts_of_rate_limiting(
    mocker: MockerFixture,
) -> None:
    mocker.patch("plex_ingest.lib.adapters.groq_synopsis_judge.time.sleep")
    side_effect: list[Any] = [_rate_limit_error() for _ in range(10)]
    judge, _ = make_judge_with_chain(mocker, side_effect)
    with pytest.raises(RuntimeError, match="rate limit exceeded"):
        judge.check(title="Test Film", year=2020, synopsis="A great film.")


def test_check_raises_value_error_on_unparseable_response(
    mocker: MockerFixture,
) -> None:
    judge, _ = make_judge_with_chain(mocker, ["not a verdict at all"])
    with pytest.raises(ValueError, match="Could not parse"):
        judge.check(title="Test Film", year=2020, synopsis="A great film.")
