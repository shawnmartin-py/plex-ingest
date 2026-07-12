from unittest.mock import MagicMock

import httpx
from pytest_mock import MockerFixture

from plex_ingest.lib.adapters.playwright_scraper import (
    SHORT_SYNOPSIS_THRESHOLD,
    PlaywrightSynopsisScraper,
    _fetch_wikipedia,
    _titles_match,
)

AVENGEMENT_PLOT = (
    "Cain Burgess, a prisoner being escorted to a hospital to learn of his "
    "mother's death, escapes after seeing her corpse."
)

AVENGEMENT_EXTRACT = f"== Plot ==\n{AVENGEMENT_PLOT}\n== Cast ==\nScott Adkins"


def _search_response(mocker: MockerFixture, *titles: str) -> MagicMock:
    resp: MagicMock = mocker.MagicMock()
    resp.json.return_value = {"query": {"search": [{"title": t} for t in titles]}}
    return resp


def _extract_response(
    mocker: MockerFixture, extract: str, page_id: str = "123"
) -> MagicMock:
    resp: MagicMock = mocker.MagicMock()
    resp.json.return_value = {"query": {"pages": {page_id: {"extract": extract}}}}
    return resp


# --- _titles_match ---


def test_titles_match_exact() -> None:
    assert _titles_match("Avengement", "Avengement") is True


def test_titles_match_wiki_has_year_disambiguation() -> None:
    assert _titles_match("Avengement", "Avengement (2019 film)") is True


def test_titles_match_case_insensitive() -> None:
    assert _titles_match("avengement", "AVENGEMENT") is True


def test_titles_match_rejects_unrelated_film() -> None:
    assert _titles_match("Avengement", "The Dark Knight") is False


def test_titles_match_rejects_superstring_title() -> None:
    # Substring containment previously let "The Dark Knight" match "The Dark
    # Knight Rises" — a different film — which is the same bug class that
    # matched "The Beast" to the unrelated "The Beast Within (2024 film)".
    assert _titles_match("The Dark Knight", "The Dark Knight Rises") is False


def test_titles_match_roman_numeral_matches_arabic_numeral() -> None:
    # Wikipedia sequel titles often use arabic numerals ("Pusher 3") where our
    # catalog uses roman ("Pusher III").
    assert _titles_match("Pusher III", "Pusher 3") is True


# --- _fetch_wikipedia ---


def test_fetch_wikipedia_skips_wrong_first_result_and_uses_correct_second(
    mocker: MockerFixture,
) -> None:
    mock_get = mocker.patch("plex_ingest.lib.adapters.playwright_scraper.httpx.get")
    mock_get.side_effect = [
        _search_response(mocker, "Avengers: Endgame", "Avengement"),
        _extract_response(mocker, AVENGEMENT_EXTRACT),
    ]
    result = _fetch_wikipedia("Avengement", 2019)
    assert result == AVENGEMENT_PLOT.strip()


def test_fetch_wikipedia_picks_closest_year_among_same_titled_films(
    mocker: MockerFixture,
) -> None:
    # Regression for the "Submission" bug: stg_movies' year can be off from
    # Wikipedia's disambiguation year, and multiple unrelated films can share
    # an exact title. The closest-year candidate should win rather than
    # whichever ranked first in Wikipedia's free-text search.
    mock_get = mocker.patch("plex_ingest.lib.adapters.playwright_scraper.httpx.get")
    mock_get.side_effect = [
        _search_response(
            mocker,
            "Submission (2004 film)",
            "Submission (1976 film)",
            "Submission (2017 film)",
        ),
        _extract_response(mocker, "== Plot ==\nCorrect plot.\n== Cast ==\nX"),
    ]
    result = _fetch_wikipedia("Submission", 2019)
    assert result == "Correct plot."
    extract_call = mock_get.call_args_list[1]
    assert extract_call.kwargs["params"]["titles"] == "Submission (2017 film)"


def test_fetch_wikipedia_skips_disambiguation_page(mocker: MockerFixture) -> None:
    # "Submission (disambiguation)" title-matches (its year-less title normalizes
    # to a bare "Submission") and would otherwise win the closest-year tiebreak
    # by defaulting to a spurious perfect match — but a disambiguation page is
    # never a film's article, so it must be excluded outright.
    mock_get = mocker.patch("plex_ingest.lib.adapters.playwright_scraper.httpx.get")
    mock_get.side_effect = [
        _search_response(
            mocker,
            "Submission (2004 film)",
            "Submission (2017 film)",
            "Submission (disambiguation)",
        ),
        _extract_response(mocker, "== Plot ==\nCorrect plot.\n== Cast ==\nX"),
    ]
    result = _fetch_wikipedia("Submission", 2019)
    assert result == "Correct plot."
    extract_call = mock_get.call_args_list[1]
    assert extract_call.kwargs["params"]["titles"] == "Submission (2017 film)"


def test_fetch_wikipedia_returns_none_when_no_result_matches(
    mocker: MockerFixture,
) -> None:
    mock_get = mocker.patch("plex_ingest.lib.adapters.playwright_scraper.httpx.get")
    mock_get.side_effect = [
        _search_response(mocker, "Avengers: Endgame", "Avengers: Infinity War"),
    ]
    assert _fetch_wikipedia("Avengement", 2019) is None


def test_fetch_wikipedia_returns_none_when_no_plot_section(
    mocker: MockerFixture,
) -> None:
    mock_get = mocker.patch("plex_ingest.lib.adapters.playwright_scraper.httpx.get")
    extract = "Avengement is a 2019 British action film.\n== Cast ==\nScott Adkins"
    mock_get.side_effect = [
        _search_response(mocker, "Avengement"),
        _extract_response(mocker, extract),
    ]
    assert _fetch_wikipedia("Avengement", 2019) is None


def test_fetch_wikipedia_returns_none_when_search_empty(mocker: MockerFixture) -> None:
    mock_get = mocker.patch("plex_ingest.lib.adapters.playwright_scraper.httpx.get")
    resp = mocker.MagicMock()
    resp.json.return_value = {"query": {"search": []}}
    mock_get.return_value = resp
    assert _fetch_wikipedia("Avengement", 2019) is None


def test_fetch_wikipedia_falls_through_on_request_error(mocker: MockerFixture) -> None:
    # A network hiccup must not raise — the cascade needs to move on to the next
    # fallback (IMDB description), not blow up the whole synopsis asset.
    mock_get = mocker.patch("plex_ingest.lib.adapters.playwright_scraper.httpx.get")
    mock_get.side_effect = httpx.HTTPError("boom")
    assert _fetch_wikipedia("Avengement", 2019) is None


# --- PlaywrightSynopsisScraper.fetch_synopsis ---


def _patch_cascade(
    mocker: MockerFixture,
    *,
    synopsis: str | None,
    wiki: str | None,
    description: str | None,
) -> MagicMock:
    mocker.patch(
        "plex_ingest.lib.adapters.playwright_scraper._browser_context"
    ).return_value.__enter__.return_value = mocker.MagicMock()
    mocker.patch(
        "plex_ingest.lib.adapters.playwright_scraper._fetch_imdb_synopsis",
        return_value=synopsis,
    )
    mock_wiki: MagicMock = mocker.patch(
        "plex_ingest.lib.adapters.playwright_scraper._fetch_wikipedia",
        return_value=wiki,
    )
    mocker.patch(
        "plex_ingest.lib.adapters.playwright_scraper._fetch_imdb_description",
        return_value=description,
    )
    return mock_wiki


def test_fetch_synopsis_uses_long_imdb_synopsis_without_checking_wikipedia(
    mocker: MockerFixture,
) -> None:
    long_synopsis = "x" * SHORT_SYNOPSIS_THRESHOLD
    mock_wiki = _patch_cascade(
        mocker, synopsis=long_synopsis, wiki="should not be used", description=None
    )
    result = PlaywrightSynopsisScraper().fetch_synopsis("tt0001", "Some Film", 2020)
    assert result == long_synopsis
    mock_wiki.assert_not_called()


def test_fetch_synopsis_prefers_longer_wikipedia_plot_over_short_synopsis(
    mocker: MockerFixture,
) -> None:
    short_synopsis = "A terse one-liner."
    long_plot = "y" * (SHORT_SYNOPSIS_THRESHOLD + 500)
    _patch_cascade(mocker, synopsis=short_synopsis, wiki=long_plot, description=None)
    result = PlaywrightSynopsisScraper().fetch_synopsis("tt0001", "Some Film", 2020)
    assert result == long_plot


def test_fetch_synopsis_keeps_short_synopsis_when_wikipedia_shorter(
    mocker: MockerFixture,
) -> None:
    short_synopsis = "A terse one-liner, but still longer than the wiki stub."
    shorter_wiki = "Even shorter."
    _patch_cascade(mocker, synopsis=short_synopsis, wiki=shorter_wiki, description=None)
    result = PlaywrightSynopsisScraper().fetch_synopsis("tt0001", "Some Film", 2020)
    assert result == short_synopsis


def test_fetch_synopsis_keeps_short_synopsis_when_no_wikipedia_plot(
    mocker: MockerFixture,
) -> None:
    short_synopsis = "The only thing we've got."
    _patch_cascade(mocker, synopsis=short_synopsis, wiki=None, description="A tagline.")
    result = PlaywrightSynopsisScraper().fetch_synopsis("tt0001", "Some Film", 2020)
    assert result == short_synopsis


def test_fetch_synopsis_uses_wikipedia_when_no_imdb_synopsis(
    mocker: MockerFixture,
) -> None:
    wiki_plot = "The Wikipedia plot."
    _patch_cascade(mocker, synopsis=None, wiki=wiki_plot, description="A tagline.")
    result = PlaywrightSynopsisScraper().fetch_synopsis("tt0001", "Some Film", 2020)
    assert result == wiki_plot


def test_fetch_synopsis_falls_back_to_description_when_nothing_else_found(
    mocker: MockerFixture,
) -> None:
    description = "A tagline."
    _patch_cascade(mocker, synopsis=None, wiki=None, description=description)
    result = PlaywrightSynopsisScraper().fetch_synopsis("tt0001", "Some Film", 2020)
    assert result == description
