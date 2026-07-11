from unittest.mock import MagicMock

import httpx
from pytest_mock import MockerFixture

from plex_ingest.lib.adapters.playwright_scraper import _fetch_wikipedia, _titles_match

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


def test_titles_match_movie_title_contained_in_wiki_title() -> None:
    assert _titles_match("The Dark Knight", "The Dark Knight Rises") is True


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
