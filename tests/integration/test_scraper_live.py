"""Live canary for playwright_scraper.py's IMDb/Wikipedia scraping selectors.

Excluded from the default `pytest` run (see [tool.pytest.ini_options] `addopts` in
pyproject.toml) -- these hit real external pages, so they're a manual/on-demand check
for DOM or API drift (`pytest -m live`), not part of the normal fast suite. Requires
Playwright's Chromium build installed locally (`uv run playwright install chromium`).

Targets a single long-established title (The Shawshank Redemption, tt0111161) that's
extremely unlikely to be delisted or restructured, so a failure here means IMDb's or
Wikipedia's page structure changed -- not that the title disappeared.
"""

import pytest

from plex_ingest.lib.adapters.playwright_scraper import (
    _browser_context,
    _fetch_imdb_synopsis,
    _fetch_wikipedia,
)

SHAWSHANK_IMDB_ID = "tt0111161"
SHAWSHANK_TITLE = "The Shawshank Redemption"
SHAWSHANK_YEAR = 1994
# A named character mentioned in essentially every version of this plot, IMDb's and
# Wikipedia's alike -- distinguishes "found the real synopsis" from "found some other
# text on the page" (e.g. a cookie-consent banner or empty container).
STABLE_KEYWORD = "Andy"


@pytest.mark.live  # type: ignore[misc]  # custom mark, untyped in pytest's stubs
def test_imdb_synopsis_selectors_match_known_page() -> None:
    with _browser_context() as context:
        page = context.new_page()
        synopsis = _fetch_imdb_synopsis(page, SHAWSHANK_IMDB_ID)

    assert synopsis is not None, (
        "IMDb synopsis scrape returned None for a known-good page -- "
        "data-testid='sub-section-synopsis' or its inner divs likely changed"
    )
    assert len(synopsis) > 200
    assert STABLE_KEYWORD in synopsis


@pytest.mark.live  # type: ignore[misc]  # custom mark, untyped in pytest's stubs
def test_wikipedia_fetch_matches_known_page() -> None:
    plot = _fetch_wikipedia(SHAWSHANK_TITLE, SHAWSHANK_YEAR)

    assert plot is not None, (
        "Wikipedia plot fetch returned None for a known-good page -- "
        "the MediaWiki search/extract API shape likely changed"
    )
    assert len(plot) > 200
    assert STABLE_KEYWORD in plot
