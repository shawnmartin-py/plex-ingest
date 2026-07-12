import random
import re
import time
from collections.abc import Iterator
from contextlib import contextmanager

import httpx
from bs4 import BeautifulSoup
from playwright.sync_api import BrowserContext, Page, sync_playwright

# Cascade ported from plex-rag's app/synopsis.py + app/browser.py: IMDB plot summary,
# then Wikipedia's plot section, then IMDB's shorter description, first hit wins —
# except an IMDB synopsis shorter than SHORT_SYNOPSIS_THRESHOLD is a real section, not
# missing data, but is treated as merely a candidate: Wikipedia's plot is also fetched
# and whichever is longer is used (some legitimately-present IMDB synopses are just a
# terse sentence or two, while Wikipedia's plot for the same film can run to 3-4k
# chars).
IMDB_PLOTSUMMARY_URL = "https://www.imdb.com/title/{imdb_id}/plotsummary"
IMDB_TITLE_URL = "https://www.imdb.com/title/{imdb_id}/"
WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
WIKIPEDIA_HEADERS = {"User-Agent": "plex-ingest-synopsis-bot/1.0"}
SHORT_SYNOPSIS_THRESHOLD = 1000


@contextmanager
def _browser_context() -> Iterator[BrowserContext]:
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        )
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        try:
            yield context
        finally:
            try:
                context.close()
            finally:
                browser.close()


def _fetch_imdb_synopsis(page: Page, imdb_id: str) -> str | None:
    url = IMDB_PLOTSUMMARY_URL.format(imdb_id=imdb_id)
    time.sleep(random.uniform(2, 4))  # noqa: S311
    page.goto(url, wait_until="networkidle")
    page.wait_for_timeout(2000)

    soup = BeautifulSoup(page.content(), "html.parser")
    # bs4 stub overload resolution doesn't cover attrs-only calls
    synopsis_section = soup.find(attrs={"data-testid": "sub-section-synopsis"})  # type: ignore[call-overload]
    if not synopsis_section or isinstance(synopsis_section, str):
        return None

    divs = synopsis_section.find_all("div", class_="ipc-html-content-inner-div")
    if not divs:
        return None

    longest = max(divs, key=lambda d: len(d.get_text(strip=True)))
    text = longest.get_text(strip=True)
    return text or None


_ROMAN_TO_ARABIC = {
    "ii": "2",
    "iii": "3",
    "iv": "4",
    "v": "5",
    "vi": "6",
    "vii": "7",
    "viii": "8",
    "ix": "9",
    "x": "10",
}


def _normalize_title(s: str) -> str:
    s = re.sub(r"\([^)]*\)", "", s)  # drop "(film)", "(2019 film)", etc.
    s = re.sub(r"[^a-z0-9 ]", "", s.lower()).strip()
    # Wikipedia sequel titles often use arabic numerals ("Pusher 3") where our
    # catalog title uses roman ("Pusher III") — canonicalize both to arabic.
    return " ".join(_ROMAN_TO_ARABIC.get(word, word) for word in s.split())


def _disambiguator_year(wiki_title: str) -> int | None:
    match = re.search(r"\((\d{4})", wiki_title)
    return int(match.group(1)) if match else None


def _titles_match(movie_title: str, wiki_title: str) -> bool:
    """Return True if wiki_title refers to the same film as movie_title.

    Exact match only (post-normalization) — substring containment previously let
    "The Beast" match the unrelated "The Beast Within (2024 film)" and let any
    same-titled-but-different film through, since the parenthetical disambiguator
    (which is where the year that would rule out a false match lives) gets
    stripped before comparison.
    """
    return _normalize_title(movie_title) == _normalize_title(wiki_title)


def _fetch_wikipedia(title: str, year: int) -> str | None:
    # A Wikipedia hiccup (network error, unexpected response shape) should fall through
    # to the next cascade step, not fail the whole partition — same as legacy behavior.
    try:
        # `year` comes from stg_movies/Plex metadata, which doesn't always agree with
        # Wikipedia's disambiguation year (festival vs. wide release, regional premiere
        # dates, etc.) — baking it into the full-text search can rank the correct page
        # far outside srlimit (confirmed: a 2-year-off value pushed the right page
        # entirely out of the top 8 results). Search on title alone and use `year` only
        # to disambiguate among exact title matches, below.
        search_params: dict[str, str | int] = {
            "action": "query",
            "list": "search",
            "srsearch": f"{title} film",
            "format": "json",
            "srlimit": 5,
        }
        search = httpx.get(
            WIKIPEDIA_API,
            headers=WIKIPEDIA_HEADERS,
            params=search_params,
            timeout=10,
        ).json()

        results = search.get("query", {}).get("search", [])
        if not results:
            return None

        candidates = [
            r["title"]
            for r in results
            if "(disambiguation)" not in r["title"].lower()
            and _titles_match(title, r["title"])
        ]
        if not candidates:
            return None

        # Among exact title matches, prefer the one whose disambiguator year is
        # closest to the catalog year (a candidate with no year disambiguator at
        # all — i.e. an unambiguous title — is treated as a perfect match).
        page_title = min(
            candidates,
            key=lambda t: abs((_disambiguator_year(t) or year) - year),
        )

        extract_params: dict[str, str | bool] = {
            "action": "query",
            "titles": page_title,
            "prop": "extracts",
            "explaintext": True,
            "format": "json",
        }
        extract = httpx.get(
            WIKIPEDIA_API,
            headers=WIKIPEDIA_HEADERS,
            params=extract_params,
            timeout=10,
        ).json()

        pages = extract.get("query", {}).get("pages", {})
        content: str = next(iter(pages.values())).get("extract", "")

        if "== Plot ==" not in content:
            return None

        plot_start = content.index("== Plot ==") + len("== Plot ==")
        rest = content[plot_start:].strip()
        next_section = rest.find("\n==")
        return rest[:next_section].strip() if next_section > 0 else rest.strip()
    except (httpx.HTTPError, KeyError, StopIteration, ValueError):
        return None


def _fetch_imdb_description(page: Page, imdb_id: str) -> str | None:
    url = IMDB_TITLE_URL.format(imdb_id=imdb_id)
    time.sleep(random.uniform(2, 4))  # noqa: S311
    page.goto(url, wait_until="domcontentloaded")
    page.wait_for_timeout(2000)

    soup = BeautifulSoup(page.content(), "html.parser")
    el = (
        soup.select_one("[data-testid='plot-xl']")
        or soup.select_one("[data-testid='plot-l']")
        or soup.select_one("[data-testid='plot']")
    )
    if el:
        text = el.get_text(strip=True)
        return text or None
    return None


class PlaywrightSynopsisScraper:
    """Implements the `SynopsisScraper` port (see `lib/ports.py`)."""

    def fetch_synopsis(self, imdb_id: str, title: str, year: int) -> str | None:
        with _browser_context() as context:
            page = context.new_page()

            synopsis = _fetch_imdb_synopsis(page, imdb_id)
            if synopsis and len(synopsis) >= SHORT_SYNOPSIS_THRESHOLD:
                return synopsis

            wiki_plot = _fetch_wikipedia(title, year)
            candidates = [c for c in (synopsis, wiki_plot) if c]
            if candidates:
                return max(candidates, key=len)

            return _fetch_imdb_description(page, imdb_id)
