import random
import re
import time
from collections.abc import Iterator
from contextlib import contextmanager

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import BrowserContext, Page, sync_playwright

# Cascade ported from plex-rag's app/synopsis.py + app/browser.py: IMDB plot summary,
# then Wikipedia's plot section, then IMDB's shorter description, first hit wins.
IMDB_PLOTSUMMARY_URL = "https://www.imdb.com/title/{imdb_id}/plotsummary"
IMDB_TITLE_URL = "https://www.imdb.com/title/{imdb_id}/"
WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
WIKIPEDIA_HEADERS = {"User-Agent": "plex-ingest-synopsis-bot/1.0"}


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


def _titles_match(movie_title: str, wiki_title: str) -> bool:
    """Return True if wiki_title plausibly refers to the same film as movie_title."""

    def _normalize(s: str) -> str:
        s = re.sub(r"\([^)]*\)", "", s)  # drop "(film)", "(2019 film)", etc.
        return re.sub(r"[^a-z0-9 ]", "", s.lower()).strip()

    movie_norm = _normalize(movie_title)
    wiki_norm = _normalize(wiki_title)
    return movie_norm in wiki_norm or wiki_norm in movie_norm


def _fetch_wikipedia(title: str, year: int) -> str | None:
    # A Wikipedia hiccup (network error, unexpected response shape) should fall through
    # to the next cascade step, not fail the whole partition — same as legacy behavior.
    try:
        search_params: dict[str, str | int] = {
            "action": "query",
            "list": "search",
            "srsearch": f"{title} {year} film",
            "format": "json",
            "srlimit": 3,
        }
        search = requests.get(
            WIKIPEDIA_API,
            headers=WIKIPEDIA_HEADERS,
            params=search_params,
            timeout=10,
        ).json()

        results = search.get("query", {}).get("search", [])
        if not results:
            return None

        page_title = next(
            (r["title"] for r in results if _titles_match(title, r["title"])),
            None,
        )
        if page_title is None:
            return None

        extract_params: dict[str, str | bool] = {
            "action": "query",
            "titles": page_title,
            "prop": "extracts",
            "explaintext": True,
            "format": "json",
        }
        extract = requests.get(
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
    except (requests.RequestException, KeyError, StopIteration, ValueError):
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
            if synopsis:
                return synopsis

            synopsis = _fetch_wikipedia(title, year)
            if synopsis:
                return synopsis

            return _fetch_imdb_description(page, imdb_id)
