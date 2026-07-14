import re

import httpx

OMDB_URL = "https://www.omdbapi.com/"

# OMDb's Runtime field is a free-text string like "104 min", or the literal "N/A" when
# unknown — not a separate structured field, so it has to be parsed out.
_RUNTIME_RE = re.compile(r"(\d+)\s*min")


class OmdbRuntimeLookup:
    """Implements the RuntimeLookup port (see lib/ports.py). Legitimate, free, licensed
    API lookup by imdb_id — chosen deliberately over scraping another IMDb page (unlike
    the existing synopsis cascade in playwright_scraper.py) specifically for this
    smaller, well-defined need. See docs/vector-store-contract.md's "runtime_minutes"
    note for why this only ever runs for streaming-platform placeholder movies."""

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key

    def fetch_runtime_minutes(self, imdb_id: str) -> int | None:
        """`None` means OMDb was reached and gave a definitive "no runtime" answer
        (unknown title, or a Runtime field it can't parse) — a real, cacheable result.
        A network/transport failure raises instead of returning `None`, so a caller
        can tell "OMDb says there's no data" apart from "we couldn't ask OMDb right
        now" and only cache the former (see streaming_runtime asset)."""
        response = httpx.get(
            OMDB_URL,
            params={"i": imdb_id, "apikey": self._api_key},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        if data.get("Response") != "True":
            return None

        match = _RUNTIME_RE.search(data.get("Runtime", ""))
        return int(match.group(1)) if match else None
