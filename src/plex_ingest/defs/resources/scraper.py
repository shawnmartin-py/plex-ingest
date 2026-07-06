import dagster as dg

from plex_ingest.lib.adapters.playwright_scraper import PlaywrightSynopsisScraper
from plex_ingest.lib.ports import SynopsisScraper


class ScraperResource(dg.ConfigurableResource):
    """Config + adapter factory only — the IMDB/Wikipedia scraping cascade lives in
    lib/adapters/playwright_scraper.py, behind the SynopsisScraper port, so it can be
    swapped or faked in tests without touching this resource or any asset."""

    def _adapter(self) -> SynopsisScraper:
        return PlaywrightSynopsisScraper()

    def fetch_synopsis(self, imdb_id: str, title: str, year: int) -> str | None:
        return self._adapter().fetch_synopsis(imdb_id, title, year)


defs = dg.Definitions(resources={"scraper": ScraperResource()})
