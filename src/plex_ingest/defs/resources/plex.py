from typing import Any

import dagster as dg

from plex_ingest.lib.adapters.plex_catalog import PlexMovieCatalog
from plex_ingest.lib.ports import MovieCatalog


class PlexResource(dg.ConfigurableResource):
    """Config + adapter factory only — the PlexServer client and field mapping live
    in lib/adapters/plex_catalog.py, behind the MovieCatalog port."""

    base_url: str = dg.EnvVar("PLEXAPI_AUTH_SERVER_BASEURL")
    token: str = dg.EnvVar("PLEXAPI_AUTH_SERVER_TOKEN")
    movie_library: str = dg.EnvVar("PLEX_MOVIE_LIBRARY")

    def _adapter(self) -> MovieCatalog:
        return PlexMovieCatalog(
            base_url=self.base_url, token=self.token, movie_library=self.movie_library
        )

    def fetch_raw_movies(self) -> list[dict[str, Any]]:
        return self._adapter().fetch_raw_movies()


defs = dg.Definitions(resources={"plex": PlexResource()})
