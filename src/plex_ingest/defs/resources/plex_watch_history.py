from datetime import date

import dagster as dg

from plex_ingest.lib.adapters.plex_watch_history import PlexWatchHistory
from plex_ingest.lib.ports import ResolvedWatchedMovie, WatchHistoryEntry


class PlexWatchHistoryResource(dg.ConfigurableResource):
    """Config + adapter factory only — the PlexServer/MyPlexAccount calls live
    in lib/adapters/plex_watch_history.py, behind the WatchHistorySource and
    WatchedMovieResolver ports."""

    base_url: str = dg.EnvVar("PLEXAPI_AUTH_SERVER_BASEURL")
    token: str = dg.EnvVar("PLEXAPI_AUTH_SERVER_TOKEN")
    movie_library: str = dg.EnvVar("PLEX_MOVIE_LIBRARY")

    def _adapter(self) -> PlexWatchHistory:
        return PlexWatchHistory(
            base_url=self.base_url, token=self.token, movie_library=self.movie_library
        )

    def fetch_history(self) -> list[WatchHistoryEntry]:
        return self._adapter().fetch_history()

    def resolve(
        self, title: str, originally_available_at: date
    ) -> ResolvedWatchedMovie | None:
        return self._adapter().resolve(title, originally_available_at)


defs = dg.Definitions(resources={"plex_watch_history": PlexWatchHistoryResource()})
