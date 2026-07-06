from datetime import UTC, datetime
from typing import Any

from plexapi.server import PlexServer


class PlexMovieCatalog:
    """Implements the `MovieCatalog` port (see `lib/ports.py`)."""

    def __init__(self, base_url: str, token: str, movie_library: str) -> None:
        self._base_url = base_url
        self._token = token
        self._movie_library = movie_library

    def _server(self) -> PlexServer:
        return PlexServer(baseurl=self._base_url, token=self._token)  # type: ignore[no-untyped-call]

    def fetch_raw_movies(self) -> list[dict[str, Any]]:
        """One row per movie currently in the library, as close to Plex's own shape as
        useful. `guids` is kept as the raw `imdb://...`/`tmdb://...` list rather than
        pre-resolving an imdb_id — that resolution is a staging-layer concern, not a raw
        ingestion one."""
        section = self._server().library.section(self._movie_library)
        synced_at = datetime.now(UTC)
        return [
            {
                "rating_key": item.ratingKey,
                "title": item.title,
                "year": item.year,
                "content_rating": item.contentRating,
                "thumb_url": item.thumbUrl,
                "guids": [guid.id for guid in item.guids],
                "genres": [genre.tag for genre in item.genres],
                "imdb_rating": item.ratings[0].value if item.ratings else None,
                "synced_at": synced_at,
            }
            for item in section.search()
        ]
