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
        ingestion one.

        `video_resolution`/`duration_ms`/`file_path` come from the item's first
        `Media`/`Part` (a movie library item has exactly one of each in this library;
        there's no multi-version support to pick between). This also covers the
        streaming-platform placeholder clips (see docs/vector-store-contract.md) —
        they're real `Movie` items with real `Media`/`Part` data, just a much shorter
        `duration_ms` and a `file_path` naming convention staging parses to detect
        them.

        `view_count` is captured raw (unfiltered) rather than excluding watched movies
        here — staging applies the unwatched-only business rule, the same split already
        used for the imdb_id-required rule, so a movie's watched state stays visible for
        debugging instead of the item silently never appearing anywhere."""
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
                "view_count": item.viewCount,
                "video_resolution": item.media[0].videoResolution
                if item.media
                else None,
                "duration_ms": item.media[0].duration if item.media else None,
                "file_path": (
                    item.media[0].parts[0].file
                    if item.media and item.media[0].parts
                    else None
                ),
                "synced_at": synced_at,
            }
            for item in section.search()
        ]
