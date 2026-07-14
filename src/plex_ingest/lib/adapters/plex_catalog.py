from datetime import UTC, datetime
from typing import Any

from plexapi.server import PlexServer

# colorTrc values that indicate an HDR transfer function (PQ for HDR10/HDR10+, HLG for
# HLG). plexapi has no dedicated hdr/isHdr boolean on VideoStream — this is Plex's own
# raw color-metadata vocabulary, not a value this repo invents.
_HDR_COLOR_TRC = {"smpte2084", "arib-std-b67"}


def _hdr_formats(video_stream: Any) -> list[str]:
    """`video_stream` is a `plexapi.media.VideoStream | None`, left untyped like every
    other plexapi object this adapter touches (see `PlexWatchHistory` for the same
    convention) — plexapi's own typing is thin enough that pinning the concrete class
    here would just fight duck-typed test doubles for no real safety gain."""
    if video_stream is None:
        return []
    formats = []
    if video_stream.colorTrc in _HDR_COLOR_TRC or video_stream.DOVIPresent:
        formats.append("HDR")
    if video_stream.DOVIPresent:
        formats.append("DV")
    return formats


def _first_video_stream(item: Any) -> Any:
    if not item.media or not item.media[0].parts:
        return None
    streams = item.media[0].parts[0].videoStreams()
    return streams[0] if streams else None


def _content_rating(item: Any) -> str | None:
    """Plex's `contentRating` is locale-tagged (`"gb/15"`, `"gb/12A"`) whenever the
    library's metadata agent resolved a non-US rating board — bare US-style values
    (`"R"`, `"PG-13"`) and `"Not Rated"` have no such prefix. Strip it so the contract
    field is always the bare rating token a UI can display directly, regardless of
    which board supplied it."""
    raw: str | None = item.contentRating
    if raw is None:
        return None
    return raw.split("/", 1)[1] if "/" in raw else raw


def _imdb_rating(item: Any) -> float | None:
    """`item.ratings` holds one entry per rating source Plex found (IMDb, Rotten
    Tomatoes, TMDb, ...) — filtering by `image` is required, not optional: blindly
    taking index `[0]` (the original bug here) would mis-tag whichever source Plex
    happened to list first as `imdb_rating`. Same filter `PlexWatchHistory.resolve`
    already uses for the `watch_history` collection."""
    return next(
        (
            rating.value
            for rating in item.ratings
            if rating.image and rating.image.startswith("imdb://")
        ),
        None,
    )


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

        `video_resolution`/`duration_ms`/`file_path`/`hdr_formats` come from the item's
        first `Media`/`Part` (a movie library item has exactly one of each in this
        library; there's no multi-version support to pick between). This also covers
        the streaming-platform placeholder clips (see docs/vector-store-contract.md) —
        they're real `Movie` items with real `Media`/`Part` data, just a much shorter
        `duration_ms` and a `file_path` naming convention staging parses to detect
        them. `hdr_formats` is read from the part's first video stream (`.streams`),
        one level deeper than the other three fields — see `_hdr_formats`/
        `_first_video_stream` above for why (plexapi has no HDR/DV signal on `Media`
        itself, only on `VideoStream`).

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
                "content_rating": _content_rating(item),
                "description": item.summary,
                "thumb_url": item.thumbUrl,
                "guids": [guid.id for guid in item.guids],
                "genres": [genre.tag for genre in item.genres],
                "imdb_rating": _imdb_rating(item),
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
                "hdr_formats": _hdr_formats(_first_video_stream(item)),
                "synced_at": synced_at,
            }
            for item in section.search()
        ]
