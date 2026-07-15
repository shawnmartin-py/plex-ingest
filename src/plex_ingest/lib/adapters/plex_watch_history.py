from datetime import date

from plexapi.server import PlexServer

from plex_ingest.lib.ports import ResolvedWatchedMovie, WatchHistoryEntry


class PlexWatchHistory:
    """Implements the `WatchHistorySource` and `WatchedMovieResolver` ports
    (see `lib/ports.py`).

    `fetch_history` reads the local server's history log directly — it
    returns every entry regardless of whether the title is still in the
    local library (`rating_key` is `None` when it isn't). `resolve` covers
    that gap: Plex's local search only covers the local library, so a title
    no longer present (deleted after watching, or marked watched manually
    for something never downloaded) can only be recovered via the account's
    cloud Discover catalog, disambiguated by exact `originallyAvailableAt`
    match against candidates (title collisions across years/remakes are
    common) — see docs/pipeline-design.md, "Watch-history data availability"
    for how this was verified against the real server. Returns `None` on no
    exact-date match or when the match lacks either a `tmdb://` or `imdb://`
    guid (both are required, mirroring stg_movies' business rule), rather
    than raising — a single unresolvable watch-history title should be
    skipped, not fail the whole pipeline run.
    """

    def __init__(self, base_url: str, token: str, movie_library: str) -> None:
        self._base_url = base_url
        self._token = token
        self._movie_library = movie_library

    def _server(self) -> PlexServer:
        return PlexServer(baseurl=self._base_url, token=self._token)  # type: ignore[no-untyped-call]

    def fetch_history(self) -> list[WatchHistoryEntry]:
        server = self._server()
        # Restrict to the movie library section server-side -- history() otherwise
        # returns TV episodes too, which then just fail resolve()'s libtype="movie"
        # Discover search one by one (confirmed live 2026-07-12: 12 of 32 history
        # entries in a real run were Westworld episode titles, each burning a wasted
        # Discover API call before being skipped). Filtering here is strictly
        # cheaper, not a behavior change: those entries were already excluded from
        # the final result, just less efficiently.
        section_id = server.library.section(self._movie_library).key
        history = server.history(librarySectionID=section_id)  # type: ignore[no-untyped-call]
        return [
            WatchHistoryEntry(
                title=item.title,
                originally_available_at=item.originallyAvailableAt.date(),
                viewed_at=item.viewedAt,
                rating_key=item.ratingKey,
            )
            for item in history
            if item.originallyAvailableAt is not None and item.viewedAt is not None
        ]

    def resolve(
        self, title: str, originally_available_at: date
    ) -> ResolvedWatchedMovie | None:
        account = self._server().myPlexAccount()  # type: ignore[no-untyped-call]
        candidates = account.searchDiscover(title, limit=20, libtype="movie")
        match = next(
            (
                c
                for c in candidates
                if c.originallyAvailableAt
                and c.originallyAvailableAt.date() == originally_available_at
            ),
            None,
        )
        if match is None:
            return None

        tmdb_id = next(
            (
                guid.id.removeprefix("tmdb://")
                for guid in match.guids
                if guid.id.startswith("tmdb://")
            ),
            None,
        )
        imdb_id = next(
            (
                guid.id.removeprefix("imdb://")
                for guid in match.guids
                if guid.id.startswith("imdb://")
            ),
            None,
        )
        if tmdb_id is None or imdb_id is None:
            return None

        imdb_rating = next(
            (
                rating.value
                for rating in match.ratings
                if rating.image and rating.image.startswith("imdb://")
            ),
            None,
        )

        return ResolvedWatchedMovie(
            tmdb_id=tmdb_id,
            imdb_id=imdb_id,
            title=match.title,
            year=match.year,
            genres=[genre.tag for genre in match.genres],
            imdb_rating=imdb_rating,
            summary=match.summary,
        )
