from datetime import date, datetime
from types import SimpleNamespace

from pytest_mock import MockerFixture

from plex_ingest.lib.adapters.plex_watch_history import PlexWatchHistory


def _history_item(
    title: str,
    originally_available_at: date | None,
    viewed_at: datetime | None,
    rating_key: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        title=title,
        originallyAvailableAt=(
            SimpleNamespace(date=lambda d=originally_available_at: d)
            if originally_available_at
            else None
        ),
        viewedAt=viewed_at,
        ratingKey=rating_key,
    )


def _candidate(
    title: str,
    originally_available_at: date | None,
    guids: list[str],
    genres: list[str],
    ratings: list[tuple[str, float]],
    summary: str = "A summary.",
    year: int = 2020,
) -> SimpleNamespace:
    return SimpleNamespace(
        title=title,
        year=year,
        summary=summary,
        originallyAvailableAt=(
            SimpleNamespace(date=lambda d=originally_available_at: d)
            if originally_available_at
            else None
        ),
        guids=[SimpleNamespace(id=g) for g in guids],
        genres=[SimpleNamespace(tag=g) for g in genres],
        ratings=[SimpleNamespace(image=image, value=value) for image, value in ratings],
    )


def test_fetch_history_maps_and_filters_entries_without_date_or_viewed_at(
    mocker: MockerFixture,
) -> None:
    adapter = PlexWatchHistory(
        base_url="http://localhost:32400",
        token="fake-token",  # noqa: S106
        movie_library="Movies",
    )
    mock_server = mocker.MagicMock()
    mock_server.history.return_value = [
        _history_item("Resolvable", date(2015, 6, 26), datetime(2026, 6, 1), "1214"),
        _history_item("No date", None, datetime(2026, 6, 1)),
        _history_item("No viewed_at", date(2015, 6, 26), None),
    ]
    adapter._server = mocker.MagicMock(return_value=mock_server)  # type: ignore[method-assign]

    result = adapter.fetch_history()

    assert len(result) == 1
    assert result[0].title == "Resolvable"
    assert result[0].rating_key == "1214"


def test_fetch_history_restricts_to_the_movie_library_section(
    mocker: MockerFixture,
) -> None:
    """Filtering server-side to the movie section, not just the movie libtype in
    resolve(), avoids wasting a Discover API call on every TV episode in history --
    confirmed live 2026-07-12: 12 of 32 real history entries were TV episodes, each
    burning a resolve() call before being skipped."""
    adapter = PlexWatchHistory(
        base_url="http://localhost:32400",
        token="fake-token",  # noqa: S106
        movie_library="All Movies",
    )
    mock_server = mocker.MagicMock()
    mock_server.library.section.return_value.key = 3
    mock_server.history.return_value = []
    adapter._server = mocker.MagicMock(return_value=mock_server)  # type: ignore[method-assign]

    adapter.fetch_history()

    mock_server.library.section.assert_called_once_with("All Movies")
    mock_server.history.assert_called_once_with(librarySectionID=3)


def test_resolve_returns_none_when_no_candidate_matches_exact_date(
    mocker: MockerFixture,
) -> None:
    adapter = PlexWatchHistory(
        base_url="http://localhost:32400",
        token="fake-token",  # noqa: S106
        movie_library="Movies",
    )
    mock_account = mocker.MagicMock()
    mock_account.searchDiscover.return_value = [
        _candidate("Knock Knock", date(2021, 10, 22), ["imdb://tt0"], ["Comedy"], []),
    ]
    mock_server = mocker.MagicMock()
    mock_server.myPlexAccount.return_value = mock_account
    adapter._server = mocker.MagicMock(return_value=mock_server)  # type: ignore[method-assign]

    result = adapter.resolve("Knock Knock", date(2015, 6, 26))

    assert result is None


def test_resolve_returns_none_when_matched_candidate_has_no_imdb_guid(
    mocker: MockerFixture,
) -> None:
    adapter = PlexWatchHistory(
        base_url="http://localhost:32400",
        token="fake-token",  # noqa: S106
        movie_library="Movies",
    )
    mock_account = mocker.MagicMock()
    mock_account.searchDiscover.return_value = [
        _candidate("Kika", date(1993, 10, 29), ["tmdb://123"], ["Comedy"], []),
    ]
    mock_server = mocker.MagicMock()
    mock_server.myPlexAccount.return_value = mock_account
    adapter._server = mocker.MagicMock(return_value=mock_server)  # type: ignore[method-assign]

    result = adapter.resolve("Kika", date(1993, 10, 29))

    assert result is None


def test_resolve_extracts_imdb_id_genres_and_imdb_scale_rating(
    mocker: MockerFixture,
) -> None:
    adapter = PlexWatchHistory(
        base_url="http://localhost:32400",
        token="fake-token",  # noqa: S106
        movie_library="Movies",
    )
    mock_account = mocker.MagicMock()
    mock_account.searchDiscover.return_value = [
        _candidate(
            "Kika",
            date(1993, 10, 29),
            guids=["imdb://tt0107315", "tmdb://456"],
            genres=["Comedy", "Drama"],
            ratings=[
                ("rottentomatoes://image.rating.ripe", 8.0),
                ("imdb://image.rating", 6.5),
            ],
            summary="Kika, a cosmetologist...",
            year=1993,
        ),
    ]
    mock_server = mocker.MagicMock()
    mock_server.myPlexAccount.return_value = mock_account
    adapter._server = mocker.MagicMock(return_value=mock_server)  # type: ignore[method-assign]

    result = adapter.resolve("Kika", date(1993, 10, 29))

    assert result is not None
    assert result.imdb_id == "tt0107315"
    assert result.imdb_rating == 6.5
    assert result.genres == ["Comedy", "Drama"]
    assert result.year == 1993
    assert result.summary == "Kika, a cosmetologist..."


def test_resolve_imdb_rating_is_none_when_no_imdb_rating_present(
    mocker: MockerFixture,
) -> None:
    adapter = PlexWatchHistory(
        base_url="http://localhost:32400",
        token="fake-token",  # noqa: S106
        movie_library="Movies",
    )
    mock_account = mocker.MagicMock()
    mock_account.searchDiscover.return_value = [
        _candidate(
            "Kika",
            date(1993, 10, 29),
            guids=["imdb://tt0107315"],
            genres=["Comedy"],
            ratings=[("rottentomatoes://image.rating.ripe", 8.0)],
        ),
    ]
    mock_server = mocker.MagicMock()
    mock_server.myPlexAccount.return_value = mock_account
    adapter._server = mocker.MagicMock(return_value=mock_server)  # type: ignore[method-assign]

    result = adapter.resolve("Kika", date(1993, 10, 29))

    assert result is not None
    assert result.imdb_rating is None
