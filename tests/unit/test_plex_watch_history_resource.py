from datetime import date, datetime

from pytest_mock import MockerFixture

from plex_ingest.defs.resources.plex_watch_history import PlexWatchHistoryResource
from plex_ingest.lib.ports import ResolvedWatchedMovie, WatchHistoryEntry


def test_fetch_history_delegates_to_adapter(mocker: MockerFixture) -> None:
    resource = PlexWatchHistoryResource(
        base_url="http://localhost:32400",
        token="fake-token",  # noqa: S106
        movie_library="Movies",
    )
    mock_adapter = mocker.MagicMock()
    expected = [
        WatchHistoryEntry(
            title="Test Film",
            originally_available_at=date(2020, 1, 1),
            viewed_at=datetime(2026, 6, 1),
            rating_key="123",
        )
    ]
    mock_adapter.fetch_history.return_value = expected
    resource._adapter = mocker.MagicMock(return_value=mock_adapter)  # type: ignore[method-assign]

    result = resource.fetch_history()

    assert result == expected
    mock_adapter.fetch_history.assert_called_once_with()


def test_resolve_delegates_to_adapter(mocker: MockerFixture) -> None:
    resource = PlexWatchHistoryResource(
        base_url="http://localhost:32400",
        token="fake-token",  # noqa: S106
        movie_library="Movies",
    )
    mock_adapter = mocker.MagicMock()
    expected = ResolvedWatchedMovie(
        tmdb_id="456",
        imdb_id="tt0107315",
        title="Kika",
        year=1993,
        genres=["Comedy"],
        imdb_rating=6.5,
        summary="A summary.",
    )
    mock_adapter.resolve.return_value = expected
    resource._adapter = mocker.MagicMock(return_value=mock_adapter)  # type: ignore[method-assign]

    result = resource.resolve("Kika", date(1993, 10, 29))

    assert result == expected
    mock_adapter.resolve.assert_called_once_with("Kika", date(1993, 10, 29))
