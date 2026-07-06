from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from plex_ingest.defs.resources.plex import PlexResource


def test_fetch_raw_movies_delegates_to_adapter(mocker: MockerFixture) -> None:
    resource = PlexResource(
        base_url="http://localhost:32400",
        token="fake-token",  # noqa: S106
        movie_library="Movies",
    )
    mock_adapter: MagicMock = mocker.MagicMock()
    mock_adapter.fetch_raw_movies.return_value = [{"title": "Test Film"}]
    resource._adapter = mocker.MagicMock(return_value=mock_adapter)  # type: ignore[method-assign]

    result = resource.fetch_raw_movies()

    assert result == [{"title": "Test Film"}]
    mock_adapter.fetch_raw_movies.assert_called_once_with()
