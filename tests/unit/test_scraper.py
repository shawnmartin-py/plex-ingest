from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from plex_ingest.defs.resources.scraper import ScraperResource


def test_fetch_synopsis_delegates_to_adapter(mocker: MockerFixture) -> None:
    resource = ScraperResource()
    mock_adapter: MagicMock = mocker.MagicMock()
    mock_adapter.fetch_synopsis.return_value = "A plot."
    resource._adapter = mocker.MagicMock(return_value=mock_adapter)  # type: ignore[method-assign]

    result = resource.fetch_synopsis("tt0111161", "The Shawshank Redemption", 1994)

    assert result == "A plot."
    mock_adapter.fetch_synopsis.assert_called_once_with(
        "tt0111161", "The Shawshank Redemption", 1994
    )
