from unittest.mock import MagicMock

import httpx
import pytest
from pytest_mock import MockerFixture

from plex_ingest.lib.adapters.omdb_client import OmdbRuntimeLookup


def _response(mocker: MockerFixture, payload: dict[str, str]) -> MagicMock:
    resp: MagicMock = mocker.MagicMock()
    resp.json.return_value = payload
    return resp


def test_fetch_runtime_minutes_parses_runtime_string(mocker: MockerFixture) -> None:
    mock_get = mocker.patch("plex_ingest.lib.adapters.omdb_client.httpx.get")
    mock_get.return_value = _response(
        mocker, {"Response": "True", "Runtime": "104 min"}
    )
    assert OmdbRuntimeLookup("key").fetch_runtime_minutes("tt0001") == 104


def test_fetch_runtime_minutes_none_when_omdb_has_no_match(
    mocker: MockerFixture,
) -> None:
    mock_get = mocker.patch("plex_ingest.lib.adapters.omdb_client.httpx.get")
    mock_get.return_value = _response(
        mocker, {"Response": "False", "Error": "Incorrect IMDb ID."}
    )
    assert OmdbRuntimeLookup("key").fetch_runtime_minutes("tt0001") is None


def test_fetch_runtime_minutes_none_when_runtime_is_na(mocker: MockerFixture) -> None:
    mock_get = mocker.patch("plex_ingest.lib.adapters.omdb_client.httpx.get")
    mock_get.return_value = _response(mocker, {"Response": "True", "Runtime": "N/A"})
    assert OmdbRuntimeLookup("key").fetch_runtime_minutes("tt0001") is None


def test_fetch_runtime_minutes_raises_on_transport_error(
    mocker: MockerFixture,
) -> None:
    """A network failure must propagate, not silently return None — the caller
    (streaming_runtime asset) needs to tell "no data" apart from "couldn't ask" so it
    only caches the former."""
    mock_get = mocker.patch("plex_ingest.lib.adapters.omdb_client.httpx.get")
    mock_get.side_effect = httpx.ConnectError("boom")
    with pytest.raises(httpx.ConnectError):
        OmdbRuntimeLookup("key").fetch_runtime_minutes("tt0001")
