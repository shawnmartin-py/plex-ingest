from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from plex_ingest.defs.resources.embeddings import EmbeddingsResource


def test_embed_query_delegates_to_adapter(mocker: MockerFixture) -> None:
    resource = EmbeddingsResource()
    mock_adapter: MagicMock = mocker.MagicMock()
    mock_adapter.embed_query.return_value = [0.1, 0.2, 0.3]
    resource._adapter = mocker.MagicMock(return_value=mock_adapter)  # type: ignore[method-assign]

    result = resource.embed_query("some text")

    assert result == [0.1, 0.2, 0.3]
    mock_adapter.embed_query.assert_called_once_with("some text")
