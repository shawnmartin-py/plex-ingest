import pytest
from pytest_mock import MockerFixture

from plex_ingest.lib.adapters.gemini_embeddings import GeminiEmbeddingClient
from plex_ingest.lib.vector_store_contract import EMBEDDING_DIM


def test_embed_query_returns_vector_matching_contract_dimension(
    mocker: MockerFixture,
) -> None:
    client = GeminiEmbeddingClient()
    mock_vendor_client = mocker.MagicMock()
    mock_vendor_client.embed_query.return_value = [0.0] * EMBEDDING_DIM
    client._client = mocker.MagicMock(return_value=mock_vendor_client)  # type: ignore[method-assign]

    result = client.embed_query("some text")

    assert len(result) == EMBEDDING_DIM


def test_embed_query_raises_when_dimension_does_not_match_contract(
    mocker: MockerFixture,
) -> None:
    client = GeminiEmbeddingClient()
    mock_vendor_client = mocker.MagicMock()
    mock_vendor_client.embed_query.return_value = [0.0] * (EMBEDDING_DIM - 1)
    client._client = mocker.MagicMock(return_value=mock_vendor_client)  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="dims"):
        client.embed_query("some text")


def test_embed_query_retries_on_429(mocker: MockerFixture) -> None:
    mock_sleep = mocker.patch("plex_ingest.lib.adapters.gemini_embeddings.time.sleep")
    client = GeminiEmbeddingClient()
    mock_vendor_client = mocker.MagicMock()
    mock_vendor_client.embed_query.side_effect = [
        Exception("429 RESOURCE_EXHAUSTED"),
        [0.0] * EMBEDDING_DIM,
    ]
    client._client = mocker.MagicMock(return_value=mock_vendor_client)  # type: ignore[method-assign]

    result = client.embed_query("some text")

    assert len(result) == EMBEDDING_DIM
    mock_sleep.assert_called_once_with(10)


def test_embed_query_does_not_retry_on_other_errors(mocker: MockerFixture) -> None:
    client = GeminiEmbeddingClient()
    mock_vendor_client = mocker.MagicMock()
    mock_vendor_client.embed_query.side_effect = ValueError("some unrelated error")
    client._client = mocker.MagicMock(return_value=mock_vendor_client)  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="unrelated error"):
        client.embed_query("some text")
