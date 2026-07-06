from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from plex_ingest.defs.resources.qdrant import QdrantResource


def _resource(mocker: MockerFixture) -> tuple[QdrantResource, MagicMock]:
    resource = QdrantResource(url="http://localhost:6333", collection="media_items")
    mock_adapter: MagicMock = mocker.MagicMock()
    resource._adapter = mocker.MagicMock(return_value=mock_adapter)  # type: ignore[method-assign]
    return resource, mock_adapter


def test_recreate_collection_delegates_to_adapter(mocker: MockerFixture) -> None:
    resource, mock_adapter = _resource(mocker)
    resource.recreate_collection()
    mock_adapter.recreate_collection.assert_called_once()


def test_upsert_points_delegates_to_adapter(mocker: MockerFixture) -> None:
    resource, mock_adapter = _resource(mocker)
    points = [("id1", [0.1], "text", {"imdb_id": "tt0001"})]
    resource.upsert_points(points)
    mock_adapter.upsert_points.assert_called_once_with(points)


def test_point_count_delegates_to_adapter(mocker: MockerFixture) -> None:
    resource, mock_adapter = _resource(mocker)
    mock_adapter.point_count.return_value = 5
    assert resource.point_count() == 5
