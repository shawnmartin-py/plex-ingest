from typing import Any

from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams

from plex_ingest.lib.vector_store_contract import DISTANCE, EMBEDDING_DIM


class QdrantPointStore:
    """Implements the `VectorStore` port (see `lib/ports.py`)."""

    def __init__(self, url: str, collection: str) -> None:
        self._url = url
        self._collection = collection

    def _client(self) -> QdrantClient:
        return QdrantClient(url=self._url)

    def recreate_collection(self) -> None:
        """Drop the collection if it exists and create it empty. Used by
        qdrant_collection's full rebuild — see phase-2-pipeline-design.md's "Asset
        boundary" in plex-rag for why a full delete+reinsert is the deliberate design,
        not incremental per-movie upserts."""
        client = self._client()
        if client.collection_exists(self._collection):
            client.delete_collection(self._collection)
        client.create_collection(
            collection_name=self._collection,
            vectors_config=VectorParams(size=EMBEDDING_DIM, distance=DISTANCE),
        )

    def upsert_points(
        self, points: list[tuple[str, list[float], str, dict[str, Any]]]
    ) -> None:
        """Bulk upsert (point_id, vector, page_content, metadata) tuples in one call —
        used for the full-collection rebuild rather than one network round-trip per
        point."""
        self._client().upsert(
            collection_name=self._collection,
            points=[
                PointStruct(
                    id=point_id,
                    vector=vector,
                    payload={"page_content": page_content, "metadata": metadata},
                )
                for point_id, vector, page_content, metadata in points
            ],
        )

    def point_count(self) -> int:
        return self._client().count(self._collection, exact=True).count
