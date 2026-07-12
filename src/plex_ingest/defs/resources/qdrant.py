from typing import Any

import dagster as dg

from plex_ingest.lib.adapters.qdrant_store import QdrantPointStore
from plex_ingest.lib.ports import VectorStore


class QdrantResource(dg.ConfigurableResource):
    """Config + adapter factory only — the qdrant_client calls live in
    lib/adapters/qdrant_store.py, behind the VectorStore port, so qdrant_collection's
    tests can fake this without a live Qdrant instance."""

    url: str = dg.EnvVar("QDRANT_URL")
    collection: str = dg.EnvVar("QDRANT_COLLECTION")

    def _adapter(self) -> VectorStore:
        return QdrantPointStore(url=self.url, collection=self.collection)

    def recreate_collection(self) -> None:
        self._adapter().recreate_collection()

    def upsert_points(
        self, points: list[tuple[str, list[float], str, dict[str, Any]]]
    ) -> None:
        self._adapter().upsert_points(points)

    def point_count(self) -> int:
        return self._adapter().point_count()


defs = dg.Definitions(
    resources={
        "qdrant": QdrantResource(),
        # Same class, second instance/config -- a different collection on the same
        # Qdrant deployment. See docs/vector-store-contract.md's `watch_history`
        # collection section.
        "watch_history_qdrant": QdrantResource(
            url=dg.EnvVar("QDRANT_URL"),
            collection=dg.EnvVar("QDRANT_WATCH_HISTORY_COLLECTION"),
        ),
    }
)
