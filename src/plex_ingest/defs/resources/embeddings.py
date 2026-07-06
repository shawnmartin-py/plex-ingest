import dagster as dg

from plex_ingest.lib.adapters.gemini_embeddings import GeminiEmbeddingClient
from plex_ingest.lib.ports import EmbeddingClient


class EmbeddingsResource(dg.ConfigurableResource):
    """Config + adapter factory only — the Gemini client and the dimension check
    live in lib/adapters/gemini_embeddings.py, behind the EmbeddingClient port."""

    def _adapter(self) -> EmbeddingClient:
        return GeminiEmbeddingClient()

    def embed_query(self, text: str) -> list[float]:
        return self._adapter().embed_query(text)


defs = dg.Definitions(resources={"embeddings": EmbeddingsResource()})
