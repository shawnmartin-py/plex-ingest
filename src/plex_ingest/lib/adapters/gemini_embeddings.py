import time

from langchain_google_genai import GoogleGenerativeAIEmbeddings

from plex_ingest.lib.vector_store_contract import EMBEDDING_DIM, EMBEDDING_MODEL

# Retry/backoff constants mirror gemini_enrichment.py — same rationale: this asset
# has no automation_condition of its own and is re-requested by the sync sensor every
# tick while its output is missing, so a 429 must back off in place rather than fail
# outright and rely on the next tick (60s+ later) to retry cold.
BASE_RETRY_DELAY = 10
MAX_RETRY_DELAY = 120


class GeminiEmbeddingClient:
    """Implements the `EmbeddingClient` port (see `lib/ports.py`)."""

    def _client(self) -> GoogleGenerativeAIEmbeddings:
        return GoogleGenerativeAIEmbeddings(model=EMBEDDING_MODEL)

    def embed_query(self, text: str) -> list[float]:
        client = self._client()
        delay = BASE_RETRY_DELAY
        while True:
            try:
                vector = client.embed_query(text)
                break
            except Exception as e:
                err = str(e)
                if (
                    "429" in err
                    or "RESOURCE_EXHAUSTED" in err
                    or "timeout" in err.lower()
                    or "deadline" in err.lower()
                    or "timed out" in err.lower()
                ):
                    time.sleep(delay)
                    delay = min(delay * 2, MAX_RETRY_DELAY)
                else:
                    raise
        dim = len(vector)
        if dim != EMBEDDING_DIM:
            msg = f"{EMBEDDING_MODEL} returned {dim} dims, expected {EMBEDDING_DIM}"
            raise ValueError(msg)
        return vector
