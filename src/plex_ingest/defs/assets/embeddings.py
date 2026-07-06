import dagster as dg
from dagster_duckdb import DuckDBResource

from plex_ingest.defs.partitions import imdb_id_partitions
from plex_ingest.defs.resources.embeddings import EmbeddingsResource
from plex_ingest.lib.stg_movies_reader import fetch_movie
from plex_ingest.lib.vector_store_contract import (
    SYNOPSIS_KEY,
    build_synopsis_document_text,
)


@dg.asset(
    partitions_def=imdb_id_partitions,
    automation_condition=dg.AutomationCondition.eager(),
    pool="gemini_embeddings",
    io_manager_key="embeddings_io_manager",
    group_name="enrichment",
    kinds={"gemini"},
)
def embeddings(
    context: dg.AssetExecutionContext,
    synopsis: str | None,
    enrichment: dict[str, str],
    embeddings: EmbeddingsResource,
    duckdb: DuckDBResource,
) -> dict[str, dict[str, object]]:
    """Embedding vector for the synopsis document plus each enrichment section of one
    movie — matches vector-store-contract.md's "up to 4 points per imdb_id" (1 synopsis
    + up to 3 enriched), not just the enriched sections. Gated by eager(), not
    on_missing(): embedding a text is a single cheap API call, and if this ever went
    stale relative to `synopsis`/`enrichment` (e.g. after an explicit backfill) the
    vector in Qdrant would no longer match the text it represents — a correctness bug,
    not just staleness. See docs/pipeline-design.md's "Idempotency and backfill
    semantics". `sync_imdb_id_partitions` also backfills this directly
    whenever it's missing on disk for a desired partition, rather than relying solely
    on eager()'s own missing-asset detection, which shares on_missing()'s cold-start
    blind spot for an asset's own initial materialization (not just its deps) — see
    that sensor's docstring. Pooled at `gemini_embeddings`, and
    `GeminiEmbeddingClient.embed_query` retries with backoff on 429/RESOURCE_EXHAUSTED
    (see gemini_embeddings.py) for the same reason enrichment does: this asset has no
    automation_condition-driven cursor of its own to fall back on for retry timing."""
    imdb_id = context.partition_key

    if not synopsis:
        msg = f"{imdb_id} has no synopsis — enrichment should not have run without one"
        raise ValueError(msg)

    with duckdb.get_connection() as conn:
        movie = fetch_movie(conn, imdb_id)

    result: dict[str, dict[str, object]] = {}

    synopsis_text = build_synopsis_document_text(
        movie.title, movie.year, movie.imdb_rating, movie.genres, synopsis
    )
    result[SYNOPSIS_KEY] = {
        "text": synopsis_text,
        "vector": embeddings.embed_query(synopsis_text),
    }

    for section, text in enrichment.items():
        result[section] = {"text": text, "vector": embeddings.embed_query(text)}

    context.log.info(f"{imdb_id}: embedded {len(result)} document(s)")
    return result


defs = dg.Definitions(assets=[embeddings])
