import dagster as dg
from dagster_duckdb import DuckDBResource

from plex_ingest.defs.partitions import watch_history_imdb_id_partitions
from plex_ingest.defs.resources.embeddings import EmbeddingsResource
from plex_ingest.lib.vector_store_contract import build_synopsis_document_text
from plex_ingest.lib.watch_history_reader import fetch_watch_history_movie


@dg.asset(
    partitions_def=watch_history_imdb_id_partitions,
    deps=["stg_watch_history"],
    pool="gemini_embeddings",
    io_manager_key="watch_history_embeddings_io_manager",
    group_name="watch_history",
    kinds={"gemini"},
)
def watch_history_embeddings(
    context: dg.AssetExecutionContext,
    embeddings: EmbeddingsResource,
    duckdb: DuckDBResource,
) -> dict[str, object]:
    """Embedding vector for one watched movie's synopsis-shaped document. Unlike
    `embeddings` (media_items), there's exactly one point per imdb_id here — no
    synopsis/enriched split — so this is a single {"text", "vector"} object, not a
    dict of documents (see `build_watch_history_points` in vector_store_contract.py).

    Carries no `automation_condition` of its own — `sync_watch_history_partitions` is
    its sole trigger, checking on-disk presence directly every tick, for the same
    cold-start reason `synopsis`/`enrichment` (media_items) don't carry one either
    (`AutomationCondition.on_missing()`/`eager()` can't reliably catch a partition
    already missing at its very first evaluation — see
    `sync_imdb_id_partitions`'s docstring for the full mechanism). `deps=` on
    `stg_watch_history` is lineage-only: the catalog fields are read directly via
    `duckdb` below, not passed through an IOManager. Pooled at `gemini_embeddings`,
    same pool and retry/backoff behavior as `embeddings` — see
    `gemini_embeddings.py`."""
    imdb_id = context.partition_key

    with duckdb.get_connection() as conn:
        row = fetch_watch_history_movie(conn, imdb_id)

    text = build_synopsis_document_text(
        row.title, row.year, row.imdb_rating, row.genres, row.summary
    )
    vector = embeddings.embed_query(text)

    context.log.info(f"{imdb_id}: embedded watch-history document")

    return {"text": text, "vector": vector}


defs = dg.Definitions(assets=[watch_history_embeddings])
