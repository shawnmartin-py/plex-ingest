import dagster as dg
from dagster_duckdb import DuckDBResource

from plex_ingest.defs.partitions import imdb_id_partitions
from plex_ingest.defs.resources.scraper import ScraperResource
from plex_ingest.lib.stg_movies_reader import fetch_movie


@dg.asset(
    partitions_def=imdb_id_partitions,
    pool="imdb_scrape",
    io_manager_key="synopsis_io_manager",
    group_name="enrichment",
    kinds={"playwright"},
    deps=["stg_movies"],
)
def synopsis(
    context: dg.AssetExecutionContext, scraper: ScraperResource, duckdb: DuckDBResource
) -> str | None:
    """Scraped synopsis text for one movie — IMDB plot summary -> Wikipedia plot ->
    IMDB description cascade, ported from plex-rag's app/synopsis.py. Partitioned by
    imdb_id. Carries no automation_condition: `sync_imdb_id_partitions` is the sole
    trigger for its first materialization, based on on-disk file presence rather than
    AutomationCondition.on_missing() (see that sensor's docstring for why). Never
    re-scraped once materialized once; redo only via explicit backfill (see
    docs/pipeline-design.md)."""
    imdb_id = context.partition_key

    with duckdb.get_connection() as conn:
        movie = fetch_movie(conn, imdb_id)

    text = scraper.fetch_synopsis(imdb_id, movie.title, movie.year)
    found = "found" if text else "no"
    context.log.info(
        f"{movie.title} ({imdb_id}): {found} synopsis ({len(text or '')} chars)"
    )
    return text


defs = dg.Definitions(assets=[synopsis])
