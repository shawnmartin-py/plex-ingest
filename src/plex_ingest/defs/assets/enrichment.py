import dagster as dg
from dagster_duckdb import DuckDBResource

from plex_ingest.defs.partitions import tmdb_id_partitions
from plex_ingest.defs.resources.enrichment_llm import EnrichmentLLMResource
from plex_ingest.lib.adapters.gemini_enrichment import DailyQuotaExhaustedError
from plex_ingest.lib.stg_movies_reader import fetch_movie


@dg.asset(
    partitions_def=tmdb_id_partitions,
    pool="gemini_llm",
    io_manager_key="enrichment_io_manager",
    group_name="enrichment",
    kinds={"gemini"},
)
def enrichment(
    context: dg.AssetExecutionContext,
    synopsis: str | None,
    enrichment_llm: EnrichmentLLMResource,
    duckdb: DuckDBResource,
) -> dict[str, str]:
    """Craft/meaning/context expert-profile sections for one movie, ported from
    plex-rag's app/services/enrichment.py. Carries no automation_condition, deliberately
    not eager(): a synopsis backfill must never silently re-trigger these paid,
    rate-limited Gemini calls (see docs/pipeline-design.md's "Idempotency and
    backfill semantics"). `sync_tmdb_id_partitions` is the sole trigger for
    its first materialization, based on on-disk file presence rather than
    AutomationCondition.on_missing() (see that sensor's docstring for why). synopsis is
    passed in as a parameter dependency, loaded via synopsis_io_manager for this same
    partition."""
    tmdb_id = context.partition_key

    with duckdb.get_connection() as conn:
        movie = fetch_movie(conn, tmdb_id)

    if not synopsis:
        msg = f"{movie.title} ({tmdb_id}) has no synopsis — cannot enrich"
        raise ValueError(msg)

    sections: dict[str, str] = {}
    for section in enrichment_llm.sections:
        try:
            text = enrichment_llm.generate_section(
                title=movie.title,
                year=movie.year,
                genres=movie.genres,
                imdb_rating=movie.imdb_rating,
                content_rating=movie.content_rating,
                synopsis=synopsis,
                section=section,
            )
        except DailyQuotaExhaustedError:
            context.log.error(
                f"{movie.title} ({tmdb_id}): Gemini's daily quota is exhausted for "
                "the enrichment model — halting rather than retrying (see the "
                "exception below for which quota and its value). This partition "
                "will not be auto-retried; re-run it manually once the quota resets "
                "or the API key/model changes."
            )
            raise
        if text is not None:
            sections[section] = text
        context.log.info(
            f"{movie.title} ({tmdb_id}): {section} {'done' if text else 'blocked'}"
        )

    return sections


defs = dg.Definitions(assets=[enrichment])
