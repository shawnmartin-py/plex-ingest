import dagster as dg
from dagster_duckdb import DuckDBResource

from plex_ingest.defs.assets.synopsis import synopsis
from plex_ingest.defs.resources.synopsis_judge import SynopsisJudgeResource
from plex_ingest.lib.stg_movies_reader import fetch_movie


@dg.asset_check(
    asset=synopsis,
    blocking=True,
    pool="groq_synopsis_judge",
    description=(
        "Verifies the scraped synopsis text actually describes this movie and not "
        "an unrelated one -- a wrong franchise entry, a mismatched "
        "remake/adaptation, or a scrape/search-cascade failure returning boilerplate "
        "instead of a plot. This is a data-quality gate, distinct from the code tests "
        "in tests/ -- it inspects the actual scraped content for one partition, not "
        "the code that scraped it. Blocking: `enrichment`/`embeddings` never run "
        "against a mismatched synopsis in the same run, so bad data can't burn a "
        "paid Gemini call or reach Qdrant (and therefore the recommender) before a "
        "human resolves it.\n\n"
        "DISABLED as of 2026-07-06: `sync_tmdb_id_partitions` now passes "
        "asset_check_keys=[] on every RunRequest, so this never actually executes in "
        "production. A full-catalog verification run "
        "(scripts/verify_synopsis_matches.py) showed the Groq/qwen3-32b judge is "
        "unreliable at scale (~85% false-mismatch rate against known-correct "
        "synopses, including contradictory verdicts for the same partition across "
        "separate runs) -- most likely relying on the model's own incomplete/"
        "hallucinated recall of 'the real plot' rather than judging the text's "
        "internal plausibility, which fails outright for anything past its training "
        "cutoff. Left in place, not deleted, pending a judge model with real "
        "search/grounding capability. See docs/pipeline-design.md's 'Data-quality "
        "checks' for the full writeup."
    ),
)
def synopsis_matches_movie(
    context: dg.AssetCheckExecutionContext,
    synopsis: str | None,
    synopsis_judge: SynopsisJudgeResource,
    duckdb: DuckDBResource,
) -> dg.AssetCheckResult:
    tmdb_id = context.partition_key

    if not synopsis:
        return dg.AssetCheckResult(
            passed=True,
            description=(
                "No synopsis was scraped for this partition -- nothing to verify "
                "here; `enrichment` fails separately on a missing synopsis."
            ),
        )

    with duckdb.get_connection() as conn:
        movie = fetch_movie(conn, tmdb_id)

    result = synopsis_judge.check(title=movie.title, year=movie.year, synopsis=synopsis)
    context.log.info(
        f"{movie.title} ({tmdb_id}): synopsis "
        f"{'matches' if result.matches else 'MISMATCH'} -- {result.reason}"
    )
    return dg.AssetCheckResult(
        passed=result.matches,
        severity=dg.AssetCheckSeverity.ERROR,
        description=result.reason,
        metadata={"title": movie.title, "year": movie.year, "reason": result.reason},
    )


defs = dg.Definitions(asset_checks=[synopsis_matches_movie])
