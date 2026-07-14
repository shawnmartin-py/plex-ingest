from datetime import UTC, datetime

import dagster as dg
import httpx
from dagster_duckdb import DuckDBResource

from plex_ingest.defs.resources.omdb import OmdbResource
from plex_ingest.lib.streaming_runtime_store import (
    CREATE_TABLE_SQL,
    TABLE_NAME,
    UPSERT_SQL,
)

_SELECT_PLACEHOLDER_IDS_SQL = """
SELECT imdb_id FROM stg_movies
WHERE source_platform IS NOT NULL
"""

_SELECT_CACHED_IDS_SQL = f"SELECT imdb_id FROM {TABLE_NAME}"  # noqa: S608 — TABLE_NAME is a module constant, not user input


@dg.asset(group_name="streaming_runtime", kinds={"omdb", "duckdb"}, deps=["stg_movies"])
def streaming_runtime(
    context: dg.AssetExecutionContext, omdb: OmdbResource, duckdb: DuckDBResource
) -> dg.MaterializeResult:
    """Resolves the real runtime for streaming-platform placeholder movies (Netflix/
    Disney+ stand-in clips — see docs/vector-store-contract.md) via OMDb, since Plex's
    own `Movie.duration` for these is just the ~4s stand-in file's technical duration,
    confirmed empirically against three real placeholder items (not assumed). A real
    download's runtime comes from `stg_movies.duration_ms` instead (see stg_movies.sql)
    — this asset only ever covers the smaller `source_platform`-flagged subset.

    Deliberately unpartitioned, unlike synopsis/enrichment/embeddings: this only ever
    processes a small, self-limiting set (however many streaming placeholders exist in
    the library), so per-imdb_id partitioning/pooling would be pure overhead against
    OMDb's generous 1000-request/day free tier. A resolved "no runtime" answer from
    OMDb is a real fact that never changes, so it's cached forever
    (`ON CONFLICT DO NOTHING`) — same "fetch once" philosophy as `synopsis`, just
    without the partition machinery. A transient network failure is NOT cached (logged
    and left for the next run to retry) — see
    `OmdbRuntimeLookup.fetch_runtime_minutes`'s docstring for why that distinction
    matters.

    A complete no-op (zero API calls, zero errors) when `OMDB_API_KEY` isn't configured
    — runtime just stays `NULL` for these movies. Not a pipeline failure either way,
    by explicit design: this integration is optional."""
    with duckdb.get_connection() as conn:
        conn.execute(CREATE_TABLE_SQL)
        placeholder_ids = {
            row[0] for row in conn.execute(_SELECT_PLACEHOLDER_IDS_SQL).fetchall()
        }
        cached_ids = {row[0] for row in conn.execute(_SELECT_CACHED_IDS_SQL).fetchall()}
        missing_ids = sorted(placeholder_ids - cached_ids)

        if not omdb.is_configured():
            context.log.info(
                f"OMDB_API_KEY not configured — skipping {len(missing_ids)} "
                "streaming-placeholder movie(s), runtime stays NULL for them"
            )
            return dg.MaterializeResult(
                metadata={
                    "table": TABLE_NAME,
                    "omdb_configured": False,
                    "placeholder_count": len(placeholder_ids),
                    "resolved_count": 0,
                    "skipped_count": len(missing_ids),
                }
            )

        resolved = 0
        not_found = 0
        errored = 0
        fetched_at = datetime.now(UTC)
        for imdb_id in missing_ids:
            try:
                runtime_minutes = omdb.fetch_runtime_minutes(imdb_id)
            except (httpx.HTTPError, ValueError) as e:
                context.log.warning(f"OMDb lookup failed for {imdb_id!r}: {e}")
                errored += 1
                continue
            if runtime_minutes is None:
                not_found += 1
            else:
                resolved += 1
            conn.execute(UPSERT_SQL, [imdb_id, runtime_minutes, fetched_at])

    context.log.info(
        f"Resolved {resolved} streaming-placeholder runtime(s) via OMDb "
        f"({not_found} with no OMDb runtime, {errored} lookup failure(s), "
        f"{len(cached_ids)} already cached)"
    )

    return dg.MaterializeResult(
        metadata={
            "table": TABLE_NAME,
            "omdb_configured": True,
            "placeholder_count": len(placeholder_ids),
            "resolved_count": resolved,
            "not_found_count": not_found,
            "errored_count": errored,
        }
    )


defs = dg.Definitions(assets=[streaming_runtime])
