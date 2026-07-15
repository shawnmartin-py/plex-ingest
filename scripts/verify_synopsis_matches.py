"""Runs the synopsis_matches_movie data-quality check against every tmdb_id that
already has a scraped synopsis on disk, without re-materializing (re-scraping)
`synopsis` itself.

Why not `dg launch --job ... --partition ...` / `dagster job backfill`: a job built
from an asset-check-only selection (`AssetSelection.checks_for_assets(synopsis)`, with
no plain asset in the selection) loses its `partitions_def` somewhere in Dagster
1.13.12's `Definitions` resolution -- confirmed empirically (`Definitions.get_job_def`
resolves `partitions_def=None` even though an explicit `partitions_def` was passed to
`define_asset_job`), which then fails CLI partition validation with "Job has no
PartitionsDefinition". Consistent with `partitions_def` on `@asset_check` being a
documented Dagster preview feature (see docs/pipeline-design.md's "Data-quality
checks"). This script sidesteps the bug entirely by calling the judge directly (the
same thing the check op does) and recording each result as a *runless* asset check
event via `DagsterInstance.report_runless_asset_event` -- so results still show up in
the Dagster UI's checks history/health for `synopsis`, exactly as if the check had run
inside a real job.

Usage:
    uv run python scripts/verify_synopsis_matches.py [tmdb_id ...]

With no arguments, verifies every partition under data/synopsis/. With one or more
tmdb_ids, verifies just those.
"""

import json
import sys
from pathlib import Path

import dagster as dg

from plex_ingest.defs.resources.duckdb import DUCKDB_PATH
from plex_ingest.defs.resources.partition_json_io_manager import PLEX_INGEST_DATA_DIR
from plex_ingest.defs.resources.synopsis_judge import SynopsisJudgeResource
from plex_ingest.lib.stg_movies_reader import fetch_movie

_SYNOPSIS_ASSET_KEY = dg.AssetKey("synopsis")
_CHECK_NAME = "synopsis_matches_movie"
_CHECK_KEY = dg.AssetCheckKey(asset_key=_SYNOPSIS_ASSET_KEY, name=_CHECK_NAME)


def _synopsis_dir() -> Path:
    return Path(PLEX_INGEST_DATA_DIR) / "synopsis"


def _tmdb_ids_to_verify() -> list[str]:
    if len(sys.argv) > 1:
        return sys.argv[1:]
    return sorted(p.stem for p in _synopsis_dir().glob("*.json"))


def _already_recorded_partitions(instance: dg.DagsterInstance) -> set[str]:
    """Partitions with an existing runless evaluation for this check.
    `report_runless_asset_event` does a plain INSERT keyed on
    (asset_key, check_name, run_id="", partition) with no upsert -- confirmed
    empirically (sqlite3.IntegrityError: UNIQUE constraint failed) that reporting a
    second runless event for the same partition crashes rather than replacing it.
    Querying history first and skipping the report step for these makes re-running
    this script safe."""
    records = instance.event_log_storage.get_asset_check_execution_history(
        check_key=_CHECK_KEY, limit=10_000
    )
    return {r.partition for r in records if r.partition is not None}


def main() -> None:
    import duckdb

    instance = dg.DagsterInstance.get()
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    judge = SynopsisJudgeResource()
    already_recorded = _already_recorded_partitions(instance)

    tmdb_ids = _tmdb_ids_to_verify()
    failed: list[str] = []
    skipped = 0

    for tmdb_id in tmdb_ids:
        synopsis_path = _synopsis_dir() / f"{tmdb_id}.json"
        synopsis = (
            json.loads(synopsis_path.read_text()) if synopsis_path.exists() else None
        )
        movie = fetch_movie(conn, tmdb_id)

        if not synopsis:
            print(f"{tmdb_id} ({movie.title}): SKIP -- no synopsis on disk")
            skipped += 1
            continue

        result = judge.check(title=movie.title, year=movie.year, synopsis=synopsis)
        status = "PASS" if result.matches else "FAIL"
        print(f"{tmdb_id} ({movie.title}): {status} -- {result.reason}")

        if tmdb_id in already_recorded:
            print("  (already recorded in the check history -- not re-reporting)")
        else:
            instance.report_runless_asset_event(
                dg.AssetCheckEvaluation(
                    asset_key=_SYNOPSIS_ASSET_KEY,
                    check_name=_CHECK_NAME,
                    passed=result.matches,
                    severity=dg.AssetCheckSeverity.ERROR,
                    description=result.reason,
                    partition=tmdb_id,
                )
            )
        if not result.matches:
            failed.append(tmdb_id)

    checked = len(tmdb_ids) - skipped
    print(
        f"\n{checked - len(failed)}/{checked} passed ({skipped} skipped, no synopsis)."
    )
    if failed:
        print("Failed:", ", ".join(failed))
        sys.exit(1)


if __name__ == "__main__":
    main()
