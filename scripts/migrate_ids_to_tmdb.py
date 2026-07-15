"""One-off imdb_id -> tmdb_id data migration (2026-07). Renames the on-disk
partition files and rekeys stg_watch_history so the already-scraped/enriched/embedded
data survives the pipeline's switch to tmdb_id partition keys without re-scraping or
re-embedding anything.

Run this:
- AFTER the tmdb_id code change is merged and `dbt build` has run (stg_movies must
  have the tmdb_id column — it is the source of the imdb->tmdb mapping), and
- while NO Dagster daemon is running (host `dg dev` or the Docker service): a sensor
  tick against un-renamed files would see every tmdb partition as missing on disk and
  request a full-catalog re-scrape.

What it does:
1. data/{synopsis,enrichment,embeddings}/{imdb_id}.json -> {tmdb_id}.json, using the
   stg_movies mapping. Files whose stem has no mapping are MOVED to
   data/migration_orphans/<dir>/ rather than left in place — a single leftover
   tt-stem in embeddings/ makes every future qdrant_collection rebuild raise (see
   build_points), and the tmdb-keyed sensor can never prune it.
2. Rebuilds stg_watch_history with the tmdb-keyed schema. The mapping here cannot
   come from stg_movies (watched movies are excluded by its view_count = 0 rule), so
   each row is re-resolved against Plex Discover, matched by its known imdb_id guid
   (falling back to exact year). Unresolvable rows are dropped and their embeddings
   file quarantined, with a report. embeddings/watch_history/ files are renamed with
   the resolved mapping.

Idempotent: stems already in the tmdb keyspace are skipped, and the table rebuild is
skipped when stg_watch_history already has a tmdb_id column. Aborts before touching
anything if two source files would rename onto the same target.

Usage:
    uv run python scripts/migrate_ids_to_tmdb.py [--dry-run]

Needs PLEXAPI_AUTH_SERVER_BASEURL / PLEXAPI_AUTH_SERVER_TOKEN in the environment for
step 2 (same vars the pipeline itself uses — `set -a; source .env` first if needed).
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

DATA_DIR = Path(os.environ.get("PLEX_INGEST_DATA_DIR", "data"))
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "data/plex_ingest.duckdb")
ORPHANS_DIR = DATA_DIR / "migration_orphans"

CATALOG_DIRS = ("synopsis", "enrichment", "embeddings")
WATCH_HISTORY_DIR = Path("embeddings") / "watch_history"

_NEW_WATCH_HISTORY_SCHEMA = """
CREATE TABLE stg_watch_history_tmdb (
    tmdb_id VARCHAR PRIMARY KEY,
    imdb_id VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    year INTEGER NOT NULL,
    genres VARCHAR[] NOT NULL,
    imdb_rating DOUBLE,
    summary VARCHAR NOT NULL,
    last_viewed_at TIMESTAMP NOT NULL
)
"""


@dataclass
class RenamePlan:
    """What one directory's migration will do, computed fully before acting."""

    directory: Path
    renames: list[tuple[Path, Path]] = field(default_factory=list)
    orphans: list[Path] = field(default_factory=list)
    already_migrated: int = 0


def _plan_directory(
    directory: Path, mapping: dict[str, str], tmdb_ids: set[str]
) -> RenamePlan:
    plan = RenamePlan(directory=directory)
    if not directory.is_dir():
        return plan
    for path in sorted(directory.glob("*.json")):
        stem = path.stem
        if stem in tmdb_ids:
            plan.already_migrated += 1
        elif stem in mapping:
            plan.renames.append((path, path.with_stem(mapping[stem])))
        else:
            plan.orphans.append(path)
    return plan


def _check_collisions(plan: RenamePlan) -> list[str]:
    """Two sources -> one target, or a target that already exists on disk. Either one
    means the mapping is wrong (or stg_movies has duplicate tmdb ids that somehow got
    past the dbt unique test) — abort rather than clobber a file."""
    errors: list[str] = []
    seen_targets: dict[Path, Path] = {}
    for source, target in plan.renames:
        if target in seen_targets:
            errors.append(
                f"{source} and {seen_targets[target]} both rename to {target}"
            )
        seen_targets[target] = source
        if target.exists():
            errors.append(f"{source} -> {target}: target already exists")
    return errors


def _execute_plan(plan: RenamePlan, *, dry_run: bool) -> None:
    label = plan.directory.relative_to(DATA_DIR)
    print(
        f"{label}: {len(plan.renames)} to rename, {len(plan.orphans)} orphan(s), "
        f"{plan.already_migrated} already migrated"
    )
    orphan_dir = ORPHANS_DIR / label
    for source, target in plan.renames:
        print(f"  rename {source.name} -> {target.name}")
        if not dry_run:
            source.rename(target)
    for source in plan.orphans:
        print(f"  quarantine {source.name} -> {orphan_dir / source.name}")
        if not dry_run:
            orphan_dir.mkdir(parents=True, exist_ok=True)
            source.rename(orphan_dir / source.name)


def _load_catalog_mapping(conn: DuckDBPyConnection) -> dict[str, str]:
    rows = conn.execute("SELECT imdb_id, tmdb_id FROM stg_movies").fetchall()
    return {imdb_id: tmdb_id for imdb_id, tmdb_id in rows}


def migrate_catalog_files(conn: DuckDBPyConnection, *, dry_run: bool) -> None:
    mapping = _load_catalog_mapping(conn)
    tmdb_ids = set(mapping.values())
    print(f"stg_movies mapping: {len(mapping)} imdb_id -> tmdb_id pairs\n")

    plans = [
        _plan_directory(DATA_DIR / name, mapping, tmdb_ids) for name in CATALOG_DIRS
    ]
    errors = [error for plan in plans for error in _check_collisions(plan)]
    if errors:
        print("ABORTING — rename collisions detected, nothing was touched:")
        for error in errors:
            print(f"  {error}")
        sys.exit(1)

    for plan in plans:
        _execute_plan(plan, dry_run=dry_run)


def _watch_history_needs_migration(conn: DuckDBPyConnection) -> bool:
    tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    if "stg_watch_history" not in tables:
        print("\nstg_watch_history: table does not exist — nothing to migrate")
        return False
    columns = {
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'stg_watch_history'"
        ).fetchall()
    }
    if "tmdb_id" in columns:
        print("\nstg_watch_history: already tmdb-keyed — skipping table rebuild")
        return False
    return True


def _resolve_tmdb_id(account: Any, title: str, year: int, imdb_id: str) -> str | None:
    """The tmdb guid for a watched movie, via the same Discover search surface the
    pipeline's own resolver uses (lib/adapters/plex_watch_history.py). Matched by the
    row's known imdb guid — exact, no ambiguity — falling back to exact year for a
    candidate that lacks an imdb guid entirely."""
    candidates = account.searchDiscover(title, limit=20, libtype="movie")

    def _guid(candidate: Any, prefix: str) -> str | None:
        return next(
            (
                guid.id.removeprefix(prefix)
                for guid in candidate.guids
                if guid.id.startswith(prefix)
            ),
            None,
        )

    by_imdb = next(
        (c for c in candidates if _guid(c, "imdb://") == imdb_id),
        None,
    )
    match = by_imdb or next((c for c in candidates if c.year == year), None)
    if match is None:
        return None
    return _guid(match, "tmdb://")


def migrate_watch_history(conn: DuckDBPyConnection, *, dry_run: bool) -> None:
    if not _watch_history_needs_migration(conn):
        return

    from plexapi.server import PlexServer

    base_url = os.environ.get("PLEXAPI_AUTH_SERVER_BASEURL")
    token = os.environ.get("PLEXAPI_AUTH_SERVER_TOKEN")
    if not base_url or not token:
        print(
            "\nABORTING watch-history migration: PLEXAPI_AUTH_SERVER_BASEURL / "
            "PLEXAPI_AUTH_SERVER_TOKEN not set (catalog files above are already "
            "handled — re-run with the env set to finish this part)."
        )
        sys.exit(1)
    server = PlexServer(baseurl=base_url, token=token)  # type: ignore[no-untyped-call]
    account = server.myPlexAccount()  # type: ignore[no-untyped-call]

    rows = conn.execute(
        "SELECT imdb_id, title, year, genres, imdb_rating, summary, last_viewed_at "
        "FROM stg_watch_history"
    ).fetchall()
    print(f"\nstg_watch_history: {len(rows)} row(s) to re-resolve via Plex Discover")

    resolved_rows: list[tuple[Any, ...]] = []
    mapping: dict[str, str] = {}
    dropped: list[tuple[str, str]] = []
    for imdb_id, title, year, genres, imdb_rating, summary, last_viewed_at in rows:
        tmdb_id = _resolve_tmdb_id(account, title, year, imdb_id)
        if tmdb_id is None or tmdb_id in mapping.values():
            reason = "no tmdb match" if tmdb_id is None else f"duplicate of {tmdb_id}"
            print(f"  DROP {imdb_id} ({title}, {year}): {reason}")
            dropped.append((imdb_id, title))
            continue
        print(f"  {imdb_id} -> {tmdb_id} ({title})")
        mapping[imdb_id] = tmdb_id
        resolved_rows.append(
            (
                tmdb_id,
                imdb_id,
                title,
                year,
                genres,
                imdb_rating,
                summary,
                last_viewed_at,
            )
        )

    wh_dir = DATA_DIR / WATCH_HISTORY_DIR
    orphan_dir = ORPHANS_DIR / WATCH_HISTORY_DIR
    plan = RenamePlan(directory=wh_dir)
    if wh_dir.is_dir():
        for path in sorted(wh_dir.glob("*.json")):
            if path.stem in mapping.values():
                plan.already_migrated += 1
            elif path.stem in mapping:
                plan.renames.append((path, path.with_stem(mapping[path.stem])))
            else:
                plan.orphans.append(path)
    errors = _check_collisions(plan)
    if errors:
        print("ABORTING — watch-history rename collisions, table not rebuilt:")
        for error in errors:
            print(f"  {error}")
        sys.exit(1)

    if dry_run:
        print(
            f"  would rebuild table with {len(resolved_rows)} row(s), "
            f"drop {len(dropped)}, rename {len(plan.renames)} file(s), "
            f"quarantine {len(plan.orphans)}"
        )
        return

    conn.execute("BEGIN")
    conn.execute(_NEW_WATCH_HISTORY_SCHEMA)
    if resolved_rows:
        conn.executemany(
            "INSERT INTO stg_watch_history_tmdb VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            resolved_rows,
        )
    conn.execute("DROP TABLE stg_watch_history")
    conn.execute("ALTER TABLE stg_watch_history_tmdb RENAME TO stg_watch_history")
    conn.execute("COMMIT")
    print(f"  rebuilt stg_watch_history: {len(resolved_rows)} row(s)")

    for source in plan.orphans:
        # Orphan = a file for a row that no longer exists (or was just dropped) —
        # leaving it in place would crash build_watch_history_points on the next
        # rebuild, same failure mode as the catalog dirs above.
        print(f"  quarantine {source.name}")
        orphan_dir.mkdir(parents=True, exist_ok=True)
        source.rename(orphan_dir / source.name)
    for source, target in plan.renames:
        print(f"  rename {source.name} -> {target.name}")
        source.rename(target)
    if dropped:
        print(f"  dropped {len(dropped)} unresolvable row(s): {dropped}")


def main() -> None:
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("DRY RUN — nothing will be modified\n")

    conn = duckdb.connect(DUCKDB_PATH, read_only=dry_run)
    try:
        migrate_catalog_files(conn, dry_run=dry_run)
        migrate_watch_history(conn, dry_run=dry_run)
    finally:
        conn.close()

    print("\nDone." if not dry_run else "\nDry run complete.")


if __name__ == "__main__":
    main()
