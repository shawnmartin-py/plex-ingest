"""One-off cleanup for the imdb_id -> tmdb_id migration (2026-07): deletes every
dynamic partition registered under the old `imdb_id` / `watch_history_imdb_id`
namespaces from the Dagster instance. The renamed partition definitions
(`tmdb_id` / `watch_history_tmdb_id`, see defs/partitions.py) are fresh namespaces,
so the old registrations are inert orphans — but they'd sit in instance storage
forever confusing future debugging (this instance has history with phantom
instigator state; see CLAUDE.md).

Run once per instance — the host one (with DAGSTER_HOME set, as usual) and the
Docker one (`docker compose exec dagster uv run python
scripts/cleanup_old_partition_namespaces.py`).

Usage:
    uv run python scripts/cleanup_old_partition_namespaces.py [--dry-run]
"""

import sys

import dagster as dg

OLD_NAMESPACES = ("imdb_id", "watch_history_imdb_id")


def main() -> None:
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("DRY RUN — nothing will be deleted\n")

    with dg.DagsterInstance.get() as instance:
        for namespace in OLD_NAMESPACES:
            keys = instance.get_dynamic_partitions(namespace)
            print(f"{namespace}: {len(keys)} registered partition(s)")
            if dry_run:
                continue
            for key in keys:
                instance.delete_dynamic_partition(namespace, key)
            if keys:
                print(f"  deleted all {len(keys)}")


if __name__ == "__main__":
    main()
