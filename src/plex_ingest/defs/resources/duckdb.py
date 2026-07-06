import os
from pathlib import Path

import dagster as dg
from dagster_duckdb import DuckDBResource

# dagster_duckdb.DuckDBResource wraps duckdb.connect() in Dagster's own backoff/retry
# (10 attempts on lock-conflict exceptions) — a real advantage over a hand-rolled
# connection resource, at no extra dependency cost (pandas/pyspark are optional extras
# on dagster-duckdb, not pulled in by this import).
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "data/plex_ingest.duckdb")
Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)

defs = dg.Definitions(resources={"duckdb": DuckDBResource(database=DUCKDB_PATH)})
