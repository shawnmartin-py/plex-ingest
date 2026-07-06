# plex-ingest

Dagster-based data pipeline for `plex-rag`: polling Plex, scraping
synopses, generating LLM enrichments, and embedding everything into a
Qdrant vector store. This repo owns all writes to Qdrant; `plex-rag` is a
read-only consumer of the collection this pipeline produces.

The only externally-visible artifact this repo produces is a Qdrant
collection, governed by [docs/vector-store-contract.md](docs/vector-store-contract.md)
(kept in sync with the copy in `plex-rag`).

## Status

Phase 2 of the `plex-ingest` extraction (porting pipeline logic into
Dagster assets) is underway — see `plex-rag`'s
`docs/epics/plex-ingest-extraction/breakdown.md` for the full phased plan
and `phase-2-pipeline-design.md` for what's still undecided. So far:

- `raw_movies` — full overwrite of the Plex movie library into DuckDB on
  every run (`src/plex_ingest/defs/assets/raw_movies.py`).
- `stg_movies` — a dbt model (`dbt_project/models/staging/stg_movies.sql`)
  that resolves `imdb_id` out of Plex's raw `guids` list and drops items
  with no IMDb match, with `not_null`/`unique` tests on `imdb_id` and
  `rating_key`.
- `synopsis`, `enrichment`, `embeddings` — partitioned by `imdb_id`
  (`src/plex_ingest/defs/assets/{synopsis,enrichment,embeddings}.py`) per
  `phase-2-pipeline-design.md`'s "Idempotency and backfill semantics".
  `synopsis`/`enrichment` carry no `automation_condition` — the
  `sync_imdb_id_partitions` sensor is their sole trigger, based on on-disk
  file presence (see "Status" below for why). `embeddings` keeps
  `eager()` for its ordinary steady-state cascade, and embeds the synopsis
  document *and* every enrichment section (up to 4 per movie), matching
  `vector-store-contract.md`'s "up to 4 points per imdb_id" exactly.
- `qdrant_collection` — final, unpartitioned full delete+reinsert of the
  Qdrant collection from every `data/embeddings/*.json` on disk, attaching
  full catalog metadata and `embedding_type` read fresh from `stg_movies`
  at rebuild time (`src/plex_ingest/defs/assets/qdrant_collection.py`).
- `sync_imdb_id_partitions` — sensor keeping the `imdb_id` dynamic
  partition set in sync with `stg_movies`, including deletion cascade for
  movies no longer in Plex (`src/plex_ingest/defs/sensors/`).

Verified end to end against real Plex/Gemini/Qdrant for the capped dev
subset (see below) — 4 Qdrant points per movie, correct `embedding_type`/
`section`/catalog metadata on each, confirmed by direct query. A follow-up
session exercised new-movie, movie-removed, and prompt-change/force-refetch
scenarios against the same live stack; the happy path works for all three.
That session found three automation-reliability gaps — see
`phase-2-pipeline-design.md`'s "Known gaps found during dev-subset
verification" in `plex-rag` for full detail. **All three are now fixed:**
both sensors default to `RUNNING`; a pure removal directly requests a
`qdrant_collection` rebuild instead of relying on an incidental future
`embeddings` update; and `on_missing()`'s cold-start gap (confirmed via
`tests/integration/test_automation_condition_cold_start.py` to be an
`evaluation_id == 0` issue — a partition already missing at the
automation-condition cursor's literal first-ever evaluation never becomes
eligible again) was fixed by removing `automation_condition` from
`synopsis`/`enrichment` entirely: `sync_imdb_id_partitions` now checks
on-disk file presence directly every tick and is their sole trigger,
sidestepping the cursor rather than working around it.
`embeddings`/`qdrant_collection` keep `eager()` for their ordinary
steady-state cascade (unaffected by the cold-start bug), with the same
sensor providing a direct backfill/rebuild request as a supplement for
their own cold-start case.

**`dg dev`'s sensors should now start themselves** on a fresh
`DAGSTER_HOME`/code location (both default to `RUNNING` as of 2026-07-05).
If a `DAGSTER_HOME` predates that fix, it may still have a persisted
`STOPPED` state — check **Automation → Sensors** in the UI (or a
`startSensor` GraphQL mutation) for `sync_imdb_id_partitions` and
`default_automation_condition_sensor` and enable manually if so.

**Currently capped by `PLEX_INGEST_PARTITION_LIMIT`** (see `.env.example`)
as a deliberate safety rail while this is being built and tested against
the real Plex/Gemini/Qdrant stack — only that many imdb_ids are ever
registered as partitions, regardless of library size. Unset it once the
pipeline has run against the full library and has broader test coverage
(currently: unit tests for the scraping cascade, retry/backoff, partition
diff/limit logic, the sensor's missing-file backfill and removal→rebuild
`RunRequest` behavior, the JSON IOManager, `embeddings`, and
`qdrant_collection`, plus integration tests for the cold-start mechanism
and content-freshness of re-materialized `synopsis`/`enrichment` — not yet
dedicated asset-level unit tests for `synopsis`/`enrichment` themselves,
e.g. their error-handling paths).

## Getting started

Install dependencies:

```bash
uv sync
```

Copy `.env.example` to `.env` and fill in a real `GOOGLE_API_KEY`:

```bash
cp .env.example .env
```

Start Qdrant (server mode, persistent volume):

```bash
docker compose up -d
```

Start the Dagster UI:

```bash
uv run dg dev
```

Open http://localhost:3000 to see the project, or materialize assets
directly from the CLI:

```bash
uv run dg launch --assets raw_movies,stg_movies
```

Set the Gemini/scrape concurrency pool limits once per instance (stored
in `DAGSTER_HOME`, not code — see "Environment variables" below):

```bash
uv run dagster instance concurrency set gemini_llm 2
uv run dagster instance concurrency set imdb_scrape 2
```

## Environment variables

| Variable | Purpose |
|---|---|
| `QDRANT_URL` | Qdrant server URL, e.g. `http://localhost:6333` |
| `QDRANT_COLLECTION` | Collection name — must match `plex-rag`'s `QDRANT_COLLECTION` |
| `GOOGLE_API_KEY` | Gemini API key, used to embed with `gemini-embedding-001` |
| `PLEXAPI_AUTH_SERVER_BASEURL` | Plex server URL, e.g. `http://192.168.1.x:32400` |
| `PLEXAPI_AUTH_SERVER_TOKEN` | Plex auth token |
| `PLEX_MOVIE_LIBRARY` | Plex movie library name, matching the Plex UI |
| `DUCKDB_PATH` | Absolute path to the local DuckDB file — must be absolute so `dbt` (invoked with a different cwd) and the Python assets resolve to the same file |
| `DAGSTER_HOME` | Absolute path to a persistent instance directory — dynamic partitions and concurrency pool limits live here, and must survive across separate `dg launch`/sensor invocations, not just one `dg dev` session |
| `PLEX_INGEST_PARTITION_LIMIT` | Dev-only safety rail: caps how many imdb_ids the partition-sync sensor ever registers. Unset once the pipeline is proven against the full library |

## Requirements

Python **3.13**, not 3.14 — see "Environment gotchas" in `CLAUDE.md` for
why (`dbt-core` doesn't import on 3.14 yet).
