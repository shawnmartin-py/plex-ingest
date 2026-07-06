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
  file presence (see CLAUDE.md's "Environment gotchas" for why). `embeddings`
  keeps `eager()` for its ordinary steady-state cascade, and embeds the
  synopsis document *and* every enrichment section (up to 4 per movie),
  matching `vector-store-contract.md`'s "up to 4 points per imdb_id" exactly.
  `enrichment` hard-fails immediately (not a silent retry loop) if Gemini's
  *daily* free-tier quota is exhausted, distinguishing it from an ordinary
  per-minute rate limit that's retried with backoff — see
  `src/plex_ingest/lib/adapters/gemini_enrichment.py`'s `KNOWN_RPM_LIMIT` /
  `DailyQuotaExhaustedError` if this needs adjusting for a different model.
- `qdrant_collection` — final, unpartitioned full delete+reinsert of the
  Qdrant collection from every `data/embeddings/*.json` on disk, attaching
  full catalog metadata and `embedding_type` read fresh from `stg_movies`
  at rebuild time (`src/plex_ingest/defs/assets/qdrant_collection.py`).
- `sync_imdb_id_partitions` — sensor keeping the `imdb_id` dynamic
  partition set in sync with `stg_movies`, including deletion cascade for
  movies no longer in Plex (`src/plex_ingest/defs/sensors/`).

Verified end to end against the real Plex/Gemini/Qdrant stack, including
new-movie, movie-removed, and prompt-change/force-refetch scenarios, and
now runs against the full library (no dev-only partition cap). See
CLAUDE.md's "Environment gotchas" for the automation-reliability quirks
found along the way (sensor default-status handling, the
`on_missing()`/`eager()` cold-start gap, `run_key` dedup, Gemini
daily-quota vs. per-minute-limit handling) — those are operational
tribal knowledge, not this file's job to duplicate.

Test coverage: unit tests for the scraping cascade, retry/backoff,
partition diff logic, the sensor's missing-file backfill and
removal→rebuild `RunRequest` behavior, the JSON IOManager, `embeddings`,
and `qdrant_collection`, plus integration tests for the cold-start
mechanism and content-freshness of re-materialized `synopsis`/`enrichment`.
Not yet covered: dedicated asset-level unit tests for `synopsis`/
`enrichment` themselves (e.g. their error-handling paths).

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
| `GOOGLE_API_KEY` | Gemini API key — used both to embed with `gemini-embedding-001` and to generate enrichment text with the configured LLM (`gemini-3.1-flash-lite` by default) |
| `PLEXAPI_AUTH_SERVER_BASEURL` | Plex server URL, e.g. `http://192.168.1.x:32400` |
| `PLEXAPI_AUTH_SERVER_TOKEN` | Plex auth token |
| `PLEX_MOVIE_LIBRARY` | Plex movie library name, matching the Plex UI |
| `DUCKDB_PATH` | Absolute path to the local DuckDB file — must be absolute so `dbt` (invoked with a different cwd) and the Python assets resolve to the same file |
| `DAGSTER_HOME` | Absolute path to a persistent instance directory — dynamic partitions and concurrency pool limits live here, and must survive across separate `dg launch`/sensor invocations, not just one `dg dev` session |

## Requirements

Python **3.13**, not 3.14 — see "Environment gotchas" in `CLAUDE.md` for
why (`dbt-core` doesn't import on 3.14 yet).
