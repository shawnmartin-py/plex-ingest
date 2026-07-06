# plex-ingest

Dagster-based data pipeline for `plex-rag`: polling Plex, scraping
synopses, generating LLM enrichments, and embedding everything into a
Qdrant vector store. This repo owns all writes to Qdrant; `plex-rag` is a
read-only consumer of the collection this pipeline produces.

## Purpose

`plex-rag` is a chatbot that gives conversational movie recommendations drawn
only from a user's own Plex library — but a chat app can't answer "something
moody and Kubrick-esque" or "what fits tonight" by querying Plex directly:
Plex only knows titles, genres, and cast, not tone, subgenre, or critical
vocabulary. `plex-ingest` is the offline half of that project — it exists to
turn a raw Plex library into a knowledge base rich enough to support that kind
of question.

For every movie in the library, this pipeline:

1. **Resolves a stable identity** — Plex's own IDs aren't reliable long-term
   keys, so each movie is matched to its IMDb ID and dropped if no match
   exists.
2. **Scrapes a synopsis** — plot-level text, for queries that are actually
   about plot.
3. **Generates an LLM "expert enrichment" profile** — a critic-style
   breakdown (craft / meaning / context: things like directorial style,
   cinematography, themes, tone, cultural context) that a synopsis alone
   doesn't capture. This is what lets the recommender match taste- and
   vibe-based requests, not just plot keywords.
4. **Embeds both** the synopsis and the enrichment sections and writes them
   into Qdrant — the single shared artifact this pipeline produces, and the
   only thing `plex-rag` ever reads. See
   [docs/vector-store-contract.md](docs/vector-store-contract.md) for the
   exact payload shape.

Because a personal library changes over time (movies get added or removed),
the pipeline is built as an incremental Dagster asset graph rather than a
one-off script: it's partitioned per movie (by IMDb ID) so only what changed
needs reprocessing, and a sensor keeps that partition set — and the deletion
of movies no longer in Plex — in sync automatically. See
`docs/pipeline-design.md` for the architectural decisions behind this asset
graph, and `CLAUDE.md` for engineering standards and how the two repos
relate.

## Pipeline

Runs against the full library end to end, verified against the real
Plex/Gemini/Qdrant stack.

![Dagster asset graph — raw_movies → stg_movies → synopsis/enrichment → embeddings → qdrant_collection](docs/images/dagster-asset-graph.png)

- `raw_movies` — full overwrite of the Plex movie library into DuckDB on
  every run (`src/plex_ingest/defs/assets/raw_movies.py`). Manual entry
  point only (see "Getting started") — nothing triggers it automatically.
- `stg_movies` — a dbt model (`dbt_project/models/staging/stg_movies.sql`)
  that resolves `imdb_id` out of Plex's raw `guids` list and drops items
  with no IMDb match, with `not_null`/`unique` tests on `imdb_id` and
  `rating_key`. Also a manual entry point.
- `synopsis`, `enrichment`, `embeddings` — partitioned by `imdb_id`
  (`src/plex_ingest/defs/assets/{synopsis,enrichment,embeddings}.py`).
  `synopsis`/`enrichment` carry no `automation_condition` — the
  `sync_imdb_id_partitions` sensor is their sole trigger, based on on-disk
  file presence (see CLAUDE.md's "Environment gotchas" for why).
  `embeddings` keeps `eager()` for its steady-state cascade, and embeds the
  synopsis document *and* every enrichment section (up to 4 points per
  movie), matching `vector-store-contract.md`. `enrichment` hard-fails
  immediately (not a silent retry loop) if Gemini's *daily* free-tier quota
  is exhausted — see `gemini_enrichment.py`'s `KNOWN_RPM_LIMIT` /
  `DailyQuotaExhaustedError` if the configured model ever changes.
- `qdrant_collection` — final, unpartitioned full delete+reinsert of the
  Qdrant collection from every `data/embeddings/*.json` on disk, attaching
  full catalog metadata read fresh from `stg_movies` at rebuild time
  (`src/plex_ingest/defs/assets/qdrant_collection.py`).
- `sync_imdb_id_partitions` — sensor keeping the `imdb_id` dynamic
  partition set in sync with `stg_movies`, including a deletion cascade for
  movies no longer in Plex (`src/plex_ingest/defs/sensors/`).

See CLAUDE.md's "Environment gotchas" for automation-reliability quirks
found along the way (sensor default-status handling, the
`on_missing()`/`eager()` cold-start gap, backfill-request dedup, Gemini
daily-quota handling) — that's operational tribal knowledge, not this
file's job to duplicate.

## Testing

Unit tests cover the scraping cascade, retry/backoff, partition diff
logic, the sensor's missing-file backfill and removal→rebuild
`RunRequest` behavior, the JSON IOManager, `embeddings`, and
`qdrant_collection`, plus asset-level unit tests for `synopsis`/`enrichment`
themselves, including their error-handling paths (missing `stg_movies` row,
scraper finding nothing, missing synopsis, daily quota exhaustion).
Integration tests cover the cold-start mechanism and content-freshness of
re-materialized `synopsis`/`enrichment`.

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
uv run dagster instance concurrency set gemini_embeddings 2
```

Or run `make up` / `make pools` for the equivalent shortcuts — see
"Makefile shortcuts" below.

## Running the pipeline end-to-end

`raw_movies` and `stg_movies` are manual entry points only — nothing
schedules them, and every asset downstream is partitioned per `imdb_id`,
so those partitions don't exist until `stg_movies` gives the sensor
something to register. With `dg dev` running:

1. **Materialize the two entry-point assets first**, either in the UI
   (select `raw_movies` and `stg_movies` in the asset graph → Materialize)
   or from the CLI:

   ```bash
   uv run dg launch --assets raw_movies,stg_movies
   ```

   (`make seed` runs this too.)

2. **Confirm both sensors are `RUNNING`**: Automation → Sensors in the UI,
   check `sync_imdb_id_partitions` and `default_automation_condition_sensor`.
   Both default to `RUNNING` on a fresh `DAGSTER_HOME`, but an instance
   created before the 2026-07-05 fix (see CLAUDE.md) can still have a
   stale persisted `STOPPED` state that `default_status` won't override —
   toggle on manually in the UI if so.

3. **Everything else is automatic.** Within one sensor tick (≤60s),
   `sync_imdb_id_partitions` registers any new `imdb_id`s and directly
   requests `synopsis`/`enrichment`/`embeddings` runs for whatever's
   missing on disk; `embeddings` → `qdrant_collection` cascades via
   `eager()`. Watch progress on the Runs tab or the asset graph's
   partition-status coloring.

There's no "run everything" button or job by design — step 1 is the only
manual action required once the environment is set up. Ordinary
steady-state library changes (movies added or removed in Plex) need no
further manual intervention.

## Makefile shortcuts

Thin wrappers around the commands above — nothing they do isn't also a
plain `dg`/`docker compose`/`dagster` command, they just save retyping:

| Target | Equivalent to |
| --- | --- |
| `make up` | `docker compose up -d` |
| `make pools` | the three `dagster instance concurrency set` commands |
| `make dev` | `uv run dg dev` |
| `make seed` | `uv run dg launch --assets raw_movies,stg_movies` |

## Environment variables

| Variable | Purpose |
| --- | --- |
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
