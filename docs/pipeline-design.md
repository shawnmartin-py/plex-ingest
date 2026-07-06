# Pipeline design decisions

The architectural record for this pipeline's asset graph: partitioning,
storage, deletion, and automation semantics. Captured here so decisions and
their reasoning aren't lost, and future changes get worked through
deliberately rather than relitigated ad hoc. Operational bugs found and
fixed after these decisions were implemented (sensor default-status,
`run_key` dedup, concurrency-slot leaks, Gemini quota labeling) live in
`CLAUDE.md`'s "Environment gotchas" instead — this doc is about *why the
pipeline is shaped the way it is*, not day-to-day operational trivia.

**Collaboration model for pipeline architecture changes:** this is a joint
call, not something to design and implement unilaterally. Surface options,
tradeoffs, and a recommendation; the final choice on asset boundaries,
partitioning, frameworks, and storage is a senior/human decision. The only
piece of this still genuinely open is the LlamaIndex/LangChain framework
choice — see [Frameworks under consideration](#frameworks-under-consideration).

## Partitioning — decided (2026-07-05)

**Dynamic partitions keyed by `imdb_id`**, applied to three assets:
`synopsis`, `enrichment`, and `embeddings`. Each asset's partition function
generates/writes all the data for that movie in one go (`enrichment` still
runs all 3 sections — craft/meaning/context — internally per partition,
keeping the section-level skip-check from the legacy code for
partial-completion resumption within a movie). A fourth, final asset,
`qdrant_collection`, is deliberately **unpartitioned** — see
[Asset boundary](#asset-boundary--decided-2026-07-05) below for why.

Partitioning exists specifically because *fetching* the underlying content
is expensive (scraping, LLM generation, embedding calls) — it is not
applied to the Qdrant write itself, which is cheap to redo in full once
the expensive data already exists on disk.

Cardinality (~156 partitions today) was raised as a possible concern but
dismissed: comparable to a single year of daily time-partitioned assets
elsewhere, well within normal Dagster scale.

Rejected alternatives:
- **No partitioning** — not viable. Rejected because it would force a
  fresh full re-fetch/re-generate of all expensive LLM calls on every run;
  there'd be no way to skip movies already processed at the Dagster level.
- **Multi-dimensional partitions (`imdb_id` × `section`, ~468 partitions)**
  — the per-section skip-check inside `enrichment` already gives
  equivalent granularity without fragmenting the partition grid 3x for
  little added benefit at this library size.
- **Static batch partitions (chunks of N movies)** — no meaningful
  boundary at 156 movies; loses "redo exactly the one that failed" for no
  real savings over per-movie partitions.

### Why not just re-derive completion from Qdrant, like the legacy code did

The legacy `enrichment.py` (in `plex-rag`, since deleted from that repo)
queried Qdrant directly ("does an enriched point for this imdb_id+section
already exist?") to decide what to skip — completion state lived entirely
in the sink, re-derived every run. Moving to Dagster-native dynamic
partitions makes that state visible in the Dagster UI (per-movie
materialization history, targeted backfill of just the failed movies)
instead of being opaque outside the orchestrator.

## Intended parallelism and rate limiting — decided (2026-07-05)

Partitions of `synopsis`/`enrichment`/`embeddings` should be allowed to
run **in parallel** where possible (not forced serial) — real throughput
control over the external rate limits (Gemini for enrichment, politeness
delays for scraping) is via **Dagster concurrency pools**: tag each asset
with a pool (`pool="gemini_llm"` on `enrichment`, `pool="imdb_scrape"` on
`synopsis`) and cap `max_concurrent` for that pool instance-wide (see
`README.md`'s "Getting started" for the exact commands). The legacy
in-process retry/backoff on 429/RESOURCE_EXHAUSTED/timeout is kept
regardless — pools throttle concurrency, backoff handles the actual
rate-limit signal when it happens anyway. `qdrant_collection`
(unpartitioned) needs no pool — Qdrant is a real client-server DB, not
rate-limited the way Gemini is.

## Idempotency and backfill semantics — decided (2026-07-05)

The automation condition differs by stage, split along **cost**: stages
where refetching is expensive/rate-limited get manual-only regeneration;
stages that just derive from already-fetched data get automatic
consistency tracking, because letting a derived value go stale relative
to its source is a correctness bug, and re-deriving it is cheap.

- **`synopsis`** — no `automation_condition`; the `sync_imdb_id_partitions`
  sensor is its sole trigger (see [Known gaps](#known-gaps-found-during-dev-subset-verification-2026-07-05)
  item 2 — `AutomationCondition.on_missing()` was originally used here but
  replaced after a cold-start bug was found in it). Never reprocessed once
  it has any materialization; scraping is the entry point and isn't free.
  Redo only via explicit backfill.
- **`enrichment`** — same: no `automation_condition`, sensor-triggered
  relative to `synopsis`'s on-disk presence. This is the one that matters
  most: a `synopsis` backfill must **not** silently cascade into fresh,
  paid Gemini calls. `eager()` was considered and rejected here
  specifically because its cascade behavior (re-running `synopsis` would
  automatically re-trigger `enrichment` too, since it'd see its upstream
  dependency change) makes the expensive stage fire as a side effect of an
  unrelated backfill — never acceptable for a rate-limited, paid API.
- **`embeddings`** — `AutomationCondition.eager()` relative to
  `enrichment`. Embedding a text is a single, comparatively cheap API
  call — but if `enrichment` changes (via normal fill-in *or* an explicit
  backfill) and `embeddings` doesn't follow automatically, you get exactly
  the failure mode being protected against: a vector in Qdrant that no
  longer matches the text it's supposed to represent. Consistency here is
  mandatory, and cheap to maintain automatically.
- **`qdrant_collection`** — `AutomationCondition.eager()` relative to
  `embeddings` (unpartitioned — see [Asset boundary](#asset-boundary--decided-2026-07-05)).
  Same reasoning as `embeddings`: the final consumer must never be allowed
  to drift from whatever `embeddings` currently holds.

**Consequence:** a deliberate, expensive redo (bug fixes, prompt/model
changes) only ever requires an **explicit backfill of `synopsis` and/or
`enrichment`** for the affected movie(s) — `embeddings` and
`qdrant_collection` automatically and correctly follow from there with no
additional manual step, and no risk of a mismatched vector being served.

## Deletion / pruning cascade — decided (2026-07-05)

The legacy `sync_library()` (`plex-rag`'s `app/main.py`, since deleted from
that repo) diffed Plex's current imdb_ids against previously-known ones,
deleted removed rows from SQLite, then deleted the corresponding Qdrant
points via a `metadata.imdb_id` filter. Because `qdrant_collection` is now
a full delete-and-reinsert rebuild rather than incremental per-movie
upserts (see below), **no Qdrant-specific deletion logic is needed at
all** — removal reduces entirely to file cleanup:

A sensor triggered off `raw_movies`/`stg_movies` materialization diffs the
current run's imdb_ids against the **registered dynamic partition set**
(not against "did stg_movies re-run" — a routine full-refresh of 156
already-known movies must not look like new data). For each imdb_id:
- **New** → `add_dynamic_partitions`. The sensor then fills in
  `synopsis` → `enrichment` → `embeddings` for it, per the conditions above.
- **Removed** (no longer in Plex) → `delete_dynamic_partition` (shared
  across `synopsis`/`enrichment`/`embeddings` since they use the same
  `DynamicPartitionsDefinition` instance), plus deletion of the stale
  `synopsis/{imdb_id}.json`, `enrichment/{imdb_id}.json`, and
  `embeddings/{imdb_id}.json` files. `delete_dynamic_partition` only
  removes the imdb_id from the active partition set — it does not delete
  historical materializations or on-disk files, so the file deletion has
  to happen explicitly. Once those files are gone, the next
  `qdrant_collection` rebuild naturally excludes that movie — there is
  nothing further to delete in Qdrant itself.

**Correction, found and fixed 2026-07-05.** The file deletion above is
invisible to Dagster's own materialization tracking (it's a direct
filesystem write, not an asset output), so `qdrant_collection`'s `eager()`
condition — which only reacts to tracked `embeddings` updates — had no
reason to fire on a pure removal. Confirmed live: removing one of the
dev-subset movies with no other movie being added in the same cycle left
its stale points in Qdrant indefinitely. **Fix in place:**
`sync_imdb_id_partitions` now also returns a `RunRequest` for
`qdrant_collection` whenever `removed_ids` is non-empty, so a pure removal
always triggers a rebuild directly rather than depending on an unrelated
future `embeddings` update.

## Intermediate/temp storage — decided (2026-07-05)

**Per-partition flat files, not DuckDB**, for the three partitioned
stages — one JSON file per movie per stage (`synopsis/{imdb_id}.json`,
`enrichment/{imdb_id}.json`, `embeddings/{imdb_id}.json`, the last holding
each section's text alongside its embedding vector), via a custom
IOManager keyed off `context.partition_key`. DuckDB is single-writer (like
SQLite) — if these partitions run concurrently (intended, see above),
concurrent writers to one DuckDB file would hit lock contention, and
serializing them just to keep DuckDB would defeat the purpose of
partitioning for these stages.

DuckDB remains exactly as already decided for `raw_movies`/`stg_movies`
(genuinely SQL-shaped, single-writer, unpartitioned) — this only concerns
the three new per-movie partitioned stages. `qdrant_collection` reads
every `embeddings/{imdb_id}.json` on disk directly; it has no storage
concern of its own.

## Asset boundary — decided (2026-07-05)

`raw_movies` → `stg_movies` (unpartitioned, as-is) → partition-sync sensor
→ `synopsis` (partitioned by `imdb_id`) → `enrichment` (partitioned by
`imdb_id`, depends on `synopsis`) → `embeddings` (partitioned by
`imdb_id`, depends on `enrichment`) → `qdrant_collection` (**unpartitioned**,
depends on all partitions of `embeddings`).

`qdrant_collection` is deliberately not partitioned, and does not do
incremental per-movie upserts. Once a movie's data is expensive to fetch,
partitioning that fetch is what earns its keep; loading already-computed
data into Qdrant is not expensive, so the simplest correct thing —
delete all points, reinsert everything currently in `embeddings/*.json`
— is both simpler and self-correcting (a movie removed from `embeddings`
via the deletion cascade above is automatically absent from the next
rebuild, *provided a rebuild actually runs* — see the correction under
[Deletion / pruning cascade](#deletion--pruning-cascade--decided-2026-07-05)).
This is a deliberate change from the legacy code's behavior of writing to
the vector store immediately, interleaved per movie.

## Qdrant payload shape — decided (2026-07-05), fixed after a real gap

`vector-store-contract.md` requires up to 4 points per `imdb_id` (1
`synopsis` + up to 3 `enriched`), each carrying full catalog metadata
(`title`/`year`/`imdb_rating`/`content_rating`/`genres`/`thumb_url`) and an
`embedding_type` field. An initial implementation of `embeddings`/
`qdrant_collection` missed this — it only embedded the 3 enrichment
sections (no synopsis-type point at all) and wrote just `imdb_id`+`section`
as metadata, with no `embedding_type`. Caught during a docs-vs-code review,
not a design change: this was a straightforward compliance gap against an
already-agreed contract, not a new decision, so it was fixed rather than
relitigated.

**Fix in place:**
- `embeddings` also embeds a synopsis document (`build_synopsis_document_text`
  in `plex_ingest/lib/vector_store_contract.py`, matching `plex-rag`'s
  `MediaItem.to_document()` field-for-field) alongside the 3 enrichment
  sections — up to 4 embedded documents per movie, keyed `"synopsis"` +
  section names.
- Catalog metadata and `embedding_type` are **not** cached in
  `embeddings/*.json` — `qdrant_collection` reads them fresh from
  `stg_movies` at rebuild time (`build_catalog_metadata`, same lib module).
  This means a catalog-only correction (e.g. a fixed title) can never go
  stale in Qdrant without needing to re-embed anything, consistent with why
  a full rebuild was chosen in the first place.
- Verified against the real Plex/Gemini/Qdrant stack for the 3-movie dev
  subset: 12 points (3 movies × 4 documents), correct `embedding_type`/
  `section`/catalog fields on each, then against the full ~156-movie
  library.

## Known gaps found during dev-subset verification (2026-07-05)

A follow-up session deliberately exercised three operational scenarios
against the real Plex/Gemini/Qdrant stack on the dev subset — a new movie
appearing, a movie disappearing from staging, and a prompt change forcing
a re-fetch — verifying end state directly in Qdrant after each. The happy
path (correct data ends up correctly embedded and rebuilt) works for all
three once automation is actually running. Three gaps were found in how
reliably that automation actually runs, none of them requiring a design
change — all are implementation/operational fixes against already-agreed
architecture. All three were fixed 2026-07-05/2026-07-06; the day-to-day
symptoms and debugging steps for each now live in `CLAUDE.md`'s
"Environment gotchas" — summarized here for the design rationale:

1. **Neither sensor ran by default.** `sync_imdb_id_partitions` had no
   `default_status=dg.DefaultSensorStatus.RUNNING`, and Dagster's
   auto-generated automation-condition sensor (which drives every
   `on_missing()`/`eager()` condition in this pipeline) also defaulted to
   `STOPPED`. Fixed by setting `default_status=RUNNING` on both, defined
   explicitly in `sync_imdb_id_partitions.py`'s `defs` assembly.

2. **`on_missing()`'s cold-start blind spot**, confirmed deterministically
   via `tests/integration/test_automation_condition_cold_start.py`. The
   mechanism: `evaluation_id == 0`, the literal very first evaluation of a
   freshly created automation-condition cursor. `on_missing()`'s expansion
   wraps a transient event in `since(...)`, whose reset condition includes
   `initial_evaluation()` (true only at evaluation_id 0). If a partition is
   *already* missing at that exact first evaluation, its
   `missing().newly_true()` event and the `initial_evaluation()` reset both
   fire on the same tick, and the tie resolves in favor of the reset — the
   condition evaluates false and never becomes true again, since
   `newly_true()` only fires once per missing-transition. This reproduces
   even for the exact `eager()` example in the public
   `dagster.evaluate_automation_conditions` docstring (confirmed against
   the installed Dagster 1.13.12) — worth raising upstream, since it
   contradicts that docstring's own claim. Partitions that start existing
   *after* evaluation_id 0 are unaffected.

   **Fix:** `synopsis` and `enrichment` no longer carry any
   `automation_condition` at all — `sync_imdb_id_partitions` is their sole
   trigger. On every tick, for every currently-desired partition, it checks
   on-disk file presence directly (`_missing_stage_assets`) and issues a
   `RunRequest` for whatever's missing, sidestepping the
   automation-condition cursor entirely. This also covers `embeddings`'s
   own cold-start case: if a desired partition's `embeddings` file is
   missing, the sensor also requests a `qdrant_collection` rebuild
   directly, rather than trusting `qdrant_collection`'s `eager()` to notice
   on its own. `embeddings` keeps its `eager()` condition for the ordinary
   steady-state cascade (re-embed when `enrichment` changes after a manual
   backfill) — that path reacts to `any_deps_updated`, a recurring event
   unaffected by the one-shot `missing()` bug.

3. **Pure removals never triggered a `qdrant_collection` rebuild.** Covered
   above under [Deletion / pruning cascade](#deletion--pruning-cascade--decided-2026-07-05).

## Frameworks under consideration

- **LlamaIndex** — for document splitting and/or enrichment. Would
  potentially replace or supplement hand-rolled chunking logic.
- **LangChain** — for abstraction over LLM calls and Qdrant interaction.
  `plex-rag` already depends on `langchain-google-genai` and
  `langchain-qdrant`; open question is whether `plex-ingest` reuses the
  same abstractions (consistency, shared idioms) or writes directly against
  `qdrant-client` (fewer dependencies, more control — this is what's
  implemented today: `lib/adapters/qdrant_store.py` uses raw
  `qdrant-client`, `lib/adapters/gemini_embeddings.py` uses
  `langchain-google-genai`). The port/adapter split in `lib/ports.py`
  exists specifically so this choice can still be swapped later without
  touching resources or assets.

Per the `dagster-expert` skill's integration workflow, whichever of these
get adopted should go through `dg list components` / `dagster-dbt` etc.
rather than hand-rolled wrappers, where a Dagster component exists for the
tool.

**Sling and dlt — decided against for the Plex extraction step
(2026-07-05):**
- **Sling: ruled out for Plex extraction, no path at all** — its connector
  model (DB/file/object-storage) has no support for arbitrary Python/SDK
  sources like `plexapi`, confirmed via official docs.
- **dlt: ruled out for `raw_movies` specifically, not forever** — it can
  wrap custom Python/SDK sources, but its schema-evolution/incremental-cursor
  machinery isn't needed for a small, fixed-schema, full-overwrite asset.
  Revisit if incremental Plex sync becomes necessary, or a real paginated
  REST API source (TMDB/OMDB) is added later.

Both remain theoretically open for later stages (scrape/enrich) but
neither has an obvious fit there either — scraping is browser automation,
not EL, and enrichment is LLM generation, not EL. Don't force-fit either
tool onto a stage it wasn't designed for; revisit only if a stage's shape
actually matches what they're for (structured extract-load).

## Other tooling decisions

- **`raw_movies`** (Plex → DuckDB): full overwrite every run, no
  partitioning. Measured directly: 156 movies, ~3s end to end — cheap
  enough that re-fetching beats incremental-sync complexity.
- **`dagster_duckdb.DuckDBResource` adopted** over a hand-rolled version —
  same interface, plus Dagster's own retry/backoff on lock contention, no
  extra dependency cost. See `src/plex_ingest/defs/resources/duckdb.py`.
- **dbt adopted for the staging transform** (`stg_movies`) — resolving
  `imdb_id` out of Plex's raw `guids` is genuinely SQL-shaped, and
  `imdb_id` is the whole system's primary key, so dbt's `not_null`/`unique`
  tests are a real data-quality gate. Wired via `dagster_dbt.DbtProjectComponent`
  with automatic lineage from `raw_movies`. See
  `dbt_project/models/staging/`.

Environment/tooling gotchas that fell out of these decisions (Python 3.13
pin, mypy pin, `DAGSTER_HOME` requirements) are in `CLAUDE.md`, not
duplicated here.

## Working notes

- Default to the `dagster-expert` plugin/skill for any Dagster-specific
  work in this project (asset patterns, `dg` CLI usage, component
  selection) to keep the project consistent with Dagster conventions.
- Update this doc directly as decisions get made, revised, or superseded —
  it's the living record, not a point-in-time snapshot.
