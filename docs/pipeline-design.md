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

## Primary key: tmdb_id — migrated from imdb_id (2026-07-15)

Everything below that names `tmdb_id` as the partition key / filename stem /
Qdrant primary key originally shipped keyed by `imdb_id`; the migration to
TMDB ids happened 2026-07-15 (both GUIDs already flowed in via
`raw_movies.guids`, so only the `stg_movies` extraction, key plumbing, and a
one-off data migration — file renames plus a `stg_watch_history` rekey, see
`scripts/migrate_ids_to_tmdb.py` — were needed; nothing was re-scraped or
re-embedded). `imdb_id` remains a required column/metadata attribute because
IMDb synopsis scraping and OMDb runtime lookups are only addressable by
tt-id, and `stg_streaming_runtime` deliberately stays imdb-keyed (it caches
an imdb-keyed API). Two operational consequences to know about:

- **Every partition shows "never materialized" in the Dagster UI** after the
  migration: materialization history lives under the old `imdb_id` partition
  keys, which were deleted (`scripts/cleanup_old_partition_namespaces.py`).
  This is cosmetic — all triggering is disk-state- or event-based — do NOT
  "fix" it with a mass backfill, which would re-scrape/re-enrich the whole
  library.
- **The post-migration Qdrant rebuilds had to be triggered manually** (one
  materialization each of `qdrant_collection` and
  `watch_history_qdrant_collection`): with no new `embeddings` events under
  the new partition keys, no automation condition had anything to react to.

## Partitioning — decided (2026-07-05)

**Dynamic partitions keyed by `tmdb_id`**, applied to three assets:
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
- **Multi-dimensional partitions (`tmdb_id` × `section`, ~468 partitions)**
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

- **`synopsis`** — no `automation_condition`; the `sync_tmdb_id_partitions`
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
current run's tmdb_ids against the **registered dynamic partition set**
(not against "did stg_movies re-run" — a routine full-refresh of 156
already-known movies must not look like new data). For each tmdb_id:
- **New** → `add_dynamic_partitions`. The sensor then fills in
  `synopsis` → `enrichment` → `embeddings` for it, per the conditions above.
- **Removed** (no longer in Plex) → `delete_dynamic_partition` (shared
  across `synopsis`/`enrichment`/`embeddings` since they use the same
  `DynamicPartitionsDefinition` instance), plus deletion of the stale
  `synopsis/{tmdb_id}.json`, `enrichment/{tmdb_id}.json`, and
  `embeddings/{tmdb_id}.json` files. `delete_dynamic_partition` only
  removes the tmdb_id from the active partition set — it does not delete
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
`sync_tmdb_id_partitions` now also returns a `RunRequest` for
`qdrant_collection` whenever `removed_ids` is non-empty, so a pure removal
always triggers a rebuild directly rather than depending on an unrelated
future `embeddings` update.

**Regression, found and fixed 2026-07-11.** The pipeline is meant to only
ever contain unwatched movies — this was enforced in the legacy
`plex-rag`/`app/plex.py` (`Plex.get_media_items(..., unwatched=True)`,
default `True`, never overridden), but the filter was silently dropped in
the Dagster rewrite: `PlexMovieCatalog.fetch_raw_movies()` called
`section.search()` with no `unwatched` argument, and neither
`raw_movies`/`stg_movies` nor any doc here ever mentioned watched status.
Confirmed via `git log --all -S"viewCount"`/`-S"unwatched"` across both
repos — zero hits in this repo's history, meaning it wasn't consciously
redesigned, just never carried over. **Fix in place:** `view_count` is now
captured raw in `fetch_raw_movies()`/`raw_movies` (unfiltered, same split
already used for the both-guids-required rule — see the `stg_movies.sql` comment
below), and `stg_movies.sql` excludes `view_count != 0` in its final
`where`. This slots directly into the existing removal cascade above: a
movie that gets watched between runs simply drops out of `stg_movies`,
which the "no longer in Plex" partition-removal path already treats as a
removal — no new deletion logic needed. One consequence worth flagging for
whoever runs this next: the first `raw_movies`/`stg_movies` run after this
fix will cause every already-ingested-but-now-watched movie to be pruned
from the live Qdrant collection via that cascade, not just newly-watched
ones going forward.

## Intermediate/temp storage — decided (2026-07-05)

**Per-partition flat files, not DuckDB**, for the three partitioned
stages — one JSON file per movie per stage (`synopsis/{tmdb_id}.json`,
`enrichment/{tmdb_id}.json`, `embeddings/{tmdb_id}.json`, the last holding
each section's text alongside its embedding vector), via a custom
IOManager keyed off `context.partition_key`. DuckDB is single-writer (like
SQLite) — if these partitions run concurrently (intended, see above),
concurrent writers to one DuckDB file would hit lock contention, and
serializing them just to keep DuckDB would defeat the purpose of
partitioning for these stages.

DuckDB remains exactly as already decided for `raw_movies`/`stg_movies`
(genuinely SQL-shaped, single-writer, unpartitioned) — this only concerns
the three new per-movie partitioned stages. `qdrant_collection` reads
every `embeddings/{tmdb_id}.json` on disk directly; it has no storage
concern of its own.

## Asset boundary — decided (2026-07-05)

`raw_movies` → `stg_movies` (unpartitioned, as-is) → partition-sync sensor
→ `synopsis` (partitioned by `tmdb_id`) → `enrichment` (partitioned by
`tmdb_id`, depends on `synopsis`) → `embeddings` (partitioned by
`tmdb_id`, depends on `enrichment`) → `qdrant_collection` (**unpartitioned**,
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

`vector-store-contract.md` requires up to 4 points per `tmdb_id` (1
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

## Data-quality checks — decided (2026-07-06), then disabled the same day

Distinct from the `tests/` pytest suite (which tests *code*), the pipeline
needed a way to catch *bad data* — specifically, a scraped synopsis that
describes the wrong film (a mismatched franchise entry, an unrelated film
pulled in by the Wikipedia fallback's search step, or a scrape/search-cascade
failure returning boilerplate instead of a plot) before it reaches
`enrichment` (a paid, rate-limited Gemini call) or `embeddings`/
`qdrant_collection` (and therefore the recommender).

Dagster's native mechanism for this is an **asset check**
(`@dg.asset_check`), not a new asset — `defs/checks/synopsis_match.py`'s
`synopsis_matches_movie` attaches to `synopsis`, partitioned the same way
(`tmdb_id`). Textual heuristics alone (e.g. checking whether the synopsis
mentions the movie's own title, the way `playwright_scraper.py`'s
`_titles_match` guards the Wikipedia page-search step) aren't enough — plot
summaries rarely restate their own title, so that only catches gross
failures, not a fluent, on-topic synopsis for the wrong film. Judging actual
content match needs an LLM.

**Provider: Groq (`qwen/qwen3-32b`), not Gemini** — a deliberate second LLM
provider, chosen for its free-tier rate limits (RPM 60 / RPD 1000 / TPM 6000
/ TPD 500000) being cheap and separate from the `gemini_llm` pool enrichment
already competes for. `lib/adapters/groq_synopsis_judge.py` implements the
`SynopsisMatchJudge` port (`lib/ports.py`) behind
`defs/resources/synopsis_judge.py`'s `SynopsisJudgeResource`, same
port/adapter split as every other external system in this pipeline. Only a
leading excerpt of the synopsis is sent (`SYNOPSIS_EXCERPT_CHARS = 700`),
not the full text — enough for the judge to place the plot in the right
film without paying token cost for the whole thing on every partition.

**Severity: blocking, `AssetCheckSeverity.ERROR`.** `blocking=True` means a
run that includes `synopsis` and any of its downstream deps (`enrichment`,
`embeddings`) — which is exactly what `sync_tmdb_id_partitions` requests
together for a cold-start partition — halts those downstream assets if the
check fails, rather than merely logging a warning. Bad data physically
cannot reach Qdrant this way; a failed check requires a human to
investigate (re-scrape, fix the `stg_movies` row, etc.) rather than silently
degrading recommendation quality later. Pooled at `groq_synopsis_judge`
(see README's "Getting started" for the `dagster instance concurrency set`
command), same convention as every other rate-limited external call in this
pipeline.

**A `synopsis` that's `None`** (the scraper found nothing) passes the check
trivially without calling the judge — there's nothing to judge, and
`enrichment` already fails separately on a missing synopsis.

**Gotcha confirmed 2026-07-06: `qwen/qwen3-32b` is a reasoning model** that
by default prepends a `<think>...</think>` block before its actual answer,
which broke the MATCH/MISMATCH parser (`_parse_verdict` in
`groq_synopsis_judge.py`) in a live smoke test against the real API (mocked
unit tests didn't catch this, since they stub the chain's output directly).
Fixed by passing `reasoning_format="hidden"` to `ChatGroq`, which drops the
thinking block server-side — also saves the (billed) reasoning tokens.
Recalibrate if the configured model ever changes to a non-reasoning one
(where `reasoning_format` is a no-op) or a provider without this parameter.

**Gotcha confirmed 2026-07-06: a job selecting only checks (no plain asset)
can't be bulk-launched across partitions in Dagster 1.13.12.** The obvious
way to (re-)verify every already-scraped `synopsis` partition without
re-scraping is `dg.AssetSelection.checks_for_assets(synopsis)` in a
`define_asset_job`, then `dg launch --job ... --partition <id>` per
partition (or `dagster job backfill --all`). This fails with `CheckError:
Job has no PartitionsDefinition`, even when `partitions_def=tmdb_id_partitions`
is passed explicitly to `define_asset_job` — confirmed empirically that the
explicit value doesn't survive `Definitions` resolution
(`Definitions.get_job_def(...).partitions_def` comes back `None`) when the
job's selection contains only `AssetCheckKey`s and no plain `AssetKey`s.
Consistent with `partitions_def` on `@dg.asset_check` being a documented
Dagster preview feature (`PreviewWarning: ... currently in preview, and may
have breaking changes in patch version releases`) — this pipeline is
pinned to an exact Dagster version anyway (see CLAUDE.md), so the practical
risk is a future *upgrade* needing to re-check this, not anything broken
in current operation.

**Working alternative: `scripts/verify_synopsis_matches.py`.** Calls the
judge directly (the same thing the check op does) for every partition under
`data/synopsis/`, then records each result as a *runless* asset-check event
via `instance.report_runless_asset_event(AssetCheckEvaluation(...))` — no
run, no job, no partitions_def resolution involved, and results still show
up in the Dagster UI's checks history/health for `synopsis` exactly as if
the check had executed inside a real job. Confirmed working end-to-end
against the real instance/API. See README's "Testing" section for usage.

### Disabled 2026-07-06: the Groq/qwen3-32b judge is unreliable at scale

Running `scripts/verify_synopsis_matches.py` against the full ~156-movie
catalog (the first real bulk use of the check) surfaced a critical problem,
not a data problem:

- **~85% false-mismatch rate.** The overwhelming majority came back FAIL,
  including extremely well-known films with unquestionably correct
  IMDB/Wikipedia synopses on disk — *Toy Story 4*, *Glass Onion*, *Alien:
  Romulus*, *Deadpool & Wolverine*, *Godzilla Minus One*, *Sinners*.
- **Inconsistent with itself.** `tt0242888` (*The Sleeping Dictionary*) and
  `tt0361127` (*The Woodsman*) both scored PASS in an earlier ad hoc
  two-partition test, with reasoning that correctly described their real
  plots. In the full batch run minutes later, the *same two partitions*
  came back FAIL, with reasoning describing different, wrong plot elements
  that matched neither the real film nor the model's own earlier verdict.
- **Root cause (most likely):** the prompt asks the judge to verify the
  synopsis against "the film" using the model's own knowledge, rather than
  judging the text's internal plausibility. `qwen/qwen3-32b`'s training
  data can't reliably cover recent releases — several failure reasons
  literally say "no such film exists" or "not a 2025 release" for real,
  already-released movies — and its recall is inconsistent between calls
  even for older/famous titles. A "hidden" `<think>` block (see the
  `reasoning_format="hidden"` gotcha above) may also mean its chain-of-thought
  isn't fully deterministic at `temperature=0`, unlike a plain
  (non-reasoning) completion.
- **Also found, not yet fixed:** the full run crashed partway through on an
  unhandled `groq.InternalServerError` (503, "over capacity") —
  `GroqSynopsisJudge.check()`'s retry/backoff only catches
  `groq.RateLimitError` (429), not this. Left as-is since the judge itself
  needs replacing regardless.

**Decision: disable the check in production, keep all the code.**
`sync_tmdb_id_partitions` now passes `asset_check_keys=[]` on every
`RunRequest` (see that sensor's docstring/comment), so `synopsis_matches_movie`
never actually executes — `synopsis`/`enrichment`/`embeddings` run exactly
as if the check didn't exist. Nothing under `defs/checks/`,
`defs/resources/synopsis_judge.py`, or `lib/adapters/groq_synopsis_judge.py`
was deleted; `scripts/verify_synopsis_matches.py` still works for manual
spot-checks, you just can't trust its verdicts yet.

**Re-enabling this needs a judge with real search/grounding**, not just a
bigger static-knowledge model — the fundamental problem is verifying a
claim against ground truth the model wasn't trained on (recent releases),
which no amount of prompt tuning against a fixed-knowledge model fully
fixes. Options to evaluate when this gets revisited: a provider with
built-in web search/grounding (e.g. Gemini with Google Search grounding),
or restructuring the check to only judge internal textual consistency
(genre/era/setting plausibility) rather than real-world fact-matching,
which sidesteps the knowledge-cutoff problem entirely at the cost of
catching fewer real mismatches.

**Event log is polluted with these false results.** Both the two-partition
ad hoc test and the ~135-partition batch run before the crash were recorded
via `report_runless_asset_event`, so `synopsis`'s check history in the
Dagster UI currently shows mostly-false FAILs. Not yet cleaned up — do that
before trusting the UI's check history for anything, or before re-enabling.

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

1. **Neither sensor ran by default.** `sync_tmdb_id_partitions` had no
   `default_status=dg.DefaultSensorStatus.RUNNING`, and Dagster's
   auto-generated automation-condition sensor (which drives every
   `on_missing()`/`eager()` condition in this pipeline) also defaulted to
   `STOPPED`. Fixed by setting `default_status=RUNNING` on both, defined
   explicitly in `sync_tmdb_id_partitions.py`'s `defs` assembly.

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
   `automation_condition` at all — `sync_tmdb_id_partitions` is their sole
   trigger. On every tick, for every currently-desired partition, it checks
   on-disk file presence directly (`_missing_stage_assets`) and issues a
   `RunRequest` for whatever's missing, sidestepping the
   automation-condition cursor entirely. `embeddings` keeps its `eager()`
   condition for its own trigger (re-embed when `enrichment` changes,
   including a partition's first-ever embedding) — that path reacts to
   `any_deps_updated`, a recurring event unaffected by the one-shot
   `missing()` bug.

   **Correction, found and fixed 2026-07-14.** An earlier version of this
   fix reasoned that `embeddings`'s cold start needed the same direct-sensor
   treatment, so `sync_tmdb_id_partitions` also requested a
   `qdrant_collection` rebuild directly whenever a partition's `embeddings`
   needed backfilling. That conflated two different meanings of "cold
   start": the `evaluation_id == 0` bug above is a one-time historical
   event (the automation-condition cursor's literal first-ever tick, long
   past for a running instance), not a property of any individual
   partition's first materialization, which fires `any_deps_updated()`
   completely normally whenever it happens. The redundant direct trigger
   caused `qdrant_collection` to run once immediately (before `embeddings`
   had actually finished for the partition, producing a stale/wasted
   rebuild) and then again correctly once `qdrant_collection`'s own
   `eager()`-derived condition reacted to the real `embeddings` update.
   **Fix:** the direct trigger now fires only on `removed_ids` — the one
   case genuinely invisible to `eager()`, since file deletion is a raw
   filesystem write with no materialization event at all (see "Deletion /
   pruning cascade" below). A backfilled `embeddings` partition is left
   entirely to `qdrant_collection`'s own condition.

3. **Pure removals never triggered a `qdrant_collection` rebuild.** Covered
   above under [Deletion / pruning cascade](#deletion--pruning-cascade--decided-2026-07-05).

## Watch-history diversity-recommender pipeline — implemented (2026-07-12)

New feature in `plex-rag`: a second recommendation mode that suggests
movies *farthest* (semantically) from a recency-weighted embedding of the
user's watch history, instead of nearest-neighbor similarity — see
`plex-rag`'s `docs/diversity-recommender.md` for the read-side design.
This section covers the new data-pipeline this feature needs here.

Verified end-to-end by invoking the asset functions directly rather than
via `dg launch` — see CLAUDE.md's "Environment gotchas" for why (a
pre-existing, unrelated dbt issue currently blocks `dg` for the whole
repo). The sensor-driven path itself hasn't been exercised live yet;
worth confirming once that's fixed.

### Why this needs a new pipeline, not a live `plex-rag` call

Considered and rejected: computing watch-history embeddings live in
`plex-rag` on every app open. Rejected because (a) `gemini-embedding-001`
is rate-limited per-minute and per-day, and re-embedding the whole watch
history on every open risks quota exhaustion for no benefit; (b)
`plex-rag` has no Plex connection today (confirmed: zero references to
`plexapi`/`PlexServer` in its `app/`) — `plex-ingest` owning the Plex
connection is already a stated boundary in `plex-rag`'s README, and a
second live connection would break it. User confirmed minute-level
freshness isn't needed, so a periodic pipeline run is an acceptable trade
for avoiding both problems.

### Watch-history data availability — investigated 2026-07-12 against the live server

Plex's local history endpoint (`/status/sessions/history/all`) splits
roughly 50/50:

- **Resolvable** entries still have a `ratingKey` (still in the local
  library) — full metadata (`summary`, `Genre`, `Director`, `Guid` incl.
  `imdb://...`, ratings) available via `/library/metadata/{ratingKey}`.
- **Unresolvable** entries (deleted after watching, or marked watched
  manually for content never downloaded — e.g. watched on Netflix/Disney+
  and flagged by hand) — history gives **only** `title` and
  `originallyAvailableAt`. No genre, summary, guid, or imdb_id. Confirmed
  this is really all there is (hitting the `historyKey` resource directly
  returns the identical payload). Local search endpoints (`/hubs/search`,
  `/library/search`) don't help — both are scoped to the local library,
  not Plex's global catalog.

Unresolvable entries **do** fully resolve via Plex's cloud services:

1. `GET https://discover.provider.plex.tv/library/search?query={title}&searchTypes=movies&searchProviders=discover`
   (the `searchProviders` param is required — omitting it 400s)
2. Disambiguate the candidate list by exact match on
   `originallyAvailableAt` against the local history record's value
   (title collisions across years/remakes are common — 10 "Knock Knock"
   candidates spanning 1985-2021 came back for one query)
3. `GET https://metadata.provider.plex.tv/library/metadata/{guid-hash}`
   (the hash from the matched candidate's `guid`, e.g.
   `plex://movie/{hash}`) — returns full `summary`, `Genre`, `Director`,
   `Guid` (incl. `imdb://...`), ratings.

Tested against 9 real unresolvable history entries (Kika, Pacific Rim,
Blue Bayou, Melissa P., Mektoub My Love, Madrid 1987, Volver, Love and
Other Disasters, Fish Tank) — 9/9 resolved cleanly with an exact-date
match. This is the same undocumented API the Plex apps themselves appear
to use for "Go to Movie" from a history item; no official docs, could
change without notice.

Decision: apply this **same resolution process uniformly to every
watch-history entry**, resolvable or not, rather than branching logic per
bucket — simpler, and the next finding shows resolvable entries' extra
local richness isn't needed anyway.

### Short description is sufficient embedding input — tested 2026-07-12

Considered: whether Plex's short summary (~80 words) is rich enough to
embed meaningfully, given the *existing* `media_items` `synopsis` points
are actually full spoiler-laden plot synopses (~1000+ words, scraped via
`playwright_scraper.py`) — a ~20x length difference. Tested directly with
`gemini-embedding-001`: embedded the real ingested Glass Onion synopsis
point, Plex's own short Glass Onion summary, and an unrelated control
movie's full synopsis.

```
cos(long Glass Onion, short Glass Onion)                = 0.887   same movie, 20x length gap
cos(long Glass Onion, control [different movie, long])  = 0.714   different movie
cos(short Glass Onion, control [different movie, long]) = 0.712   different movie, mixed length
```

Same-movie similarity stayed well clear of different-movie similarity
regardless of length pairing — length isn't acting as a confound here
(n=1, worth a couple more spot checks before fully trusting it, but it
directly contradicted the concern that prompted the test). Decision: no
need for a third-party metadata service (OMDb/TMDb) — Plex's own short
summary is enough.

### Pipeline shape

New Qdrant collection `watch_history` (schema: `docs/vector-store-contract.md`
in both repos), kept separate from `media_items`. Implemented as
`defs/partitions.py`'s `watch_history_tmdb_id_partitions`,
`defs/sensors/sync_watch_history_partitions.py`,
`defs/assets/stg_watch_history.py`, `defs/assets/watch_history_embeddings.py`,
and `defs/assets/watch_history_qdrant_collection.py` — deliberately mirrors
the existing `tmdb_id_partitions` / `sync_tmdb_id_partitions` /
`qdrant_collection` pattern, reusing the existing `EmbeddingsResource`/
`gemini_embeddings` pool as-is.

Two deviations from that existing pattern, both driven by explicit
requirements from the design conversation rather than incidental —
worth knowing *that* they're deliberate even though the reasoning itself
now lives in the code's own docstrings, not duplicated here:

1. Partition sync is **add-only** — unlike the existing sensor, a tmdb_id
   already embedded is never re-embedded just because it later ages out of
   the fetch window (a rewatch could bring it back into relevance).
2. The relevance window is enforced at `watch_history_qdrant_collection`
   **query time, not upstream** — `stg_watch_history` itself is unbounded
   (an upsert, never pruned by age), following the "full rebuild is cheap
   once the expensive data exists" philosophy `qdrant_collection` already
   established.

**Correction, found and fixed 2026-07-14.** `sync_watch_history_partitions`
originally also mirrored `qdrant_collection`'s (pre-correction)
direct-trigger pattern: it requested `watch_history_qdrant_collection`
directly whenever anything got backfilled, plus a separate
`_qdrant_collection_needs_cold_start` check for the case where it had never
materialized at all. Both were removed alongside the equivalent fix to
`sync_tmdb_id_partitions` (see "Known gaps," item 2's correction) — the
same reasoning applies, and more cleanly here: `any_deps_updated()` (what
`watch_history_qdrant_collection`'s `eager()` condition reacts to) is a
plain materialization-event read off the instance's event log, unaffected
by which sensor (if any) requested the run that produced it, and fires
correctly on a partition's first-ever materialization just as reliably as
its hundredth. Unlike `qdrant_collection`, there was never a genuine
residual case here needing a direct trigger — this sensor is add-only and
never deletes anything, so there's no `removed_ids`-equivalent gap for
`eager()` to miss. `sync_watch_history_partitions` now only ever requests
`watch_history_embeddings`, and `watch_history_qdrant_collection` is left
entirely to its own `eager()` condition.

**dbt vs. Python for `stg_watch_history`: decided Python**, for both the
raw fetch and the dedupe/upsert transform (2026-07-12, user call — "the raw
should be python, then we can decide if dbt makes sense for
transformation"). Resolution needs live Discover API calls per title, not
a SQL transform of data already at rest, so it doesn't fit `stg_movies`'s
dbt-model shape. Whether dbt takes over the transform step specifically is
still open, but nothing is blocked on it.

**Scheduling cadence — decided (2026-07-14):** both `sync_tmdb_id_partitions`
and `sync_watch_history_partitions` moved from `60`/`120` seconds to a shared
`600` seconds. Neither sensor's reactivity was actually tick-rate-bound: a
backlog is fully queued the tick it's first noticed (Dagster's run queue
drains it via concurrency-pool slots regardless of how often the sensor
re-ticks afterward), the two DuckDB source tables
(`stg_movies`/`stg_watch_history`) only change once a day (via
`poll_plex_job`, below), and both sensors' backfilled assets share the same
2-wide `gemini_embeddings` pool, so tightening either sensor's interval
doesn't increase effective throughput — only the pool width does. Sub-minute
polling was pure overhead (a DuckDB query plus a file-stat check per
partition per stage, every tick, almost always a no-op). `600` still reacts
same-day to the daily upstream refresh and to failed-run retries.

## Daily entry-point schedule — decided (2026-07-14)

`raw_movies`, `stg_movies`, and `stg_watch_history` previously had no
automation of their own — pure manual entry points, relying on someone to
run `make seed`/`make seed-watch-history` before the downstream sensors
had anything to register. Fixed with a single `ScheduleDefinition`
(`poll_plex_job`, `src/plex_ingest/defs/schedules/poll_plex_daily.py`)
materializing all three at 1am UTC daily — a schedule rather than a
sensor or `automation_condition`, since this is a fixed-time trigger with
no dependency-aware logic needed (see the `dagster-expert` skill's
"Choosing an automation approach" guidance). `stg_movies` is included
explicitly by asset-key string, not left to run only via its `raw_movies`
dependency, since it carries no `automation_condition` of its own and
would otherwise never re-run after its first materialization.

### Still open

- **Schedule timezone** for `poll_plex_job` — currently UTC (Dagster's own
  default), not a measured local-time decision. No convention existed
  before this either way — see `sync_watch_history_partitions`'s
  "Scheduling cadence" item above, the first time cadence came up at all.

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
  `tmdb_id`/`imdb_id` out of Plex's raw `guids` is genuinely SQL-shaped, and
  `tmdb_id` is the whole system's primary key (with `imdb_id` still required
  alongside it for IMDb scraping/OMDb), so dbt's `not_null`/`unique`
  tests are a real data-quality gate. Wired via `dagster_dbt.DbtProjectComponent`
  with automatic lineage from `raw_movies`. See
  `dbt_project/models/staging/`.

Environment/tooling gotchas that fell out of these decisions (Python 3.13
pin, mypy pin, `DAGSTER_HOME` requirements) are in `CLAUDE.md`, not
duplicated here.

## Docker `dg dev` memory footprint — investigated 2026-07-14, fix deferred

The Dockerized `dagster` service's `dg dev` runs at ~973MB RSS (against a
5.785GiB container limit — not currently urgent, but investigated on
request). `docker top`-ing the container breaks it down:

| Process | RSS |
|---|---|
| `dagster api grpc --lazy-load-user-code` (the actual code server holding `plex_ingest.definitions`) | 374 MB |
| `dg dev` supervisor | 182 MB |
| `dagster-webserver` | 173 MB |
| `dagster._daemon` | 142 MB |
| `dagster code-server` supervisor (parent of the grpc process above) | 102 MB |
| misc (`uv run`, multiprocessing resource_tracker) | ~63 MB |

The webserver/daemon/supervisor overhead (~600MB) is Dagster's normal
dev-mode 3-process architecture and isn't reducible without dropping the
daemon (kills sensors/schedules) or the webserver (kills the UI) — not
worth it.

The one real lever is the 374MB code-server process: all seven
`defs/resources/*.py` files (`embeddings.py`, `enrichment_llm.py`,
`scraper.py`, `synopsis_judge.py`, `qdrant.py`, `plex.py`,
`plex_watch_history.py`) import their concrete adapter class (e.g.
`GeminiEmbeddingClient`, `GroqSynopsisJudge`, `PlaywrightSynopsisScraper`)
at module top-level, even though each is only ever constructed inside the
resource's own `_adapter()` method. Dagster must import every file under
`defs/` to build the asset graph, so this long-lived process eagerly pays
for every vendor SDK's import cost even though it never actually calls
`_adapter()` — that only happens in the short-lived per-run subprocess a
launched run executes in. Measured import cost inside the container
(`resource.getrusage(...).ru_maxrss` delta per module):

- `langchain_google_genai` (Gemini embeddings + enrichment): 86.5 MB
- `qdrant_client`: 34 MB
- `groq` + `langchain_groq`: 1.6 MB
- `playwright`: 4.1 MB
- `plexapi`: 2.6 MB

**Two-tier fix, not yet applied:**

1. Move each resource's adapter import from module top-level into its
   `_adapter()` method body (7 files). Behavior-preserving, low risk.
   Recovers ~95MB — everything above except `qdrant_client`.
2. `qdrant_client`'s 34MB survives step 1 because
   `lib/vector_store_contract.py` does
   `from qdrant_client.models import Distance` at module level, and it's
   imported by four *asset* files (`assets/embeddings.py`,
   `assets/qdrant_collection.py`, `assets/watch_history_embeddings.py`,
   `assets/watch_history_qdrant_collection.py`), which are always eagerly
   loaded to build the asset graph — no way around that part. Recovering
   this would mean replacing the imported `Distance` enum in
   `vector_store_contract.py` with our own domain constant instead of
   Qdrant's, which is a bigger change to a file shared across four assets,
   not a pure resource-layer tweak.

Deferred rather than applied on 2026-07-14 — revisit if the container's
footprint becomes an actual constraint (host memory pressure, tighter
`mem_limit`), not just a curiosity.

## Working notes

- Default to the `dagster-expert` plugin/skill for any Dagster-specific
  work in this project (asset patterns, `dg` CLI usage, component
  selection) to keep the project consistent with Dagster conventions.
- Update this doc directly as decisions get made, revised, or superseded —
  it's the living record, not a point-in-time snapshot.
