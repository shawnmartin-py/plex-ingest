# plex-ingest

Dagster-based data pipeline for `plex-rag`: polling Plex, scraping
synopses, generating LLM enrichments, and embedding everything into a
Qdrant vector store. This repo owns all writes to Qdrant; `plex-rag`
(sibling repo) is a read-only consumer of the collection this pipeline
produces. See `README.md` for setup and `docs/vector-store-contract.md` for
the data contract between the two repos.

This repo is part of the `plex-ingest-extraction` epic tracked in
`plex-rag` (sibling repo, lives at
`/Users/shawnmartin/projects/python/plex-project`) —
`docs/epics/plex-ingest-extraction/breakdown.md` there has the phased
plan, and `phase-2-pipeline-design.md` has the architectural decisions for
this phase (partitioning, storage, deletion cascade, automation semantics,
Qdrant payload shape — all decided; only the LlamaIndex/LangChain
framework choice is still open).

**Before resuming epic work, check `plex-rag`'s repo root for a
`HANDOFF-plex-ingest-extraction-*.md` file** — it has current progress and
concrete next steps in more detail than the epic docs alone.

## Engineering standards

This is a data pipeline built by software engineers, not a data science
notebook. Apply the same rigor here as in any production service, even
while the pipeline is still prototype-stage:

- **SOLID, decoupled design from the start.** Prototype status is not
  license to skip separation of concerns — it means the scope is small,
  not that the structure is sloppy.
- **Dagster: strict separation between resources and assets.**
  Resources (external system clients: Qdrant, embeddings, Plex, etc.) live
  under `src/plex_ingest/defs/resources/`, one file per integration.
  Assets live under `src/plex_ingest/defs/assets/` and should be thin
  orchestration — call into resource methods, don't reimplement
  connection/client logic inline in the asset function.
- **Domain constants and non-Dagster logic live outside `defs/`.**
  E.g. `src/plex_ingest/lib/vector_store_contract.py` holds the embedding
  model/dimension/distance constants shared by multiple resources — these
  aren't Dagster-specific, so they don't belong inside a resource or asset
  file.
- **Always consult the `dagster-expert` skill** before writing Dagster
  code, scaffolding a project, or choosing between a plain resource, a
  Pythonic integration, and a full Component — don't improvise Dagster
  patterns from general knowledge.
- **Files named and placed correctly.** No dumping unrelated logic into
  whatever file is open; match the existing `defs/{assets,resources}` /
  `lib/` split as the pipeline grows.
- **Pipeline architecture decisions are joint, not unilateral.** The
  LlamaIndex/LangChain framework choice is still open — see
  `phase-2-pipeline-design.md` in `plex-rag`. Surface options and a
  recommendation; the user makes the final call. (Partitioning, storage,
  deletion cascade, automation semantics, and dbt-for-staging are already
  decided; Sling and dlt were evaluated and rejected for Plex extraction —
  see that doc's "Decisions made so far" before re-litigating any of it.)
- **A contract-compliance gap is a bug to fix, not a design question.**
  `docs/vector-store-contract.md` is the already-agreed source of truth for
  what gets written to Qdrant. If an asset's actual output doesn't match it
  (wrong metadata fields, missing point types), that's a defect — fix it
  directly rather than treating it as something needing a fresh joint
  decision. (This happened once already: `embeddings`/`qdrant_collection`
  initially missed the synopsis-type point and full catalog metadata —
  see `phase-2-pipeline-design.md`'s "Qdrant payload shape" for what was
  wrong and how it was fixed.)

## Pre-commit is enforced, not advisory

The git hook is installed (`pre-commit install` has been run against this
repo's `.git`), so `ruff`, `ruff-format`, `mypy`, and the generic hygiene
hooks (trailing whitespace, YAML/JSON validation, `detect-secrets`,
`yamlfmt`) run automatically on every `git commit` and block the commit on
failure. Run `pre-commit run --all-files` after any nontrivial change
instead of waiting to find out at commit time. Treat lint/type errors as
build breaks, not suggestions.

If you add a new top-level module under `src/plex_ingest/`, keep
`src/plex_ingest/py.typed` in place — mypy needs it to type-check
cross-module imports within this package (PEP 561 marker).

## Environment gotchas (confirmed, not guessed)

- **This project is pinned to Python 3.13 (`>=3.13,<3.14`), not 3.14.**
  `dbt-core`/`dbt-common` (via `mashumaro`'s JSON-schema codegen) fail to
  import at all on Python 3.14 — confirmed with the latest `mashumaro`
  release (3.22) too, so it's not a version-pin fix, it's a real
  incompatibility. Don't bump `requires-python`/`.python-version` back to
  3.14 without first confirming upstream has fixed this.
- **The `mypy` pre-commit hook is pinned to `mypy==1.9.0`, not latest.**
  `mypy>=1.20` requires a `pathspec` API (`patterns.gitignore`) that
  `dbt-core`'s dependency chain resolves below (`pathspec==0.12.1`) — since
  the hook's `entry` installs this project's own dependencies into the same
  venv as mypy (`uv pip install .`), a newer mypy breaks there even though
  the project's own `.venv` is unaffected. If `dbt-core` is ever removed,
  this pin can likely be relaxed again.
- **`DAGSTER_HOME` must be set to a persistent directory, not left unset.**
  Dynamic partitions (`imdb_id`, shared by `synopsis`/`enrichment`/
  `embeddings`) and the concurrency pool limits below live in the
  instance's storage. Without it, `dg`/`dagster` CLI invocations fall back
  to an ephemeral instance, and dynamic partitions added in one process
  vanish before the next `dg launch` sees them. See `.env.example`.
- **Concurrency pool limits (`gemini_llm`, `imdb_scrape`) are set via
  `dagster instance concurrency set <pool> <limit>`, not code or YAML.**
  They're instance state, not part of the asset definitions — re-run this
  after ever recreating `DAGSTER_HOME` from scratch. See README's "Getting
  started" for the exact commands.
- **`PLEX_INGEST_PARTITION_LIMIT` caps the dev/test footprint.** While this
  pipeline is being proven out, the `sync_imdb_id_partitions` sensor only
  ever registers this many imdb_ids as partitions, regardless of library
  size — currently `3`. Don't remove this without confirming the pipeline
  has been run successfully against the full library and has adequate test
  coverage first (see `phase-2-pipeline-design.md`'s framing of this as a
  deliberate, temporary safety rail, not a permanent limitation).
- **`sync_imdb_id_partitions` and `default_automation_condition_sensor` now
  both default to `RUNNING`** (fixed 2026-07-05 — see
  `phase-2-pipeline-design.md`'s "Known gaps found during dev-subset
  verification", gap #1) **on a fresh code location/instance.** A
  `DAGSTER_HOME` created *before* this fix landed may still have a
  persisted `STOPPED` state for either sensor — check **Automation →
  Sensors** in the UI (or the webserver's GraphQL `sensorsOrError` query)
  and toggle on manually if so; `default_status` only governs the initial
  state the first time Dagster sees that instigator, not an existing
  persisted state. Don't use the bare `dagster sensor start <name>` CLI (no
  `-w`/`-l`) to toggle — it can resolve a different code-location identity
  than `dg dev`'s own workspace (`plex_ingest.definitions` vs.
  `plex-ingest`) and silently toggle a phantom instigator state the running
  daemon never looks at. Use a `startSensor` GraphQL mutation against the
  running webserver (or the UI) instead.
- **`on_missing()`/`eager()` never pick up a partition (or asset) that was
  already missing at `evaluation_id == 0`** — the literal first-ever
  evaluation of a freshly created automation-condition cursor, not
  ordinary daemon restarts (which preserve the cursor fine). If a
  partition is *already* missing at that one moment, it's stuck forever —
  confirmed deterministically in
  `tests/integration/test_automation_condition_cold_start.py`, and this is
  a genuine Dagster (1.13.12) behavior, not specific to this pipeline.
  **`synopsis`/`enrichment` no longer use `automation_condition` at all**
  because of this — `sync_imdb_id_partitions` is their sole trigger,
  checking on-disk file presence directly every tick instead of relying on
  the cursor. Don't add `on_missing()`/`eager()` back to either asset
  without re-reading `phase-2-pipeline-design.md`'s "Known gaps", item 2.
  `embeddings`/`qdrant_collection` still use `eager()` for their ordinary
  steady-state cascade (unaffected — it reacts to `any_deps_updated`, a
  recurring event, not the one-shot `missing()` transition), with the same
  sensor providing a direct backfill as a supplement for their own
  cold-start case.
