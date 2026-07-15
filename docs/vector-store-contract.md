# Vector Store Contract

This is the data-level contract between `plex-rag` (the recommender) and
`plex-ingest` (the data pipeline, Dagster-based, in a separate repo). It is
the one thing that must stay in sync **manually** across the two repos —
there is no shared code package enforcing it, by deliberate choice (a
shared package would re-couple two repos with very different stacks for
very little payoff). This is the `plex-ingest` copy; keep it in sync with
the `plex-rag` copy by hand.

Nothing in this doc is aspirational — it describes exactly what
`plex-ingest` writes and what `plex-rag` reads. If you change one side,
change the other side and this doc in the same change.

## Qdrant deployment

- Qdrant runs as a **Docker container** (server mode), not the embedded
  on-disk client. Both repos connect over the network via
  `QdrantClient(url=...)`, never `QdrantClient(path=...)`.
- The `docker-compose.yml` defining the Qdrant service and its persistent
  volume lives in `plex-ingest` (the data-owning repo).
- `plex-rag` does **not** start or manage the Qdrant container. On startup
  (CLI `chat` and the NiceGUI web app) it does a preflight check: connect to
  the configured URL, confirm the collection exists, and confirm
  `vectors_config.size` matches [Embedding model](#embedding-model) below.
  If the check fails, fail fast with a clear "Qdrant isn't reachable / start
  it in plex-ingest first" message — don't try to reach into the sibling
  repo to start it.
- Connection is env-var driven on both sides: `QDRANT_URL` (e.g.
  `http://localhost:6333`), `QDRANT_COLLECTION`. Both repos must point at
  the same collection name.

## Embedding model

- Model: `gemini-embedding-001`
- Dimensions: `3072`
- Distance metric: Cosine

Both repos construct their embeddings client with this exact model string —
`plex-ingest` to embed documents at write time, `plex-rag` to embed queries
at read time. A mismatch here doesn't error; it silently produces
meaningless similarity scores, which is why the recommender's preflight
check validates vector size at startup as a cheap early warning.

## Payload shape

Every Qdrant point is written via `langchain_qdrant.QdrantVectorStore` with
its defaults: `content_payload_key="page_content"`,
`metadata_payload_key="metadata"`. So every point's payload looks like:

```json
{
  "page_content": "<embedded text — see below>",
  "metadata": { "...": "..." }
}
```

`plex-rag` reads movie metadata **exclusively from this payload** — it has
no direct database (SQLite/DuckDB/parquet) dependency on `plex-ingest`'s
internal storage. This is a deliberate simplification: the Qdrant collection
is the only cross-repo data dependency.

### `metadata` fields (all point types)

| Field | Type | Notes |
|---|---|---|
| `tmdb_id` | string | **Primary key across both repos** — the numeric TMDB id as a string (e.g. `"603"`, from the movie's `tmdb://` Plex guid). On the ingest side it is also the Dagster partition key, the on-disk filename stem (`data/*/{tmdb_id}.json`), and the point-ID derivation input. Never an int on the wire. |
| `imdb_id` | string | IMDb id (`tt`-prefixed) — a plain metadata attribute since the 2026-07 tmdb_id migration, no longer the primary key. Still structurally required upstream (IMDb synopsis scraping and OMDb runtime lookups only accept tt-ids) and used by `plex-rag` for IMDb links, so every movie carries both ids. |
| `type` | string | `movie` (currently the only type synced) |
| `title` | string | |
| `year` | int | |
| `imdb_rating` | float | Extracted from Plex's `Rating` list, filtered to the entry whose `image` starts with `imdb://` — same extraction the `watch_history` collection below already used; `media_items` used to blindly take index `[0]` (could mis-tag a Rotten Tomatoes/TMDb score as `imdb_rating` if Plex ever ordered its ratings list differently), fixed to match. |
| `content_rating` | string | The maturity/age rating, distinct from `imdb_rating` — e.g. `PG-13` (US/MPAA) or `15` / `12A` (UK/BBFC). Plex's raw `contentRating` is locale-tagged whenever a non-US rating board resolved it (`"gb/15"`) — the `gb/`-style prefix is stripped at extraction (`plex_catalog.py`) so this is always the bare rating token. |
| `description` | string \| null | Plex's own short blurb (`Movie.summary`) — a promotional-length, spoiler-free synopsis, distinct from the long scraped `synopsis` in `page_content` below. Display-only: not embedded, not folded into any `page_content`. |
| `genres` | string | comma-joined, not a list (`", ".join(genres)`) |
| `thumb_url` | string \| null | Plex-hosted poster URL |
| `video_resolution` | string \| null | Raw Plex `Media.videoResolution` value (`sd`/`480`/`576`/`720`/`1080`/`4k`). Mutually exclusive with `source_platform` — null whenever `source_platform` is set. |
| `hdr_formats` | list[string] | Zero or more of `"HDR"` / `"DV"` — not mutually exclusive (a Dolby Vision profile 7/8 dual-layer file signals both at once). `"HDR"` is a single flat bucket covering every HDR transfer function Plex reports (HDR10, HDR10+, HLG) — plexapi has no separate HDR10 vs HDR10+ signal. `[]` for a real download with no HDR/DV, and also `[]` (not meaningful) for placeholder clips, same treatment as `video_resolution` -> `null`. |
| `source_platform` | string \| null | `"Netflix"` or `"Disney+"` — set when the library item is actually a short placeholder clip standing in for a movie only available on that streaming platform (a real file, ~4s long, named `"Title - Year - (Platform).ext"`), not a real download. Mutually exclusive with `video_resolution`. |
| `embedding_type` | string | `"synopsis"` or `"enriched"` — see below |
| `section` | string | only present when `embedding_type == "enriched"`: `craft` / `meaning` / `context` |
| `runtime_minutes` | int \| null | For a real download: derived from Plex's `Media.duration`, rounded to the nearest minute. For a streaming-platform placeholder clip (`source_platform` set): Plex's own duration is meaningless — confirmed empirically against real placeholder items, both top-level `duration` and `media[0].duration` reflect only the ~4s stand-in file, never the real film — so this instead comes from a targeted OMDb API lookup (free, licensed, keyed by `imdb_id`) done only for that subset (`streaming_runtime` asset). `null` when unresolved: `OMDB_API_KEY` isn't configured (a deliberate no-op, not an error — see that asset's docstring), OMDb has no runtime for the title, or the lookup hasn't run yet. |

### `page_content` by `embedding_type`

| `embedding_type` | `page_content` |
|---|---|
| `synopsis` | `"Title: {title}\nYear: {year}\nIMDb Rating: {imdb_rating}\nGenres: {genres}\nSynopsis: {synopsis}"` — full synopsis text lives here, not in `metadata` |
| `enriched` | The raw LLM-generated profile prose for that `section` |

One `tmdb_id` produces up to 4 points total: one `synopsis` point plus up to
three `enriched` points (one per section). Retrievers filter by
`metadata.embedding_type` (and `metadata.section` where relevant) via
Qdrant `Filter`/`FieldCondition`.

## `watch_history` collection — implemented (2026-07-12)

A second, separate collection populated by `plex-ingest`'s watch-history
pipeline (see this repo's `docs/pipeline-design.md`, "Watch-history
diversity-recommender pipeline") and read by `plex-rag`'s diversity
recommender (see `plex-rag`'s `docs/diversity-recommender.md`). Kept
separate from `media_items` — different lifecycle (add-only, with the
relevance window enforced at query time rather than by deleting old data)
and a different source (watch history, not the unwatched catalog).

- **Connection:** same Qdrant deployment and client as `media_items`, a
  different collection name — new env var `QDRANT_WATCH_HISTORY_COLLECTION`
  (default `watch_history`) on both sides, analogous to `QDRANT_COLLECTION`.
- **Embedding model:** same as `media_items` — `gemini-embedding-001`, 3072
  dimensions, cosine distance.
- **Payload shape:** same `QdrantVectorStore` defaults
  (`page_content` + `metadata`). Only one point type per `tmdb_id` — no
  `embedding_type`/`section` split needed, unlike `media_items`.

### `metadata` fields

| Field | Type | Notes |
|---|---|---|
| `tmdb_id` | string | Primary key, same meaning as in `media_items` |
| `imdb_id` | string | Metadata attribute, same meaning as in `media_items` |
| `title` | string | |
| `year` | int | |
| `imdb_rating` | float | Extracted from Plex's `Rating` list, filtered to the entry whose `image` starts with `imdb://` — same extraction `media_items` already does |
| `genres` | string | comma-joined, same convention as `media_items` |
| `last_viewed_at` | string | ISO 8601 (`datetime.isoformat()`), naive/no tzinfo — matches Plex's own local-server timestamps. The most recent `viewedAt` for this `tmdb_id`; Plex history can contain repeat watches, this collection dedupes to one point per `tmdb_id`, keeping the max |

### `page_content`

```
Title: {title}
Year: {year}
IMDb Rating: {imdb_rating}
Genres: {genres}
Synopsis: {summary}
```

Deliberately the same shape as `media_items`' `synopsis` `page_content` —
see this repo's `docs/pipeline-design.md` for why a short Plex-provided
summary here (rather than a full scraped synopsis) tested as sufficient
embedding input. `summary` itself is not stored as a separate metadata
field, only embedded in `page_content` — consistent with how `media_items`
stores full synopsis text only in `page_content`, not `metadata`.

## Migration note (2026-07-15): imdb_id -> tmdb_id primary key

The primary key across both repos switched from `imdb_id` to `tmdb_id`.
Point IDs are deterministic UUIDv5s whose input string embeds the primary
key, so every point ID changed in that migration — safe because both
collections are full delete+rebuild on every write (`recreate_collection`),
and executed as one full rebuild per collection at cutover. `imdb_id`
remains in every payload as a plain attribute, which is what made the
cutover non-breaking for a `plex-rag` deployed against the old contract.

## Non-contract items (intentionally NOT synced)

- **LLM model for generation** (`gemini-3.1-flash-lite` today) — used for
  chat generation, query rewriting, and HyDE in `plex-rag`, and for
  enrichment authoring in `plex-ingest`. These don't need to match each
  other; each repo can change its generation model independently since it
  never ends up embedded into a vector.
- **Safety settings** — each repo configures its own Gemini safety
  thresholds independently.
