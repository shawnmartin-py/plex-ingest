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
  (CLI `chat` and the Streamlit app) it does a preflight check: connect to
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
| `imdb_id` | string | Primary key across both repos |
| `type` | string | `movie` (currently the only type synced) |
| `title` | string | |
| `year` | int | |
| `imdb_rating` | float | |
| `content_rating` | string | e.g. `PG-13` |
| `genres` | string | comma-joined, not a list (`", ".join(genres)`) |
| `thumb_url` | string \| null | Plex-hosted poster URL |
| `video_resolution` | string \| null | Raw Plex `Media.videoResolution` value (`sd`/`480`/`576`/`720`/`1080`/`4k`). Mutually exclusive with `source_platform` — null whenever `source_platform` is set. |
| `source_platform` | string \| null | `"Netflix"` or `"Disney+"` — set when the library item is actually a short placeholder clip standing in for a movie only available on that streaming platform (a real file, ~4s long, named `"Title - Year - (Platform).ext"`), not a real download. Mutually exclusive with `video_resolution`. |
| `embedding_type` | string | `"synopsis"` or `"enriched"` — see below |
| `section` | string | only present when `embedding_type == "enriched"`: `craft` / `meaning` / `context` |

### `page_content` by `embedding_type`

| `embedding_type` | `page_content` |
|---|---|
| `synopsis` | `"Title: {title}\nYear: {year}\nIMDb Rating: {imdb_rating}\nGenres: {genres}\nSynopsis: {synopsis}"` — full synopsis text lives here, not in `metadata` |
| `enriched` | The raw LLM-generated profile prose for that `section` |

One `imdb_id` produces up to 4 points total: one `synopsis` point plus up to
three `enriched` points (one per section). Retrievers filter by
`metadata.embedding_type` (and `metadata.section` where relevant) via
Qdrant `Filter`/`FieldCondition`.

## Non-contract items (intentionally NOT synced)

- **LLM model for generation** (`gemini-3.1-flash-lite` today) — used for
  chat generation, query rewriting, and HyDE in `plex-rag`, and for
  enrichment authoring in `plex-ingest`. These don't need to match each
  other; each repo can change its generation model independently since it
  never ends up embedded into a vector.
- **Safety settings** — each repo configures its own Gemini safety
  thresholds independently.
