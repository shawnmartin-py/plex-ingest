#!/usr/bin/env bash
set -euo pipefail

# Idempotent — instance concurrency limits are stored in DAGSTER_HOME, not code
# (see README's "Environment gotchas"), so this needs to run every container
# start, not just once. Re-setting an already-set limit is a no-op.
for pool in gemini_llm imdb_scrape gemini_embeddings groq_synopsis_judge; do
  uv run dagster instance concurrency set "$pool" 2
done

exec uv run dg dev -h 0.0.0.0 -p 3000
