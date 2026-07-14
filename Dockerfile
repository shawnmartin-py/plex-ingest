FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /uvx /usr/local/bin/

WORKDIR /app

# Install deps in their own layer first so source edits don't bust the cache.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

COPY . .
RUN uv sync --frozen

# Chromium for the synopsis scraper (src/plex_ingest/lib/adapters/playwright_scraper.py).
# Installed via `uv run` rather than pinned in the image so the browser build always
# matches whatever `playwright` version uv.lock resolved, not a base-image guess.
RUN uv run playwright install --with-deps chromium

RUN chmod +x entrypoint.sh

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 3000

ENTRYPOINT ["./entrypoint.sh"]
