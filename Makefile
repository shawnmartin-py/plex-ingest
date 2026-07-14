.PHONY: up pools dev seed seed-watch-history dev-docker pools-docker

up:
	docker compose up -d

pools:
	uv run dagster instance concurrency set gemini_llm 2
	uv run dagster instance concurrency set imdb_scrape 2
	uv run dagster instance concurrency set gemini_embeddings 2
	uv run dagster instance concurrency set groq_synopsis_judge 2

dev:
	uv run dg dev

dev-docker:
	docker compose up --build dagster

pools-docker:
	docker compose exec dagster dagster instance concurrency set gemini_llm 2
	docker compose exec dagster dagster instance concurrency set imdb_scrape 2
	docker compose exec dagster dagster instance concurrency set gemini_embeddings 2
	docker compose exec dagster dagster instance concurrency set groq_synopsis_judge 2

seed:
	uv run dg launch --assets raw_movies,stg_movies

seed-watch-history:
	uv run dg launch --assets stg_watch_history
