.PHONY: up pools dev seed

up:
	docker compose up -d

pools:
	uv run dagster instance concurrency set gemini_llm 2
	uv run dagster instance concurrency set imdb_scrape 2
	uv run dagster instance concurrency set gemini_embeddings 2
	uv run dagster instance concurrency set groq_synopsis_judge 2

dev:
	uv run dg dev

seed:
	uv run dg launch --assets raw_movies,stg_movies
