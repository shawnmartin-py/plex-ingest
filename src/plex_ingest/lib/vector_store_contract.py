import json
import uuid
from pathlib import Path

from qdrant_client.models import Distance

from plex_ingest.lib.media_source import StreamingSource, VideoResolution
from plex_ingest.lib.stg_movies_reader import MovieCatalogRow

# Mirrors docs/vector-store-contract.md. Keep in sync manually with plex-rag's copy.
EMBEDDING_MODEL = "gemini-embedding-001"
EMBEDDING_DIM = 3072
DISTANCE = Distance.COSINE

SYNOPSIS_KEY = "synopsis"

# Namespace for the deterministic point IDs qdrant_collection assigns to each
# (imdb_id, document key) pair — fixed forever so re-running the rebuild never changes
# an existing point's ID.
_POINT_ID_NAMESPACE = uuid.UUID("f4b2b1d0-6c0a-4a9e-9b0a-6b3c1e2f9a11")


def build_synopsis_document_text(
    title: str, year: int, imdb_rating: float | None, genres: list[str], synopsis: str
) -> str:
    """Matches plex-rag's MediaItem.to_document() exactly — field order and label text
    are part of the contract, not incidental formatting."""
    return (
        f"Title: {title}\n"
        f"Year: {year}\n"
        f"IMDb Rating: {imdb_rating}\n"
        f"Genres: {', '.join(genres)}\n"
        f"Synopsis: {synopsis}"
    )


def build_catalog_metadata(
    imdb_id: str,
    title: str,
    year: int,
    imdb_rating: float | None,
    content_rating: str | None,
    genres: list[str],
    thumb_url: str | None,
    video_resolution: VideoResolution | None,
    source_platform: StreamingSource | None,
) -> dict[str, object]:
    """Matches plex-rag's MediaItem.to_metadata() exactly — see "metadata fields" in
    vector-store-contract.md. `type` is hardcoded to "movie": stg_movies only ever
    carries movies (no `type` column), matching the contract's "currently the only
    type synced" note. `video_resolution`/`source_platform` are mutually exclusive
    (enforced in stg_movies.sql) and serialized as their raw enum value — plain
    strings on the wire, like every other contract field."""
    return {
        "imdb_id": imdb_id,
        "type": "movie",
        "title": title,
        "year": year,
        "imdb_rating": imdb_rating,
        "content_rating": content_rating,
        "genres": ", ".join(genres),
        "thumb_url": thumb_url,
        "video_resolution": video_resolution.value if video_resolution else None,
        "source_platform": source_platform.value if source_platform else None,
    }


def build_points(
    catalog: dict[str, MovieCatalogRow], embeddings_dir: Path
) -> list[tuple[str, list[float], str, dict[str, object]]]:
    """Assemble the (point_id, vector, page_content, metadata) tuples for every
    embeddings/{imdb_id}.json file on disk, joined against the stg_movies catalog.
    This *is* vector-store-contract.md's payload shape, so it lives alongside the
    other contract-shape builders rather than inline in the qdrant_collection asset —
    keeps the asset itself down to orchestration (fetch catalog, build points, call
    the resource)."""
    points: list[tuple[str, list[float], str, dict[str, object]]] = []
    for path in sorted(embeddings_dir.glob("*.json")):
        imdb_id = path.stem
        if imdb_id not in catalog:
            msg = (
                f"embeddings/{imdb_id}.json exists but no matching stg_movies row — "
                "partition sync is out of date"
            )
            raise ValueError(msg)
        movie = catalog[imdb_id]
        metadata_base = build_catalog_metadata(
            imdb_id,
            movie.title,
            movie.year,
            movie.imdb_rating,
            movie.content_rating,
            movie.genres,
            movie.thumb_url,
            movie.video_resolution,
            movie.source_platform,
        )

        documents = json.loads(path.read_text())
        for key, data in documents.items():
            point_id = str(uuid.uuid5(_POINT_ID_NAMESPACE, f"{imdb_id}:{key}"))
            metadata = {
                **metadata_base,
                "embedding_type": "synopsis" if key == SYNOPSIS_KEY else "enriched",
            }
            if key != SYNOPSIS_KEY:
                metadata["section"] = key
            points.append((point_id, data["vector"], data["text"], metadata))
    return points
