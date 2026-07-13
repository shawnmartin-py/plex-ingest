import json
import uuid
from datetime import datetime
from pathlib import Path

from qdrant_client.models import Distance

from plex_ingest.lib.media_source import HdrFormat, StreamingSource, VideoResolution
from plex_ingest.lib.stg_movies_reader import MovieCatalogRow
from plex_ingest.lib.watch_history_reader import WatchHistoryRow

# Mirrors docs/vector-store-contract.md. Keep in sync manually with plex-rag's copy.
EMBEDDING_MODEL = "gemini-embedding-001"
EMBEDDING_DIM = 3072
DISTANCE = Distance.COSINE

SYNOPSIS_KEY = "synopsis"

# Namespace for the deterministic point IDs qdrant_collection assigns to each
# (imdb_id, document key) pair — fixed forever so re-running the rebuild never changes
# an existing point's ID.
_POINT_ID_NAMESPACE = uuid.UUID("f4b2b1d0-6c0a-4a9e-9b0a-6b3c1e2f9a11")

# Separate namespace for watch_history_qdrant_collection's points — deliberately
# distinct from _POINT_ID_NAMESPACE even though the two collections never share point
# IDs anyway (different Qdrant collections), keeping the two ID spaces independent.
_WATCH_HISTORY_POINT_ID_NAMESPACE = uuid.UUID("a3e7c9d2-1f4b-4e6a-8c3d-2b9f7e1a5d60")


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
    description: str | None,
    genres: list[str],
    thumb_url: str | None,
    video_resolution: VideoResolution | None,
    hdr_formats: list[HdrFormat],
    source_platform: StreamingSource | None,
) -> dict[str, object]:
    """Matches plex-rag's MediaItem.to_metadata() exactly — see "metadata fields" in
    vector-store-contract.md. `type` is hardcoded to "movie": stg_movies only ever
    carries movies (no `type` column), matching the contract's "currently the only
    type synced" note. `video_resolution`/`source_platform` are mutually exclusive
    (enforced in stg_movies.sql) and serialized as their raw enum value — plain
    strings on the wire, like every other contract field. `hdr_formats` is a list
    (unlike every other field here) since a movie can be both HDR- and
    Dolby-Vision-encoded at once — see `HdrFormat`'s docstring; also nulled to `[]`
    for placeholder clips in stg_movies.sql, same as `video_resolution`->`None`.
    `description` is Plex's own short blurb (`Movie.summary`) — a display-only field,
    deliberately not folded into `build_synopsis_document_text`'s embedded text,
    unlike the scraped `synopsis`."""
    return {
        "imdb_id": imdb_id,
        "type": "movie",
        "title": title,
        "year": year,
        "imdb_rating": imdb_rating,
        "content_rating": content_rating,
        "description": description,
        "genres": ", ".join(genres),
        "thumb_url": thumb_url,
        "video_resolution": video_resolution.value if video_resolution else None,
        "hdr_formats": [fmt.value for fmt in hdr_formats],
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
            movie.description,
            movie.genres,
            movie.thumb_url,
            movie.video_resolution,
            movie.hdr_formats,
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


def build_watch_history_metadata(
    imdb_id: str,
    title: str,
    year: int,
    imdb_rating: float | None,
    genres: list[str],
    last_viewed_at: datetime,
) -> dict[str, object]:
    """Matches the `watch_history` collection's "metadata fields" in
    vector-store-contract.md — a smaller field set than `build_catalog_metadata`'s
    (no content_rating/thumb_url/video_resolution/source_platform: those are
    media_items-specific display fields this collection has no use for), plus
    `last_viewed_at`, which media_items has no equivalent of."""
    return {
        "imdb_id": imdb_id,
        "title": title,
        "year": year,
        "imdb_rating": imdb_rating,
        "genres": ", ".join(genres),
        "last_viewed_at": last_viewed_at.isoformat(),
    }


def build_watch_history_points(
    watch_history: dict[str, WatchHistoryRow],
    embeddings_dir: Path,
    in_window_ids: set[str],
) -> list[tuple[str, list[float], str, dict[str, object]]]:
    """Assemble the (point_id, vector, page_content, metadata) tuples for every
    embeddings/watch_history/{imdb_id}.json file on disk whose imdb_id is in
    `in_window_ids`, joined against `watch_history` (the *full*, unwindowed
    stg_watch_history rows — see `fetch_all_watch_history`). Simpler than
    `build_points`: exactly one point per imdb_id here (no embedding_type/section
    split — see vector-store-contract.md's `watch_history` collection section), so
    each embeddings file holds a single {"text": ..., "vector": ...} object rather
    than a dict of documents.

    `watch_history` and `in_window_ids` are deliberately separate: an embeddings file
    with no row in `watch_history` at all means partition sync is out of date (a real
    bug, raised below); an embeddings file whose imdb_id has a row but isn't in
    `in_window_ids` just means it aged past the relevance window (expected, silently
    excluded, not a bug — its embedding stays cached for if a rewatch brings it back
    into the window later)."""
    points: list[tuple[str, list[float], str, dict[str, object]]] = []
    for path in sorted(embeddings_dir.glob("*.json")):
        imdb_id = path.stem
        if imdb_id not in watch_history:
            msg = (
                f"embeddings/watch_history/{imdb_id}.json exists but no matching "
                "stg_watch_history row — partition sync is out of date"
            )
            raise ValueError(msg)
        if imdb_id not in in_window_ids:
            continue
        row = watch_history[imdb_id]
        metadata = build_watch_history_metadata(
            imdb_id,
            row.title,
            row.year,
            row.imdb_rating,
            row.genres,
            row.last_viewed_at,
        )
        document = json.loads(path.read_text())
        point_id = str(uuid.uuid5(_WATCH_HISTORY_POINT_ID_NAMESPACE, imdb_id))
        points.append((point_id, document["vector"], document["text"], metadata))
    return points
