import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import dagster as dg
import pytest
from pytest_mock import MockerFixture

from plex_ingest.defs.assets.watch_history_qdrant_collection import (
    watch_history_qdrant_collection,
)

_NOW = datetime.now(UTC).replace(tzinfo=None)

# Matches watch_history_reader._COLUMNS order: imdb_id, title, year, genres,
# imdb_rating, summary, last_viewed_at.
WatchHistoryDbRow = tuple[str, str, int, list[str], float | None, str, datetime]


def _watch_history_row(
    imdb_id: str, title: str = "Test Film", days_ago: int = 1
) -> WatchHistoryDbRow:
    return (
        imdb_id,
        title,
        2020,
        ["Drama"],
        7.5,
        "A summary.",
        _NOW - timedelta(days=days_ago),
    )


def _mock_duckdb(mocker: MockerFixture, rows: list[WatchHistoryDbRow]) -> MagicMock:
    mock_duckdb = cast(MagicMock, mocker.MagicMock())
    mock_conn = mock_duckdb.get_connection.return_value.__enter__.return_value
    mock_conn.execute.return_value.fetchall.return_value = rows
    return mock_duckdb


def _write_embeddings_fixture(
    embeddings_dir: Path, imdb_id: str, text: str = "text"
) -> None:
    embeddings_dir.mkdir(parents=True, exist_ok=True)
    (embeddings_dir / f"{imdb_id}.json").write_text(
        json.dumps({"text": text, "vector": [0.1, 0.2]})
    )


def test_rebuilds_from_embeddings_within_window(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    import plex_ingest.defs.assets.watch_history_qdrant_collection as module

    mocker.patch.object(module, "PLEX_INGEST_DATA_DIR", str(tmp_path))
    embeddings_dir = tmp_path / "embeddings" / "watch_history"
    _write_embeddings_fixture(embeddings_dir, "tt0001")

    mock_qdrant = mocker.MagicMock()
    mock_qdrant.collection = "watch_history"
    mock_qdrant.point_count.return_value = 1
    mock_duckdb = _mock_duckdb(mocker, [_watch_history_row("tt0001", days_ago=1)])

    watch_history_qdrant_collection(dg.build_asset_context(), mock_qdrant, mock_duckdb)

    mock_qdrant.recreate_collection.assert_called_once()
    (points,), _ = mock_qdrant.upsert_points.call_args
    assert len(points) == 1


def test_excludes_embedding_for_a_movie_aged_out_of_window_without_raising(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """Core behavior this asset exists to get right: an embedding whose row is older
    than the relevance window is silently excluded, not treated as a sync error --
    the cached embedding file is untouched (add-only pipeline, see the sync sensor)."""
    import plex_ingest.defs.assets.watch_history_qdrant_collection as module

    mocker.patch.object(module, "PLEX_INGEST_DATA_DIR", str(tmp_path))
    embeddings_dir = tmp_path / "embeddings" / "watch_history"
    _write_embeddings_fixture(embeddings_dir, "tt0001")

    mock_qdrant = mocker.MagicMock()
    mock_qdrant.collection = "watch_history"
    mock_qdrant.point_count.return_value = 0
    # Row exists but is 90 days old -- outside the 60-day relevance window.
    mock_duckdb = _mock_duckdb(mocker, [_watch_history_row("tt0001", days_ago=90)])

    watch_history_qdrant_collection(dg.build_asset_context(), mock_qdrant, mock_duckdb)

    mock_qdrant.upsert_points.assert_not_called()
    assert (embeddings_dir / "tt0001.json").exists()


def test_raises_when_embeddings_file_has_no_matching_row_at_all(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """Distinct from aging out: no row for this imdb_id in stg_watch_history at all
    means partition sync is genuinely out of date -- still a real bug, still raised."""
    import plex_ingest.defs.assets.watch_history_qdrant_collection as module

    mocker.patch.object(module, "PLEX_INGEST_DATA_DIR", str(tmp_path))
    embeddings_dir = tmp_path / "embeddings" / "watch_history"
    _write_embeddings_fixture(embeddings_dir, "tt0001")

    mock_qdrant = mocker.MagicMock()
    mock_duckdb = _mock_duckdb(mocker, [])  # no rows at all

    with pytest.raises(ValueError, match="tt0001"):
        watch_history_qdrant_collection(
            dg.build_asset_context(), mock_qdrant, mock_duckdb
        )


def test_point_metadata_has_no_embedding_type_or_section(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    import plex_ingest.defs.assets.watch_history_qdrant_collection as module

    mocker.patch.object(module, "PLEX_INGEST_DATA_DIR", str(tmp_path))
    embeddings_dir = tmp_path / "embeddings" / "watch_history"
    _write_embeddings_fixture(embeddings_dir, "tt0001")

    mock_qdrant = mocker.MagicMock()
    mock_qdrant.collection = "watch_history"
    mock_qdrant.point_count.return_value = 1
    mock_duckdb = _mock_duckdb(
        mocker, [_watch_history_row("tt0001", title="My Film", days_ago=1)]
    )

    watch_history_qdrant_collection(dg.build_asset_context(), mock_qdrant, mock_duckdb)

    (points,), _ = mock_qdrant.upsert_points.call_args
    metadata = points[0][3]
    assert metadata["imdb_id"] == "tt0001"
    assert metadata["title"] == "My Film"
    assert metadata["year"] == 2020
    assert metadata["imdb_rating"] == 7.5
    assert metadata["genres"] == "Drama"
    assert "last_viewed_at" in metadata
    assert "embedding_type" not in metadata
    assert "section" not in metadata


def test_point_ids_are_stable_across_rebuilds(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    import plex_ingest.defs.assets.watch_history_qdrant_collection as module

    mocker.patch.object(module, "PLEX_INGEST_DATA_DIR", str(tmp_path))
    embeddings_dir = tmp_path / "embeddings" / "watch_history"
    _write_embeddings_fixture(embeddings_dir, "tt0001")

    mock_qdrant = mocker.MagicMock()
    mock_qdrant.collection = "watch_history"
    mock_qdrant.point_count.return_value = 1
    mock_duckdb = _mock_duckdb(mocker, [_watch_history_row("tt0001", days_ago=1)])

    watch_history_qdrant_collection(dg.build_asset_context(), mock_qdrant, mock_duckdb)
    (first_points,), _ = mock_qdrant.upsert_points.call_args
    first_id = first_points[0][0]

    mock_qdrant.reset_mock()
    mock_qdrant.point_count.return_value = 1
    watch_history_qdrant_collection(dg.build_asset_context(), mock_qdrant, mock_duckdb)
    (second_points,), _ = mock_qdrant.upsert_points.call_args
    second_id = second_points[0][0]

    assert first_id == second_id
