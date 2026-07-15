import json
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import dagster as dg
import pytest
from pytest_mock import MockerFixture

from plex_ingest.defs.assets.qdrant_collection import qdrant_collection

# Matches stg_movies_reader._COLUMNS order: tmdb_id, imdb_id, title, year, genres,
# imdb_rating, content_rating, description, thumb_url, video_resolution,
# hdr_formats, source_platform, runtime_minutes.
CatalogRow = tuple[
    str,
    str,
    str,
    int,
    list[str],
    float,
    str,
    str | None,
    str | None,
    str | None,
    list[str],
    str | None,
    int | None,
]


def _catalog_row(
    tmdb_id: str,
    imdb_id: str = "tt0001",
    title: str = "Test Film",
    video_resolution: str | None = None,
    hdr_formats: list[str] | None = None,
    source_platform: str | None = None,
    runtime_minutes: int | None = 104,
) -> CatalogRow:
    return (
        tmdb_id,
        imdb_id,
        title,
        2020,
        ["Drama"],
        7.5,
        "PG-13",
        "A great film.",
        "http://example.com/thumb.jpg",
        video_resolution,
        hdr_formats if hdr_formats is not None else [],
        source_platform,
        runtime_minutes,
    )


def _mock_duckdb(mocker: MockerFixture, rows: list[CatalogRow]) -> MagicMock:
    mock_duckdb = cast(MagicMock, mocker.MagicMock())
    mock_conn = mock_duckdb.get_connection.return_value.__enter__.return_value
    mock_conn.execute.return_value.fetchall.return_value = rows
    return mock_duckdb


def _write_embeddings_fixture(
    embeddings_dir: Path, tmdb_id: str, keys: dict[str, list[float]]
) -> None:
    embeddings_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        key: {"text": f"{key} text for {tmdb_id}", "vector": vector}
        for key, vector in keys.items()
    }
    (embeddings_dir / f"{tmdb_id}.json").write_text(json.dumps(payload))


def test_rebuilds_from_every_embeddings_file_on_disk(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    import plex_ingest.defs.assets.qdrant_collection as qdrant_collection_module

    mocker.patch.object(qdrant_collection_module, "PLEX_INGEST_DATA_DIR", str(tmp_path))
    embeddings_dir = tmp_path / "embeddings"
    _write_embeddings_fixture(
        embeddings_dir, "101", {"synopsis": [0.0], "craft": [0.1, 0.2]}
    )
    _write_embeddings_fixture(
        embeddings_dir, "102", {"synopsis": [0.0], "craft": [0.3], "meaning": [0.5]}
    )

    mock_qdrant = mocker.MagicMock()
    mock_qdrant.collection = "media_items"
    mock_qdrant.point_count.return_value = 5
    mock_duckdb = _mock_duckdb(mocker, [_catalog_row("101"), _catalog_row("102")])

    qdrant_collection(dg.build_asset_context(), mock_qdrant, mock_duckdb)

    mock_qdrant.recreate_collection.assert_called_once()
    (points,), _ = mock_qdrant.upsert_points.call_args
    assert len(points) == 5


def test_synopsis_point_has_synopsis_embedding_type_and_no_section(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    import plex_ingest.defs.assets.qdrant_collection as qdrant_collection_module

    mocker.patch.object(qdrant_collection_module, "PLEX_INGEST_DATA_DIR", str(tmp_path))
    embeddings_dir = tmp_path / "embeddings"
    _write_embeddings_fixture(
        embeddings_dir, "101", {"synopsis": [0.0], "craft": [0.1]}
    )

    mock_qdrant = mocker.MagicMock()
    mock_qdrant.collection = "media_items"
    mock_qdrant.point_count.return_value = 2
    mock_duckdb = _mock_duckdb(mocker, [_catalog_row("101")])

    qdrant_collection(dg.build_asset_context(), mock_qdrant, mock_duckdb)

    (points,), _ = mock_qdrant.upsert_points.call_args
    metadatas = {p[3]["embedding_type"]: p[3] for p in points}
    assert metadatas["synopsis"]["embedding_type"] == "synopsis"
    assert "section" not in metadatas["synopsis"]
    assert metadatas["enriched"]["embedding_type"] == "enriched"
    assert metadatas["enriched"]["section"] == "craft"


def test_points_carry_full_catalog_metadata(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    import plex_ingest.defs.assets.qdrant_collection as qdrant_collection_module

    mocker.patch.object(qdrant_collection_module, "PLEX_INGEST_DATA_DIR", str(tmp_path))
    embeddings_dir = tmp_path / "embeddings"
    _write_embeddings_fixture(embeddings_dir, "101", {"synopsis": [0.0]})

    mock_qdrant = mocker.MagicMock()
    mock_qdrant.collection = "media_items"
    mock_qdrant.point_count.return_value = 1
    mock_duckdb = _mock_duckdb(mocker, [_catalog_row("101", title="My Film")])

    qdrant_collection(dg.build_asset_context(), mock_qdrant, mock_duckdb)

    (points,), _ = mock_qdrant.upsert_points.call_args
    metadata = points[0][3]
    assert metadata["tmdb_id"] == "101"
    assert metadata["imdb_id"] == "tt0001"
    assert metadata["type"] == "movie"
    assert metadata["title"] == "My Film"
    assert metadata["year"] == 2020
    assert metadata["imdb_rating"] == 7.5
    assert metadata["content_rating"] == "PG-13"
    assert metadata["description"] == "A great film."
    assert metadata["genres"] == "Drama"
    assert metadata["thumb_url"] == "http://example.com/thumb.jpg"
    assert metadata["video_resolution"] is None
    assert metadata["hdr_formats"] == []
    assert metadata["source_platform"] is None
    assert metadata["runtime_minutes"] == 104


def test_points_carry_null_runtime_for_an_unresolved_streaming_placeholder(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    import plex_ingest.defs.assets.qdrant_collection as qdrant_collection_module

    mocker.patch.object(qdrant_collection_module, "PLEX_INGEST_DATA_DIR", str(tmp_path))
    embeddings_dir = tmp_path / "embeddings"
    _write_embeddings_fixture(embeddings_dir, "101", {"synopsis": [0.0]})

    mock_qdrant = mocker.MagicMock()
    mock_qdrant.collection = "media_items"
    mock_qdrant.point_count.return_value = 1
    mock_duckdb = _mock_duckdb(
        mocker,
        [_catalog_row("101", source_platform="Netflix", runtime_minutes=None)],
    )

    qdrant_collection(dg.build_asset_context(), mock_qdrant, mock_duckdb)

    (points,), _ = mock_qdrant.upsert_points.call_args
    metadata = points[0][3]
    assert metadata["source_platform"] == "Netflix"
    assert metadata["runtime_minutes"] is None


def test_points_carry_video_resolution_for_a_real_download(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    import plex_ingest.defs.assets.qdrant_collection as qdrant_collection_module

    mocker.patch.object(qdrant_collection_module, "PLEX_INGEST_DATA_DIR", str(tmp_path))
    embeddings_dir = tmp_path / "embeddings"
    _write_embeddings_fixture(embeddings_dir, "101", {"synopsis": [0.0]})

    mock_qdrant = mocker.MagicMock()
    mock_qdrant.collection = "media_items"
    mock_qdrant.point_count.return_value = 1
    mock_duckdb = _mock_duckdb(mocker, [_catalog_row("101", video_resolution="4k")])

    qdrant_collection(dg.build_asset_context(), mock_qdrant, mock_duckdb)

    (points,), _ = mock_qdrant.upsert_points.call_args
    metadata = points[0][3]
    assert metadata["video_resolution"] == "4k"
    assert metadata["source_platform"] is None


def test_points_carry_hdr_formats_for_a_real_download(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    import plex_ingest.defs.assets.qdrant_collection as qdrant_collection_module

    mocker.patch.object(qdrant_collection_module, "PLEX_INGEST_DATA_DIR", str(tmp_path))
    embeddings_dir = tmp_path / "embeddings"
    _write_embeddings_fixture(embeddings_dir, "101", {"synopsis": [0.0]})

    mock_qdrant = mocker.MagicMock()
    mock_qdrant.collection = "media_items"
    mock_qdrant.point_count.return_value = 1
    mock_duckdb = _mock_duckdb(mocker, [_catalog_row("101", hdr_formats=["HDR"])])

    qdrant_collection(dg.build_asset_context(), mock_qdrant, mock_duckdb)

    (points,), _ = mock_qdrant.upsert_points.call_args
    metadata = points[0][3]
    assert metadata["hdr_formats"] == ["HDR"]


def test_points_carry_both_hdr_and_dv_when_both_present(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    import plex_ingest.defs.assets.qdrant_collection as qdrant_collection_module

    mocker.patch.object(qdrant_collection_module, "PLEX_INGEST_DATA_DIR", str(tmp_path))
    embeddings_dir = tmp_path / "embeddings"
    _write_embeddings_fixture(embeddings_dir, "101", {"synopsis": [0.0]})

    mock_qdrant = mocker.MagicMock()
    mock_qdrant.collection = "media_items"
    mock_qdrant.point_count.return_value = 1
    mock_duckdb = _mock_duckdb(mocker, [_catalog_row("101", hdr_formats=["HDR", "DV"])])

    qdrant_collection(dg.build_asset_context(), mock_qdrant, mock_duckdb)

    (points,), _ = mock_qdrant.upsert_points.call_args
    metadata = points[0][3]
    assert metadata["hdr_formats"] == ["HDR", "DV"]


def test_unrecognized_hdr_format_raises(tmp_path: Path, mocker: MockerFixture) -> None:
    import plex_ingest.defs.assets.qdrant_collection as qdrant_collection_module

    mocker.patch.object(qdrant_collection_module, "PLEX_INGEST_DATA_DIR", str(tmp_path))
    embeddings_dir = tmp_path / "embeddings"
    _write_embeddings_fixture(embeddings_dir, "101", {"synopsis": [0.0]})

    mock_qdrant = mocker.MagicMock()
    mock_duckdb = _mock_duckdb(mocker, [_catalog_row("101", hdr_formats=["HDR10+"])])

    with pytest.raises(ValueError, match="101"):
        qdrant_collection(dg.build_asset_context(), mock_qdrant, mock_duckdb)


def test_points_carry_source_platform_for_a_streaming_placeholder(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    import plex_ingest.defs.assets.qdrant_collection as qdrant_collection_module

    mocker.patch.object(qdrant_collection_module, "PLEX_INGEST_DATA_DIR", str(tmp_path))
    embeddings_dir = tmp_path / "embeddings"
    _write_embeddings_fixture(embeddings_dir, "101", {"synopsis": [0.0]})

    mock_qdrant = mocker.MagicMock()
    mock_qdrant.collection = "media_items"
    mock_qdrant.point_count.return_value = 1
    mock_duckdb = _mock_duckdb(mocker, [_catalog_row("101", source_platform="Netflix")])

    qdrant_collection(dg.build_asset_context(), mock_qdrant, mock_duckdb)

    (points,), _ = mock_qdrant.upsert_points.call_args
    metadata = points[0][3]
    assert metadata["source_platform"] == "Netflix"
    assert metadata["video_resolution"] is None


def test_unrecognized_video_resolution_raises(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    import plex_ingest.defs.assets.qdrant_collection as qdrant_collection_module

    mocker.patch.object(qdrant_collection_module, "PLEX_INGEST_DATA_DIR", str(tmp_path))
    embeddings_dir = tmp_path / "embeddings"
    _write_embeddings_fixture(embeddings_dir, "101", {"synopsis": [0.0]})

    mock_qdrant = mocker.MagicMock()
    mock_duckdb = _mock_duckdb(mocker, [_catalog_row("101", video_resolution="8k")])

    with pytest.raises(ValueError, match="101"):
        qdrant_collection(dg.build_asset_context(), mock_qdrant, mock_duckdb)


def test_raises_when_embeddings_file_has_no_matching_catalog_row(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    import plex_ingest.defs.assets.qdrant_collection as qdrant_collection_module

    mocker.patch.object(qdrant_collection_module, "PLEX_INGEST_DATA_DIR", str(tmp_path))
    embeddings_dir = tmp_path / "embeddings"
    _write_embeddings_fixture(embeddings_dir, "101", {"synopsis": [0.0]})

    mock_qdrant = mocker.MagicMock()
    mock_duckdb = _mock_duckdb(mocker, [])  # no catalog rows at all

    with pytest.raises(ValueError, match="101"):
        qdrant_collection(dg.build_asset_context(), mock_qdrant, mock_duckdb)


def test_deleted_movie_is_absent_from_the_rebuild(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    import plex_ingest.defs.assets.qdrant_collection as qdrant_collection_module

    mocker.patch.object(qdrant_collection_module, "PLEX_INGEST_DATA_DIR", str(tmp_path))
    embeddings_dir = tmp_path / "embeddings"
    _write_embeddings_fixture(embeddings_dir, "101", {"synopsis": [0.0]})

    mock_qdrant = mocker.MagicMock()
    mock_qdrant.collection = "media_items"
    mock_qdrant.point_count.return_value = 1
    mock_duckdb = _mock_duckdb(mocker, [_catalog_row("101")])
    qdrant_collection(dg.build_asset_context(), mock_qdrant, mock_duckdb)
    (first_points,), _ = mock_qdrant.upsert_points.call_args
    assert len(first_points) == 1

    # 101 is "removed" by deleting its embeddings file, same as the sync sensor does.
    (embeddings_dir / "101.json").unlink()
    mock_qdrant.reset_mock()
    mock_qdrant.point_count.return_value = 0

    qdrant_collection(dg.build_asset_context(), mock_qdrant, mock_duckdb)

    mock_qdrant.upsert_points.assert_not_called()


def test_point_ids_are_stable_across_rebuilds(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    import plex_ingest.defs.assets.qdrant_collection as qdrant_collection_module

    mocker.patch.object(qdrant_collection_module, "PLEX_INGEST_DATA_DIR", str(tmp_path))
    embeddings_dir = tmp_path / "embeddings"
    _write_embeddings_fixture(embeddings_dir, "101", {"synopsis": [0.0]})

    mock_qdrant = mocker.MagicMock()
    mock_qdrant.collection = "media_items"
    mock_qdrant.point_count.return_value = 1
    mock_duckdb = _mock_duckdb(mocker, [_catalog_row("101")])

    qdrant_collection(dg.build_asset_context(), mock_qdrant, mock_duckdb)
    (first_points,), _ = mock_qdrant.upsert_points.call_args
    first_id = first_points[0][0]

    mock_qdrant.reset_mock()
    mock_qdrant.point_count.return_value = 1
    qdrant_collection(dg.build_asset_context(), mock_qdrant, mock_duckdb)
    (second_points,), _ = mock_qdrant.upsert_points.call_args
    second_id = second_points[0][0]

    assert first_id == second_id
