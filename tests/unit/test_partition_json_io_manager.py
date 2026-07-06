from pathlib import Path

import dagster as dg

from plex_ingest.defs.resources.partition_json_io_manager import JsonPartitionIOManager


def test_handle_output_writes_one_file_per_partition(tmp_path: Path) -> None:
    manager = JsonPartitionIOManager(base_dir=str(tmp_path))
    context = dg.build_output_context(partition_key="tt0242888")

    manager.handle_output(context, {"craft": "text"})

    assert (tmp_path / "tt0242888.json").read_text() == '{"craft": "text"}'


def test_load_input_reads_back_what_was_written(tmp_path: Path) -> None:
    manager = JsonPartitionIOManager(base_dir=str(tmp_path))
    output_context = dg.build_output_context(partition_key="tt0242888")
    manager.handle_output(output_context, {"craft": "text"})

    input_context = dg.build_input_context(partition_key="tt0242888")
    assert manager.load_input(input_context) == {"craft": "text"}


def test_handle_output_creates_base_dir_if_missing(tmp_path: Path) -> None:
    manager = JsonPartitionIOManager(base_dir=str(tmp_path / "nested" / "dir"))
    context = dg.build_output_context(partition_key="tt0242888")

    manager.handle_output(context, "value")

    assert (tmp_path / "nested" / "dir" / "tt0242888.json").exists()


def test_different_partitions_get_different_files(tmp_path: Path) -> None:
    manager = JsonPartitionIOManager(base_dir=str(tmp_path))
    manager.handle_output(dg.build_output_context(partition_key="tt0001"), "a")
    manager.handle_output(dg.build_output_context(partition_key="tt0002"), "b")

    assert manager.load_input(dg.build_input_context(partition_key="tt0001")) == "a"
    assert manager.load_input(dg.build_input_context(partition_key="tt0002")) == "b"
