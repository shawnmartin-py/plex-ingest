import json
import os
from pathlib import Path
from typing import Any

import dagster as dg

# One JSON file per partition key (imdb_id), not DuckDB — DuckDB is single-writer and
# these assets are meant to run with real partition concurrency. See
# docs/pipeline-design.md ("Intermediate/temp storage") for the full reasoning.
PLEX_INGEST_DATA_DIR = os.environ.get("PLEX_INGEST_DATA_DIR", "data")


class JsonPartitionIOManager(dg.ConfigurableIOManager):
    base_dir: str

    def path_for(self, partition_key: str) -> Path:
        """Sole owner of the on-disk layout for a partition's file — anything that
        needs to locate or remove a partition's file (e.g. the partition-sync sensor)
        should go through this rather than re-deriving the path itself."""
        return Path(self.base_dir) / f"{partition_key}.json"

    def _path(self, partition_key: str | None) -> Path:
        if partition_key is None:
            msg = "JsonPartitionIOManager requires a partitioned asset"
            raise ValueError(msg)
        return self.path_for(partition_key)

    def handle_output(self, context: dg.OutputContext, obj: Any) -> None:
        path = self._path(context.partition_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(obj))

    def load_input(self, context: dg.InputContext) -> Any:
        return json.loads(self._path(context.partition_key).read_text())


SYNOPSIS_IO_MANAGER = JsonPartitionIOManager(
    base_dir=f"{PLEX_INGEST_DATA_DIR}/synopsis"
)
ENRICHMENT_IO_MANAGER = JsonPartitionIOManager(
    base_dir=f"{PLEX_INGEST_DATA_DIR}/enrichment"
)
EMBEDDINGS_IO_MANAGER = JsonPartitionIOManager(
    base_dir=f"{PLEX_INGEST_DATA_DIR}/embeddings"
)
WATCH_HISTORY_EMBEDDINGS_IO_MANAGER = JsonPartitionIOManager(
    base_dir=f"{PLEX_INGEST_DATA_DIR}/embeddings/watch_history"
)

defs = dg.Definitions(
    resources={
        "synopsis_io_manager": SYNOPSIS_IO_MANAGER,
        "enrichment_io_manager": ENRICHMENT_IO_MANAGER,
        "embeddings_io_manager": EMBEDDINGS_IO_MANAGER,
        "watch_history_embeddings_io_manager": WATCH_HISTORY_EMBEDDINGS_IO_MANAGER,
    }
)
