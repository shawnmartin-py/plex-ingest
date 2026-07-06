import dagster as dg

# Shared by synopsis/enrichment/embeddings so a single delete_dynamic_partition
# request (issued by the partition-sync sensor) removes an imdb_id from all three
# at once. See docs/epics/plex-ingest-extraction/phase-2-pipeline-design.md in
# plex-rag for why these three stages are partitioned and qdrant_collection isn't.
imdb_id_partitions = dg.DynamicPartitionsDefinition(name="imdb_id")
