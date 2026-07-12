import dagster as dg

# Shared by synopsis/enrichment/embeddings so a single delete_dynamic_partition
# request (issued by the partition-sync sensor) removes an imdb_id from all three
# at once. See docs/pipeline-design.md for why these three stages are
# partitioned and qdrant_collection isn't.
imdb_id_partitions = dg.DynamicPartitionsDefinition(name="imdb_id")

# Separate namespace from imdb_id_partitions, not shared: this partition set is
# add-only (see sync_watch_history_partitions) and has its own lifecycle, unrelated
# to the unwatched-catalog pipeline's. See docs/pipeline-design.md's "Watch-history
# diversity-recommender pipeline".
watch_history_imdb_id_partitions = dg.DynamicPartitionsDefinition(
    name="watch_history_imdb_id"
)
