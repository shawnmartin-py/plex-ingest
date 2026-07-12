"""Proves the fix in qdrant_collection.py (_WAIT_FOR_PIPELINE_TO_SETTLE): the asset
must not fire a rebuild while synopsis or enrichment -- two hops upstream, not a
*direct* dep -- are still materializing for any imdb_id partition, even though the
triggering event (an embeddings update) has already happened.

Before the fix, `qdrant_collection` had `deps=["embeddings"]` and plain `eager()`.
`eager()`'s own `~any_deps_in_progress()` guard only inspects an asset's *direct* deps,
so a synopsis (or enrichment) run in flight on some unrelated partition was invisible
to it -- during a sync_imdb_id_partitions-driven backfill spanning many imdb_id
partitions at different pipeline stages, that meant a full Qdrant rebuild after every
single embeddings partition, each one stale within moments as more partitions landed.
`test_blocked_while_synopsis_in_progress_elsewhere` reproduces exactly that shape and
would fail against the old deps=["embeddings"] + eager() configuration.
"""

import threading

import dagster as dg
from dagster_duckdb import DuckDBResource

from plex_ingest.defs.assets.qdrant_collection import qdrant_collection
from plex_ingest.defs.partitions import imdb_id_partitions
from plex_ingest.defs.resources.qdrant import QdrantResource

_PARTITIONS_DEF_NAME = "imdb_id"

# qdrant_collection is never actually materialized in these tests (only its
# automation condition is evaluated), but Definitions still needs to resolve its
# resource requirements to build the repository -- these are never called.
_UNUSED_RESOURCES = {
    "qdrant": QdrantResource(url="http://example.invalid", collection="unused"),
    "duckdb": DuckDBResource(database=":memory:"),
}


def _stand_in_upstream_assets(
    synopsis_started: threading.Event, release_synopsis: threading.Event
) -> tuple[dg.AssetsDefinition, dg.AssetsDefinition, dg.AssetsDefinition]:
    """Minimal stand-ins for the real synopsis/enrichment/embeddings assets, keyed
    identically so they satisfy qdrant_collection's `deps=` without needing the real
    scraper/LLM/embeddings resources. `synopsis`'s compute signals `synopsis_started`
    and then blocks on `release_synopsis`, letting a test hold its run in the STARTED
    state for as long as it needs to observe in-progress behavior."""

    @dg.asset(key="synopsis", partitions_def=imdb_id_partitions)
    def synopsis() -> str:
        synopsis_started.set()
        release_synopsis.wait(timeout=5)
        return "synopsis text"

    @dg.asset(key="enrichment", partitions_def=imdb_id_partitions)
    def enrichment() -> dict[str, str]:
        return {"craft": "craft text"}

    @dg.asset(key="embeddings", partitions_def=imdb_id_partitions)
    def embeddings() -> dict[str, object]:
        return {"synopsis": {"text": "synopsis text", "vector": [0.0]}}

    return synopsis, enrichment, embeddings


def test_blocked_while_synopsis_in_progress_elsewhere() -> None:
    synopsis_started = threading.Event()
    release_synopsis = threading.Event()
    synopsis, enrichment, embeddings = _stand_in_upstream_assets(
        synopsis_started, release_synopsis
    )
    defs = dg.Definitions(
        assets=[synopsis, enrichment, embeddings, qdrant_collection],
        resources=_UNUSED_RESOURCES,
    )
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, ["tt_ready"])

    # A baseline tick before anything happens, purely to get past evaluation_id 0 --
    # an event and initial_evaluation() landing on that exact same first tick resolve
    # in favor of initial_evaluation() and the event is lost (see
    # test_automation_condition_cold_start.py). Without this, the embeddings update
    # below would never be seen as a trigger at all, independent of the in-progress
    # behavior this test is actually about.
    tick0 = dg.evaluate_automation_conditions(defs=defs, instance=instance)

    # The triggering event: embeddings updates for tt_ready.
    embeddings_result = dg.materialize(
        [embeddings], instance=instance, partition_key="tt_ready"
    )
    assert embeddings_result.success

    # A synopsis run (e.g. a re-backfill) is kicked off for the same imdb_id and held
    # mid-flight -- simulating the sensor still driving this partition through an
    # earlier pipeline stage even though its embeddings are already fresh.
    thread = threading.Thread(
        target=dg.materialize,
        kwargs={
            "assets": [synopsis],
            "instance": instance,
            "partition_key": "tt_ready",
        },
    )
    thread.start()
    assert synopsis_started.wait(timeout=5), "synopsis run never started"

    try:
        result = dg.evaluate_automation_conditions(
            defs=defs, instance=instance, cursor=tick0.cursor
        )
        assert result.get_num_requested(dg.AssetKey("qdrant_collection")) == 0
    finally:
        release_synopsis.set()
        thread.join(timeout=5)
        assert not thread.is_alive()

    # synopsis's run has now finished, so nothing upstream is in progress any more --
    # the still-unhandled embeddings update from earlier gets qdrant_collection
    # requested on the very next tick.
    result = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor
    )
    assert result.get_num_requested(dg.AssetKey("qdrant_collection")) == 1


def test_requested_once_pipeline_has_settled() -> None:
    """Control case: with no in-progress runs anywhere, an embeddings update alone is
    enough to request qdrant_collection -- confirms the widened condition still
    reduces to ordinary eager() behavior once the pipeline is quiet, i.e. this isn't
    accidentally gated shut altogether."""
    synopsis, enrichment, embeddings = _stand_in_upstream_assets(
        threading.Event(), threading.Event()
    )
    defs = dg.Definitions(
        assets=[synopsis, enrichment, embeddings, qdrant_collection],
        resources=_UNUSED_RESOURCES,
    )
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, ["tt_ready"])

    result = dg.evaluate_automation_conditions(defs=defs, instance=instance)
    assert result.get_num_requested(dg.AssetKey("qdrant_collection")) == 0

    embeddings_result = dg.materialize(
        [embeddings], instance=instance, partition_key="tt_ready"
    )
    assert embeddings_result.success

    result = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor
    )
    assert result.get_num_requested(dg.AssetKey("qdrant_collection")) == 1
