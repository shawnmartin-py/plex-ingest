"""Investigates "bug #2" from the dev-subset verification session
(docs/pipeline-design.md's "Known gaps... (2026-07-05)", item 2): the claim that
`on_missing()` has a cold-start blind spot where a partition already missing at some
critical moment gets permanently stuck.

CONFIRMED, and more precise/severe than originally described. The mechanism is not
about `dg dev` restarts losing the automation cursor (`start_sensor`/`stop_sensor`
preserve the cursor whenever a stored sensor state already exists -- see below) -- it's
about `evaluation_id == 0`, the literal very first evaluation of a freshly created
automation-condition cursor:

`on_missing()`'s (and `eager()`'s) expansion wraps a transient event in `since(...)`,
where the reset condition is
`newly_requested() | newly_updated() | initial_evaluation()`.
`initial_evaluation()` is true only on evaluation_id 0. When a partition is *already*
missing at that exact first evaluation, its `missing().newly_true()` event and the
`initial_evaluation()` reset condition both become true on the *same* tick -- and this
installed Dagster version (1.13.12) resolves that tie in favor of the reset, so `SINCE`
evaluates false. Because `newly_true()` only fires once (on the transition to missing),
and this partition never un-misses and re-misses again, it never gets another chance:
the condition stays false forever, confirmed here across many subsequent ticks with the
cursor correctly threaded through (see
`test_partition_missing_at_evaluation_id_zero_never_requested`).
This reproduces even for the *exact* example in the public
`dagster.evaluate_automation_conditions` docstring (an unpartitioned asset with
`eager()`), which claims `total_requested == 1` on tick 1 -- in this installed version
it's 0. Worth flagging upstream; not something this pipeline can work around by
switching conditions.

A partition that starts existing/missing *after* evaluation_id 0 (i.e. added once the
cursor already has any history) is unaffected -- confirmed in
`test_partition_added_after_first_tick_gets_requested_normally`. This matches every
"new movie added" scenario already verified live against a running `dg dev` daemon.

Practical consequence for this pipeline: bug #1 (sensors defaulting to `STOPPED`,
item 1 of the same "Known gaps" section) meant the automation-condition sensor's first
successful tick ever happened well after `stg_movies` already had rows in it -- so
every partition present at that moment was permanently missed, matching exactly what
the original verification session observed. Now that bug #1 is fixed
(`default_status=DefaultSensorStatus.RUNNING`), this only bites *once* per
`DAGSTER_HOME`'s lifetime: whatever's in `stg_movies` the very first time the
automation-condition sensor ever ticks successfully. Ordinary restarts afterward reuse
the already-nonzero cursor via `instance.all_instigator_state(...)`
(dagster/_daemon/sensor.py) and `DagsterInstance.start_sensor()`'s
`data.with_sensor_start_timestamp(...)` (dagster/_core/instance/__init__.py), both of
which preserve the existing cursor rather than resetting it -- so this is a first-deploy
concern, not a recurring one. Still needs an explicit initial backfill (or equivalent)
for whatever's present in `stg_movies` before the automation-condition sensor's first
tick, on any fresh `DAGSTER_HOME` (including a real production rollout), rather than
relying on `on_missing()` alone to catch it.

Only the scraper/LLM adapters are faked, with unique random text per call, in the
content-freshness tests below -- proving genuine re-execution rather than relying on
real IMDB/Wikipedia/Gemini calls (slow, rate-limited, and what made the manual
live-verification sessions flaky).
"""

import uuid
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import dagster as dg
from pytest_mock import MockerFixture

from plex_ingest.defs.assets.enrichment import enrichment as production_enrichment
from plex_ingest.defs.assets.synopsis import synopsis as production_synopsis
from plex_ingest.defs.partitions import imdb_id_partitions
from plex_ingest.defs.resources.enrichment_llm import EnrichmentLLMResource
from plex_ingest.defs.resources.partition_json_io_manager import JsonPartitionIOManager
from plex_ingest.defs.resources.scraper import ScraperResource

# Matches partitions.py's DynamicPartitionsDefinition(name="imdb_id") -- used as a
# literal here (not imdb_id_partitions.name, which types as str | None) since
# DynamicPartitionsDefinition.name is always set for this project's usage.
_PARTITIONS_DEF_NAME = "imdb_id"

# --- Part 1: the evaluation_id == 0 mechanism, via minimal local assets ---


def _on_missing_chain() -> tuple[dg.AssetsDefinition, dg.AssetsDefinition]:
    """Two dynamically-partitioned assets chained exactly like the real
    synopsis->enrichment relationship (`on_missing()` on both, downstream depends on
    upstream), without the real assets' unrelated `stg_movies` external dependency."""

    @dg.asset(
        key="synopsis",
        partitions_def=imdb_id_partitions,
        automation_condition=dg.AutomationCondition.on_missing(),
    )
    def _synopsis() -> str:
        return f"synopsis-{uuid.uuid4()}"

    @dg.asset(
        key="enrichment",
        partitions_def=imdb_id_partitions,
        automation_condition=dg.AutomationCondition.on_missing(),
    )
    def _enrichment(synopsis: str) -> str:
        return f"enrichment-{uuid.uuid4()}"

    return _synopsis, _enrichment


def test_partition_missing_at_evaluation_id_zero_never_requested() -> None:
    """A partition already missing at the automation condition cursor's very first
    evaluation (evaluation_id 0) never gets requested -- not on that tick, and not on
    any later tick even with the cursor correctly threaded through and its dependency
    resolved in between. This is the confirmed root cause behind bug #2."""
    imdb_id = "tt_stuck"
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, [imdb_id])
    synopsis, enrichment = _on_missing_chain()
    defs = dg.Definitions(assets=[synopsis, enrichment])

    tick = dg.evaluate_automation_conditions(defs=defs, instance=instance)
    assert tick.get_requested_partitions(dg.AssetKey("synopsis")) == set()

    # Materialize synopsis by hand (a real run launcher would do this independent of
    # the automation sensor loop) -- its dependency is no longer missing.
    synopsis_result = dg.materialize(
        [synopsis], instance=instance, partition_key=imdb_id
    )
    assert synopsis_result.success

    # enrichment's own missing-transition also happened at evaluation_id 0, so it stays
    # stuck too, across many further ticks with the cursor correctly threaded.
    for _ in range(5):
        tick = dg.evaluate_automation_conditions(
            defs=defs, instance=instance, cursor=tick.cursor
        )
        assert tick.get_requested_partitions(dg.AssetKey("enrichment")) == set()


def test_partition_added_after_first_tick_gets_requested_normally() -> None:
    """A partition that starts existing only once the cursor already has some history
    (evaluation_id > 0) is unaffected -- it gets requested the moment it's added and
    self-heals normally through its dependency chain. This is the ordinary, already
    live-verified case (a new movie added to a running pipeline)."""
    instance = dg.DagsterInstance.ephemeral()
    synopsis, enrichment = _on_missing_chain()
    defs = dg.Definitions(assets=[synopsis, enrichment])

    # Tick 0: no partitions registered yet at all. Just establishes evaluation_id > 0
    # for subsequent ticks.
    tick0 = dg.evaluate_automation_conditions(defs=defs, instance=instance)

    imdb_id = "tt_added_later"
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, [imdb_id])
    tick1 = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=tick0.cursor
    )
    assert tick1.get_requested_partitions(dg.AssetKey("synopsis")) == {imdb_id}
    # enrichment is requested on the *same* tick, not blocked: any_deps_missing() only
    # counts a dep as blocking if it *won't* be requested this tick (run grouping, see
    # any_deps_missing() == any_deps_match(missing() & ~will_be_requested())) -- since
    # synopsis will_be_requested() this tick, it doesn't count against enrichment.
    assert tick1.get_requested_partitions(dg.AssetKey("enrichment")) == {imdb_id}

    result = dg.materialize(
        [synopsis, enrichment], instance=instance, partition_key=imdb_id
    )
    assert result.success


# --- Part 2: real synopsis/enrichment assets produce genuinely fresh content ---

# Matches stg_movies_reader._COLUMNS order: imdb_id, title, year, genres,
# imdb_rating, content_rating, description, thumb_url, video_resolution,
# hdr_formats, source_platform.
CatalogRow = tuple[
    str,
    str,
    int,
    list[str],
    float,
    str | None,
    str | None,
    str | None,
    str | None,
    list[str],
    str | None,
]


def _mock_duckdb(mocker: MockerFixture, imdb_id: str) -> MagicMock:
    row: CatalogRow = (
        imdb_id,
        "Test Film",
        2020,
        ["Drama"],
        7.5,
        "PG-13",
        "A short description.",
        None,
        None,
        [],
        None,
    )
    mock_duckdb = cast(MagicMock, mocker.MagicMock())
    mock_conn = mock_duckdb.get_connection.return_value.__enter__.return_value
    mock_conn.execute.return_value.fetchone.return_value = row
    return mock_duckdb


class _RandomSynopsisScraper:
    """Fake for the `SynopsisScraper` port -- unique random text per call stands in for
    a real IMDB/Wikipedia scrape, so a re-materialization is provably fresh execution
    rather than stale/duplicated content."""

    def fetch_synopsis(self, imdb_id: str, title: str, year: int) -> str | None:
        return f"synopsis-{uuid.uuid4()}"


class _RandomEnrichmentGenerator:
    """Fake for the `EnrichmentGenerator` port -- same rationale as
    `_RandomSynopsisScraper`, standing in for real Gemini calls."""

    @property
    def sections(self) -> tuple[str, ...]:
        return ("craft", "meaning", "context")

    def generate_section(
        self,
        *,
        title: str,
        year: int,
        genres: list[str],
        imdb_rating: float | None,
        content_rating: str | None,
        synopsis: str,
        section: str,
    ) -> str | None:
        return f"{section}-{uuid.uuid4()}"


class _FakeScraperResource(ScraperResource):
    """Subclassing (not instance-monkeypatching `_adapter`) so the override survives
    Dagster's resource re-validation/copying when actually executing a job -- a plain
    instance attribute assignment doesn't carry through that step."""

    def _adapter(self) -> _RandomSynopsisScraper:
        return _RandomSynopsisScraper()


class _FakeEnrichmentLLMResource(EnrichmentLLMResource):
    def _adapter(self) -> _RandomEnrichmentGenerator:
        return _RandomEnrichmentGenerator()


def _production_resources(
    mocker: MockerFixture, imdb_id: str, tmp_path_str: str
) -> dict[str, object]:
    return {
        "scraper": _FakeScraperResource(),
        "enrichment_llm": _FakeEnrichmentLLMResource(),
        "duckdb": _mock_duckdb(mocker, imdb_id),
        "synopsis_io_manager": JsonPartitionIOManager(
            base_dir=f"{tmp_path_str}/synopsis"
        ),
        "enrichment_io_manager": JsonPartitionIOManager(
            base_dir=f"{tmp_path_str}/enrichment"
        ),
    }


def test_repeated_synopsis_materializations_produce_unique_content(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """A re-materialization after being wiped/re-triggered must produce genuinely new
    content, not silently reuse a previous run's output -- what the original live
    verification session checked for real via an injected marker string. Here, the fake
    adapter's randomized text lets the same assertion run fast and deterministically."""
    imdb_id = "tt_unique_synopsis"
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, [imdb_id])
    resources = _production_resources(mocker, imdb_id, str(tmp_path))

    first = dg.materialize(
        [production_synopsis],
        instance=instance,
        partition_key=imdb_id,
        resources=resources,
    )
    second = dg.materialize(
        [production_synopsis],
        instance=instance,
        partition_key=imdb_id,
        resources=resources,
    )

    assert first.output_for_node("synopsis") != second.output_for_node("synopsis")


def test_repeated_enrichment_materializations_produce_unique_content(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """Same as above, for `enrichment` -- covers the second real asset's compute path
    (dict of 3 sections) separately, since it's a distinct function from `synopsis`."""
    imdb_id = "tt_unique_enrichment"
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PARTITIONS_DEF_NAME, [imdb_id])
    resources = _production_resources(mocker, imdb_id, str(tmp_path))

    dg.materialize(
        [production_synopsis],
        instance=instance,
        partition_key=imdb_id,
        resources=resources,
    )
    first = dg.materialize(
        [production_synopsis, production_enrichment],
        instance=instance,
        partition_key=imdb_id,
        resources=resources,
        selection=[production_enrichment],
    )
    second = dg.materialize(
        [production_synopsis, production_enrichment],
        instance=instance,
        partition_key=imdb_id,
        resources=resources,
        selection=[production_enrichment],
    )

    assert first.output_for_node("enrichment") != second.output_for_node("enrichment")
