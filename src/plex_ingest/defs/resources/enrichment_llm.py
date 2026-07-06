import dagster as dg

from plex_ingest.lib.adapters.gemini_enrichment import GeminiEnrichmentGenerator
from plex_ingest.lib.ports import EnrichmentGenerator


class EnrichmentLLMResource(dg.ConfigurableResource):
    """Config + adapter factory only — prompts, retry/backoff, and the Gemini chain
    live in lib/adapters/gemini_enrichment.py, behind the EnrichmentGenerator port.
    Keeping this thin is what lets a future LangChain/LlamaIndex framework swap (see
    phase-2-pipeline-design.md in plex-rag) land as a new adapter, not a rewrite of
    this resource or the enrichment asset."""

    model: str = "gemini-3.1-flash-lite"
    temperature: float = 0
    timeout: int = 60

    def _adapter(self) -> EnrichmentGenerator:
        return GeminiEnrichmentGenerator(
            model=self.model, temperature=self.temperature, timeout=self.timeout
        )

    @property
    def sections(self) -> tuple[str, ...]:
        return self._adapter().sections

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
        return self._adapter().generate_section(
            title=title,
            year=year,
            genres=genres,
            imdb_rating=imdb_rating,
            content_rating=content_rating,
            synopsis=synopsis,
            section=section,
        )


defs = dg.Definitions(resources={"enrichment_llm": EnrichmentLLMResource()})
