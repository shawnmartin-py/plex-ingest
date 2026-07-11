import dagster as dg

from plex_ingest.lib.adapters.groq_synopsis_judge import GroqSynopsisJudge
from plex_ingest.lib.ports import SynopsisMatchJudge, SynopsisMatchResult


class SynopsisJudgeResource(dg.ConfigurableResource):
    """Config + adapter factory only — the Groq call, prompt, and retry/backoff live
    in lib/adapters/groq_synopsis_judge.py, behind the SynopsisMatchJudge port, so the
    judge provider/model can be swapped without touching this resource or the
    `synopsis_matches_movie` check that uses it (see `defs/checks/synopsis_match.py`).
    Reads its API key from the `GROQ_API_KEY` env var via ChatGroq's own default,
    same convention as EnrichmentLLMResource/GOOGLE_API_KEY."""

    model: str = "qwen/qwen3-32b"
    temperature: float = 0
    timeout: int = 30

    def _adapter(self) -> SynopsisMatchJudge:
        return GroqSynopsisJudge(
            model=self.model, temperature=self.temperature, timeout=self.timeout
        )

    def check(self, *, title: str, year: int, synopsis: str) -> SynopsisMatchResult:
        return self._adapter().check(title=title, year=year, synopsis=synopsis)


defs = dg.Definitions(resources={"synopsis_judge": SynopsisJudgeResource()})
