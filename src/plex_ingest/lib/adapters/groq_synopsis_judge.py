import re
import time
from typing import Any

import groq
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import Runnable
from langchain_groq import ChatGroq

from plex_ingest.lib.ports import SynopsisMatchResult

# Only a leading excerpt is sent, not the full synopsis -- enough for a cheap judge
# model to place the plot in the right film (setting, characters, central conflict)
# without paying for the whole text on every partition. Backs
# `defs/checks/synopsis_match.py`, a data-quality check, not the ingestion path
# itself.
SYNOPSIS_EXCERPT_CHARS = 700

# Groq's free-tier qwen/qwen3-32b limits (RPM 60 / RPD 1000 / TPM 6000 / TPD 500000)
# are generous relative to catalog size, so unlike Gemini's enrichment path
# (gemini_enrichment.py's KNOWN_RPM_LIMIT/DailyQuotaExhaustedError split) there's no
# need to distinguish a per-minute burst from a genuine daily cap here -- a bounded
# retry that eventually gives up is enough; recalibrate if RPD exhaustion actually
# starts happening in practice.
BASE_RETRY_DELAY = 5
MAX_RETRY_DELAY = 30
MAX_ATTEMPTS = 4

_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            (
                "You are a strict fact-checker verifying that a plot synopsis "
                "actually describes a specific film, not a different one. Respond "
                "with exactly one line in the form 'MATCH: <reason>' or "
                "'MISMATCH: <reason>', where <reason> is a single short sentence.\n"
                "\n"
                "Answer MISMATCH only if the synopsis clearly describes a different "
                "film entirely -- a different entry in a franchise, an unrelated "
                "film, a remake/adaptation with a different plot, or text that isn't "
                "a plot summary at all (boilerplate, an error page, unrelated "
                "content). Do not answer MISMATCH just because the synopsis is "
                "brief, vague, or incomplete -- only because it describes the wrong "
                "film."
            ),
        ),
        ("human", "Film: {title} ({year})\nSynopsis excerpt: {excerpt}"),
    ]
)

_VERDICT_RE = re.compile(r"^(MATCH|MISMATCH)\b[:\-]?\s*(.*)", re.IGNORECASE | re.DOTALL)


def _parse_verdict(response: str, title: str, year: int) -> SynopsisMatchResult:
    match = _VERDICT_RE.match(response.strip())
    if match is None:
        msg = (
            f"Could not parse a MATCH/MISMATCH verdict from the judge model for "
            f"{title!r} ({year}): {response!r}"
        )
        raise ValueError(msg)
    verdict, reason = match.group(1).upper(), match.group(2).strip()
    return SynopsisMatchResult(matches=verdict == "MATCH", reason=reason or verdict)


class GroqSynopsisJudge:
    """Implements the `SynopsisMatchJudge` port (see `lib/ports.py`)."""

    def __init__(
        self, model: str = "qwen/qwen3-32b", temperature: float = 0, timeout: int = 30
    ) -> None:
        self.model = model
        self.temperature = temperature
        self.timeout = timeout

    def _chain(self) -> Runnable[dict[str, Any], str]:
        llm = ChatGroq(
            model=self.model,
            temperature=self.temperature,
            timeout=self.timeout,
            # qwen3-32b is a reasoning model that otherwise prepends a
            # <think>...</think> block before the verdict -- "hidden" drops it
            # server-side so the response is just the MATCH/MISMATCH line
            # _parse_verdict expects, and saves the (billed) reasoning tokens besides.
            reasoning_format="hidden",
        )
        return _PROMPT | llm | StrOutputParser()

    def check(self, *, title: str, year: int, synopsis: str) -> SynopsisMatchResult:
        excerpt = synopsis.strip()[:SYNOPSIS_EXCERPT_CHARS]
        chain = self._chain()
        delay = BASE_RETRY_DELAY
        attempt = 0
        while True:
            attempt += 1
            try:
                response: str = chain.invoke(
                    {"title": title, "year": year, "excerpt": excerpt}
                )
                return _parse_verdict(response, title, year)
            except groq.RateLimitError as e:
                if attempt >= MAX_ATTEMPTS:
                    msg = (
                        f"Groq rate limit exceeded after {MAX_ATTEMPTS} attempts "
                        f"judging {title!r} ({year})"
                    )
                    raise RuntimeError(msg) from e
                retry_after = e.response.headers.get("retry-after")
                time.sleep(float(retry_after) if retry_after else delay)
                delay = min(delay * 2, MAX_RETRY_DELAY)
