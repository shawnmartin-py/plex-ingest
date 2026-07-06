import time
from typing import Any

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import Runnable
from langchain_google_genai import (
    ChatGoogleGenerativeAI,
    HarmBlockThreshold,
    HarmCategory,
)

# Retry/backoff constants and prompts ported verbatim from plex-rag's
# app/services/enrichment.py — the prompt text is carefully tuned for retrieval
# quality, not something to rephrase in the port.
BASE_RETRY_DELAY = 10
MAX_RETRY_DELAY = 120

SECTIONS = ("craft", "meaning", "context")

# Gemini's free-tier RESOURCE_EXHAUSTED errors always report
# quotaId="GenerateRequestsPerDayPerProjectPerModel-FreeTier" -- confirmed empirically
# (2026-07-06) even when the real trigger is a burst against the per-*minute* cap, not
# the day's budget: firing a concurrent burst well under any plausible daily limit
# still produced this "PerDay"-named quotaId, just with a small quotaValue matching the
# RPM figure rather than the (much larger) daily one. So quotaId text cannot
# distinguish "the day's quota is truly gone" from "brief per-minute throttling" --
# only the numeric quotaValue can, compared against the model's documented RPM
# ceiling (https://ai.google.dev/gemini-api/docs/rate-limits). Anything at/under that
# ceiling is a per-minute burst (transient, worth retrying); anything clearly above it
# is the real daily cap, which won't clear until Google's next reset, so retrying is
# pointless. Recalibrate this if EnrichmentLLMResource's configured model changes.
KNOWN_RPM_LIMIT = 15  # gemini-3.1-flash-lite, free tier


class DailyQuotaExhaustedError(RuntimeError):
    """Gemini's free-tier *daily* request quota is exhausted for the enrichment
    model -- distinct from a transient per-minute burst, which is retried instead.
    Deliberately not retried: every retry before the next daily reset is guaranteed
    to fail the same way."""


_SAFETY_OFF = {
    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
}

_HUMAN = (
    "human",
    (
        "Title: {title} ({year})\n"
        "Genres: {genres}\n"
        "IMDb Rating: {imdb_rating}\n"
        "Content Rating: {content_rating}\n"
        "Synopsis: {synopsis}"
    ),
)

_CRAFT_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            (
                "You are a film expert generating a focused profile of a film's craft "
                "and identity for use in a semantic recommendation system.\n"
                "\n"
                "Write in dense, continuous prose covering:\n"
                "- Exact subgenre positioning with precision — not 'thriller' but "
                "'paranoid Cold War conspiracy thriller' or 'slow-burn Scandinavian "
                "psychological horror'\n"
                "- The cinematic movement, tradition, or school it belongs to (French "
                "New Wave, Italian neorealism, New Hollywood, J-horror, Dogme 95, "
                "Ozploitation, etc.)\n"
                "- Country of origin and how it fits within that national cinema's "
                "history\n"
                "- The director's signature style, obsessions, and where this film "
                "sits in their career — debut, peak, late period, or departure\n"
                "- Which directors influenced them, and which directors they in turn "
                "influenced\n"
                "- If a known auteur, name their recurring thematic and visual "
                "preoccupations across their body of work\n"
                "- Visual grammar: camera movement (handheld, static, slow zoom), "
                "aspect ratio, depth of field, lighting philosophy (chiaroscuro, "
                "naturalistic, neon)\n"
                "- Color palette and what it communicates emotionally\n"
                "- Editing rhythm — fragmented and disorienting, languid and "
                "contemplative, or classically invisible\n"
                "- Score or soundtrack: composer, genre of music, how it functions "
                "emotionally\n"
                "- Cinematographer if notable; production design and costume as "
                "storytelling\n"
                "\n"
                "Be the expert recommender, not a Wikipedia editor. Every sentence "
                "should carry retrieval signal — specific names, subgenre labels, "
                "movement names, technique terms."
            ),
        ),
        _HUMAN,
    ]
)

_MEANING_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            (
                "You are a film expert generating a focused profile of a film's "
                "narrative, themes, and emotional experience for use in a semantic "
                "recommendation system.\n"
                "\n"
                "Write in dense, continuous prose covering:\n"
                "- How the story is told — linear or non-linear, unreliable narrator, "
                "multiple perspectives, found footage, epistolary, or other formal "
                "conceits\n"
                "- How much it withholds versus reveals, and where tension is "
                "generated: plot, character, atmosphere, or ideas\n"
                "- Core themes and recurring motifs — what questions it asks without "
                "necessarily answering\n"
                "- What the film is actually about beneath the surface plot: identity, "
                "mortality, power, grief, memory, capitalism, masculinity, "
                "colonialism, faith, etc.\n"
                "- Any literary, mythological, or philosophical traditions it draws "
                "from\n"
                "- Tone and emotional register with precision — numbing, exhilarating, "
                "suffocating, melancholy, darkly comedic, tender, alienating, or "
                "cathartic\n"
                "- Whether tone shifts across the film (comedy that turns brutal, "
                "horror that becomes tragic) and what the viewer carries out "
                "afterward\n"
                "- Acting style: naturalistic, theatrical, minimalist, Method, "
                "Brechtian — and how it serves the film\n"
                "- Ensemble dynamic, character archetypes or anti-archetypes present\n"
                "- How the film ends emotionally — cathartic, ambiguous, devastating, "
                "ironic — without revealing plot specifics\n"
                "\n"
                "Be the expert recommender, not a Wikipedia editor. Every sentence "
                "should carry retrieval signal — specific thematic keywords, tone "
                "descriptors, narrative terms."
            ),
        ),
        _HUMAN,
    ]
)

_CONTEXT_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            (
                "You are a film expert generating a focused profile of a film's "
                "cultural position, audience fit, and comparable films for use in a "
                "semantic recommendation system.\n"
                "\n"
                "Write in dense, continuous prose covering:\n"
                "- Why this film was made when it was — cultural anxieties, political "
                "events, or social movements it responds to, consciously or not\n"
                "- Initial critical reception versus retrospective reassessment — was "
                "it controversial, ahead of its time, or rediscovered later\n"
                "- Awards recognition, cult following, and cultural footprint — has it "
                "been remade, referenced, or parodied in ways that signal its reach\n"
                "- Who this film is for — be honest and specific about the viewer it "
                "rewards\n"
                "- What prior film experiences best prepare a viewer for it\n"
                "- Whether it demands patience or is immediately engaging; whether it "
                "improves on rewatch; whether it's best watched alone or with others\n"
                "- At least six films that share meaningful DNA, approached from "
                "multiple angles: same director's other work, same national cinema, "
                "same thematic obsession, same visual style, same emotional register, "
                "same cult audience. For each, name the specific axis of similarity — "
                "not just the title\n"
                "- What would surprise a first-time viewer who only knew the genre "
                "label\n"
                "- What makes this film unmistakable — the one thing it does that "
                "almost no other film does\n"
                "- End with a dense paragraph of retrieval-optimized descriptors: "
                "adjectives, genre micro-labels, thematic keywords, mood words, "
                "director names, actor names, cinematographer, composer, country, "
                "decade, movement names, and any other terms a knowledgeable person "
                "might use to find this film. Include synonyms and adjacent terms. "
                "This paragraph exists purely for search recall.\n"
                "\n"
                "Be the expert recommender, not a Wikipedia editor. Every sentence "
                "should carry retrieval signal."
            ),
        ),
        _HUMAN,
    ]
)

_PROMPTS = {
    "craft": _CRAFT_PROMPT,
    "meaning": _MEANING_PROMPT,
    "context": _CONTEXT_PROMPT,
}


def _quota_violation(exc: Exception) -> dict[str, Any] | None:
    """The QuotaFailure violation dict (quotaId/quotaValue/quotaDimensions/...) from a
    Gemini RESOURCE_EXHAUSTED error, read off the structured `.details` that
    `google.genai.errors.ClientError` (the `__cause__` langchain wraps) exposes --
    None if that's unavailable or doesn't contain one, in which case callers should
    fall back to the existing retry behavior rather than guessing."""
    cause = exc.__cause__
    details = getattr(cause, "details", None)
    if not isinstance(details, dict):
        return None
    for detail in details.get("error", {}).get("details", []):
        if detail.get("@type") == "type.googleapis.com/google.rpc.QuotaFailure":
            violations = detail.get("violations") or []
            if violations:
                return dict(violations[0])
    return None


def _raise_if_daily_quota_exhausted(exc: Exception) -> None:
    """Raises DailyQuotaExhaustedError if a RESOURCE_EXHAUSTED error is Gemini's daily
    quota rather than a per-minute burst (see KNOWN_RPM_LIMIT for how we tell). A
    no-op -- falling through to the existing retry -- if we can't tell, which is the
    safe default."""
    violation = _quota_violation(exc)
    if violation is None:
        return
    try:
        quota_value = int(violation["quotaValue"])
    except (KeyError, ValueError):
        return
    if quota_value <= KNOWN_RPM_LIMIT:
        return
    raise DailyQuotaExhaustedError(
        f"Gemini daily quota exhausted for model "
        f"{violation.get('quotaDimensions', {}).get('model')!r}: "
        f"quotaId={violation.get('quotaId')!r}, quotaValue={quota_value} "
        f"(> the {KNOWN_RPM_LIMIT}/min RPM ceiling, so this is the daily cap, not a "
        "per-minute burst). Not retrying — this won't clear until Google's next "
        "daily reset. Switch API key/model, or wait for the reset."
    ) from exc


class GeminiEnrichmentGenerator:
    """Implements the `EnrichmentGenerator` port (see `lib/ports.py`)."""

    def __init__(
        self,
        model: str = "gemini-3.1-flash-lite",
        temperature: float = 0,
        timeout: int = 60,
    ) -> None:
        self.model = model
        self.temperature = temperature
        self.timeout = timeout

    @property
    def sections(self) -> tuple[str, ...]:
        return SECTIONS

    def _chain(self, section: str) -> Runnable[dict[str, Any], str]:
        llm = ChatGoogleGenerativeAI(
            model=self.model,
            temperature=self.temperature,
            safety_settings=_SAFETY_OFF,
            timeout=self.timeout,
        )
        return _PROMPTS[section] | llm | StrOutputParser()

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
        """Generate one enrichment section for one movie, with the legacy retry/backoff
        and content-policy-block handling: on a per-minute RESOURCE_EXHAUSTED/timeout,
        retry with exponential backoff; on the *daily* quota being exhausted, raise
        DailyQuotaExhaustedError immediately instead (see KNOWN_RPM_LIMIT); on an empty
        response with a synopsis present, retry once without the synopsis (it may have
        triggered a safety block)."""
        chain = self._chain(section)
        delay = BASE_RETRY_DELAY
        current_synopsis: str | None = synopsis
        while True:
            try:
                result: str = chain.invoke(
                    {
                        "title": title,
                        "year": year,
                        "genres": ", ".join(genres),
                        "imdb_rating": imdb_rating,
                        "content_rating": content_rating,
                        "synopsis": current_synopsis or "(synopsis unavailable)",
                    }
                )
                if not result.strip():
                    if current_synopsis:
                        current_synopsis = None
                        continue
                    return None
                return result
            except Exception as e:
                err = str(e)
                if "429" in err or "RESOURCE_EXHAUSTED" in err:
                    _raise_if_daily_quota_exhausted(e)
                elif not (
                    "timeout" in err.lower()
                    or "deadline" in err.lower()
                    or "timed out" in err.lower()
                ):
                    raise
                time.sleep(delay)
                delay = min(delay * 2, MAX_RETRY_DELAY)
