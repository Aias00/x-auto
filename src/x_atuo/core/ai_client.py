"""Optional AI providers for selection and drafting."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from x_atuo.automation.config import AISettings
from x_atuo.automation.state import FeedCandidate


class AIProviderError(RuntimeError):
    """Raised when an AI provider cannot complete a request."""


@dataclass(slots=True, frozen=True)
class AISelectionResult:
    tweet_id: str
    reason: str


@dataclass(slots=True, frozen=True)
class AIDraftResult:
    text: str
    rationale: str


@dataclass(slots=True, frozen=True)
class AIModerationResult:
    tweet_id: str
    allowed: bool
    category: str | None
    reason: str


@dataclass(slots=True, frozen=True)
class AIReplyStyleDecision:
    style: str
    reason: str


class BaseAIProvider:
    def select_candidate(self, candidates: list[FeedCandidate]) -> AISelectionResult:
        raise NotImplementedError

    def moderate_candidates(self, candidates: list[FeedCandidate]) -> list[AIModerationResult]:
        raise NotImplementedError

    def draft_reply(self, candidate: FeedCandidate, context: dict[str, Any] | None = None) -> AIDraftResult:
        raise NotImplementedError

    def classify_reply_style(self, candidate: FeedCandidate) -> AIReplyStyleDecision:
        raise NotImplementedError


def _compact_candidate_payload(
    candidate_payload: dict[str, Any],
    *,
    include_media_types: bool = False,
) -> dict[str, Any]:
    compact = {
        key: candidate_payload[key]
        for key in ("tweet_id", "screen_name", "text", "created_at", "author_verified")
        if candidate_payload.get(key) is not None
    }
    if not include_media_types:
        return compact

    metadata = candidate_payload.get("metadata")
    media = metadata.get("media") if isinstance(metadata, dict) else None
    if not isinstance(media, list):
        return compact

    media_types: list[str] = []
    for item in media:
        if not isinstance(item, dict):
            continue
        media_type = item.get("type")
        if not isinstance(media_type, str) or not media_type:
            continue
        if media_type not in media_types:
            media_types.append(media_type)
    if media_types:
        compact["media_types"] = media_types
    return compact


class MockAIProvider(BaseAIProvider):
    """Mock provider for local testing and local AI-shaped flows."""

    def select_candidate(self, candidates: list[FeedCandidate]) -> AISelectionResult:
        if not candidates:
            raise AIProviderError("No candidates available")
        selected = max(candidates, key=lambda item: len(item.text or ""))
        return AISelectionResult(
            tweet_id=selected.tweet_id,
            reason="mock provider selected the richest candidate text",
        )

    def moderate_candidates(self, candidates: list[FeedCandidate]) -> list[AIModerationResult]:
        return [
            AIModerationResult(
                tweet_id=candidate.tweet_id,
                allowed=True,
                category=None,
                reason="mock provider kept candidate",
            )
            for candidate in candidates
        ]

    def draft_reply(self, candidate: FeedCandidate, context: dict[str, Any] | None = None) -> AIDraftResult:
        handle = candidate.screen_name or "author"
        return AIDraftResult(
            text=f"@{handle} The real shift here is where the bottleneck moves next.",
            rationale="mock provider generated a shorter technical reply",
        )

    def classify_reply_style(self, candidate: FeedCandidate) -> AIReplyStyleDecision:
        return AIReplyStyleDecision(style="technical", reason="mock provider default")

class OpenAICompatibleProvider(BaseAIProvider):
    """Simple OpenAI-compatible chat completions provider via stdlib HTTP."""

    def __init__(self, settings: AISettings):
        if not settings.api_key:
            raise AIProviderError("AI provider configured without api_key")
        if not settings.model:
            raise AIProviderError("AI provider configured without model")
        self.settings = settings

    def _chat(self, system: str, user: str) -> str:
        url = self.settings.base_url.rstrip("/") + "/chat/completions"
        payload = {
            "model": self.settings.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": 0.2,
        }
        request = Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.settings.api_key}",
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.settings.timeout_seconds) as response:
                body = json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError) as exc:
            raise AIProviderError(f"AI request failed: {exc}") from exc
        try:
            return str(body["choices"][0]["message"]["content"]).strip()
        except (KeyError, IndexError, TypeError) as exc:
            raise AIProviderError("AI response missing choices[0].message.content") from exc

    def _parse_json_content(self, content: str) -> dict:
        cleaned = content.strip()
        fenced = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", cleaned, flags=re.DOTALL | re.IGNORECASE)
        if fenced:
            cleaned = fenced.group(1).strip()
        if not cleaned.startswith(("{", "[")):
            object_match = re.search(r"(\{.*\}|\[.*\])", cleaned, flags=re.DOTALL)
            if object_match:
                cleaned = object_match.group(1).strip()
        parsed = json.loads(cleaned)
        if not isinstance(parsed, dict):
            raise AIProviderError(f"Expected JSON object response, got: {type(parsed)!r}")
        return parsed

    def select_candidate(self, candidates: list[FeedCandidate]) -> AISelectionResult:
        content = self._chat(
            "Select one Twitter candidate for engagement. Return JSON with tweet_id and reason. Keep the reason short, plain-English, and matched to the post itself instead of forcing technical framing.",
            json.dumps([item.model_dump(mode="json") for item in candidates], ensure_ascii=False),
        )
        try:
            parsed = self._parse_json_content(content)
            return AISelectionResult(tweet_id=str(parsed["tweet_id"]), reason=str(parsed.get("reason") or ""))
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise AIProviderError(f"Could not parse AI selection response: {content}") from exc

    def moderate_candidates(self, candidates: list[FeedCandidate]) -> list[AIModerationResult]:
        content = self._chat(
            "Review Twitter feed candidates for reply safety. Reject anything about crime, violence, fraud, scams, drugs, war, military conflict, law enforcement, case news, adult or NSFW content, hate or harassment, self-harm or dangerous behavior, gambling or illicit activity, extremism, crypto shilling or guaranteed-profit investment claims, and medical or legal high-risk advice. Allow technical, product, engineering, builder, developer-adjacent, pets and animals, lifestyle, food, travel, scenic photography, entertainment, memes, and casual social content. Always allow posts from @elonmusk. Return JSON with a results array of {tweet_id, allowed, category, reason}.",
            json.dumps([item.model_dump(mode="json") for item in candidates], ensure_ascii=False),
        )
        try:
            parsed = self._parse_json_content(content)
            raw_results = parsed["results"]
            if not isinstance(raw_results, list):
                raise TypeError("results must be a list")
            return [
                AIModerationResult(
                    tweet_id=str(item["tweet_id"]),
                    allowed=bool(item["allowed"]),
                    category=str(item["category"]) if item.get("category") is not None else None,
                    reason=str(item.get("reason") or ""),
                )
                for item in raw_results
            ]
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise AIProviderError(f"Could not parse AI moderation response: {content}") from exc

    def classify_reply_style(self, candidate: FeedCandidate) -> AIReplyStyleDecision:
        content = self._chat(
            "Classify the best reply style for a Twitter post. Return JSON with style and reason. Use style=technical for engineering, developer, product, infrastructure, AI, coding, or tooling posts. Use style=non_technical for pets, animals, lifestyle, food, travel, scenic, entertainment, meme, casual social, image-first, or reaction posts. Use style=mixed when the post mixes casual content with product or AI references.",
            json.dumps(
                {"candidate": _compact_candidate_payload(candidate.model_dump(mode="json"), include_media_types=True)},
                ensure_ascii=False,
            ),
        )
        try:
            parsed = self._parse_json_content(content)
            style = str(parsed.get("style") or "technical")
            if style not in {"technical", "non_technical", "mixed"}:
                style = "technical"
            return AIReplyStyleDecision(style=style, reason=str(parsed.get("reason") or ""))
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise AIProviderError(f"Could not parse AI reply style response: {content}") from exc

    def draft_reply(self, candidate: FeedCandidate, context: dict[str, Any] | None = None) -> AIDraftResult:
        reply_style = str(context.get("reply_style") or "") if isinstance(context, dict) else ""
        system = (
            "Draft one short technical Twitter reply under 100 chars. "
            "Write like a sharp practitioner, not a summarizer. Lead with a judgment, useful angle, or tension. "
            "Keep it conversational and plainspoken. Prefer a direct statement, not a question. "
            "Avoid generic praise, repetition, and restating the post. One or two short sentences. No lists. No emojis. "
            "Return JSON with text and rationale."
        )
        if reply_style == "non_technical":
            system = (
                "Draft one short Twitter reply under 100 chars for a non-technical post. "
                "Sound natural, relaxed, and human. Lead with a light observation, mild reaction, gentle humor, or easy empathy. "
                "Do not force technical jargon, engineering framing, or heavy analysis onto the post. "
                "Keep it conversational and plainspoken. One or two short sentences. Prefer statements over questions. No lists. No emojis. Return JSON with text and rationale."
            )
        elif reply_style == "mixed":
            system = (
                "Draft one short Twitter reply under 100 chars for a mixed technical and casual post. "
                "Keep it natural and human, with one light judgment or observation. Use plain language and avoid heavy technical framing unless the post clearly invites it. "
                "One or two short sentences. Prefer statements over questions. No lists. No emojis. Return JSON with text and rationale."
            )
        user_payload: dict[str, Any] = {
            "candidate": _compact_candidate_payload(
                candidate.model_dump(mode="json"),
                include_media_types=True,
            )
        }
        content = self._chat(system, json.dumps(user_payload, ensure_ascii=False))
        try:
            parsed = self._parse_json_content(content)
            return AIDraftResult(text=str(parsed["text"]), rationale=str(parsed.get("rationale") or ""))
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise AIProviderError(f"Could not parse AI draft response: {content}") from exc

def build_ai_provider(settings: AISettings) -> BaseAIProvider | None:
    if settings.provider == "none":
        return None
    if settings.provider == "mock":
        return MockAIProvider()
    if settings.provider == "openai_compatible":
        return OpenAICompatibleProvider(settings)
    raise AIProviderError(f"Unsupported AI provider: {settings.provider}")
