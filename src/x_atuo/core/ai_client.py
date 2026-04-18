"""Optional AI providers for selection and drafting."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from x_atuo.automation.config import AISettings
from x_atuo.automation.state import FeedCandidate, RepoContext


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


class BaseAIProvider:
    def select_candidate(self, candidates: list[FeedCandidate]) -> AISelectionResult:
        raise NotImplementedError

    def moderate_candidates(self, candidates: list[FeedCandidate]) -> list[AIModerationResult]:
        raise NotImplementedError

    def draft_reply(self, candidate: FeedCandidate) -> AIDraftResult:
        raise NotImplementedError

    def draft_repo_post(self, context: RepoContext) -> AIDraftResult:
        raise NotImplementedError


class MockAIProvider(BaseAIProvider):
    """Deterministic provider for local testing and fallback demos."""

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

    def draft_reply(self, candidate: FeedCandidate) -> AIDraftResult:
        handle = candidate.screen_name or "author"
        return AIDraftResult(
            text=f"@{handle} The real shift here is where the bottleneck moves next.",
            rationale="mock provider generated a shorter technical reply",
        )

    def draft_repo_post(self, context: RepoContext) -> AIDraftResult:
        repo_name = context.repo_name or context.repo_url
        desc = context.description or context.readme_excerpt or "Open-source repository"
        text = (
            f"{repo_name}: {desc.rstrip('.')}.\n\n"
            f"Link: {context.repo_url}\n#OpenSource #GitHub #DevTools"
        )
        return AIDraftResult(
            text=text[:180],
            rationale="mock provider generated a shorter repository summary",
        )


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
            "Select one Twitter candidate for technical engagement. Return JSON with tweet_id and reason.",
            json.dumps([item.model_dump(mode="json") for item in candidates], ensure_ascii=False),
        )
        try:
            parsed = self._parse_json_content(content)
            return AISelectionResult(tweet_id=str(parsed["tweet_id"]), reason=str(parsed.get("reason") or ""))
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise AIProviderError(f"Could not parse AI selection response: {content}") from exc

    def moderate_candidates(self, candidates: list[FeedCandidate]) -> list[AIModerationResult]:
        content = self._chat(
            "Review Twitter feed candidates for reply safety. Reject anything about politics, crime, violence, fraud, scams, drugs, war, military conflict, law enforcement, or case news. Allow technical, product, engineering, and builder content. Return JSON with a results array of {tweet_id, allowed, category, reason}.",
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

    def draft_reply(self, candidate: FeedCandidate) -> AIDraftResult:
        content = self._chat(
            "Draft one short technical Twitter reply under 100 chars. Write like a sharp practitioner, not a summarizer. Lead with a judgment, useful angle, or tension. Keep it conversational and plainspoken. Prefer a direct statement, not a question. Avoid generic praise, repetition, and restating the post. One or two short sentences. No lists. No emojis. Return JSON with text and rationale.",
            json.dumps(candidate.model_dump(mode="json"), ensure_ascii=False),
        )
        try:
            parsed = self._parse_json_content(content)
            return AIDraftResult(text=str(parsed["text"]), rationale=str(parsed.get("rationale") or ""))
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise AIProviderError(f"Could not parse AI draft response: {content}") from exc

    def draft_repo_post(self, context: RepoContext) -> AIDraftResult:
        content = self._chat(
            "Draft one short repository recommendation tweet under 160 chars. Keep it simple and direct. Return JSON with text and rationale.",
            json.dumps(context.model_dump(mode="json"), ensure_ascii=False),
        )
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
