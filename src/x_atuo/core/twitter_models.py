"""Structured models for Twitter automation workflows."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


AttemptOutcome = Literal[
    "would_reply",
    "author_not_verified",
    "reply_restricted",
    "reply_failed",
    "follow_failed",
    "tweet_fetch_failed",
    "replied",
]
EngageStatus = Literal["executed", "dry_run", "skipped", "failed"]


@dataclass(slots=True, frozen=True)
class TweetAuthor:
    screen_name: str
    verified: bool = False
    name: str | None = None
    user_id: str | None = None
    raw: dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass(slots=True, frozen=True)
class TweetRecord:
    tweet_id: str
    text: str
    author: TweetAuthor
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "TweetRecord":
        author = payload.get("author", {}) or {}
        return cls(
            tweet_id=str(payload.get("id") or payload.get("rest_id") or ""),
            text=str(payload.get("text") or payload.get("full_text") or ""),
            author=TweetAuthor(
                screen_name=str(author.get("screenName") or ""),
                verified=bool(author.get("verified")),
                name=str(author.get("name")) if author.get("name") else None,
                user_id=str(author.get("id")) if author.get("id") else None,
                raw=dict(author) if isinstance(author, dict) else {},
            ),
            raw=dict(payload),
        )

    @property
    def screen_name(self) -> str:
        return self.author.screen_name

    @property
    def verified(self) -> bool:
        return self.author.verified


@dataclass(slots=True, frozen=True)
class Candidate:
    tweet_id: str
    screen_name: str
    reply_text: str


@dataclass(slots=True, frozen=True)
class TwitterCommandResult:
    action: Literal["reply", "follow"]
    ok: bool
    dry_run: bool = False
    target_tweet_id: str | None = None
    tweet_id: str | None = None
    screen_name: str | None = None
    text: str | None = None
    payload: dict[str, Any] = field(default_factory=dict, repr=False)
    error_code: str | None = None
    error_message: str | None = None


@dataclass(slots=True, frozen=True)
class CandidateAttempt:
    candidate: Candidate
    outcome: AttemptOutcome
    tweet_id: str
    screen_name: str
    tweet: TweetRecord | None = None
    detail: str | None = None
    reply_result: TwitterCommandResult | None = None
    follow_result: TwitterCommandResult | None = None


@dataclass(slots=True, frozen=True)
class EngageResult:
    ok: bool
    status: EngageStatus
    attempts: tuple[CandidateAttempt, ...] = ()
    selected_candidate: Candidate | None = None
    reply_result: TwitterCommandResult | None = None
    follow_result: TwitterCommandResult | None = None
    feed_items: tuple[TweetRecord, ...] = ()
    error: str | None = None


@dataclass(slots=True, frozen=True)
class PostResult:
    ok: bool
    action: Literal["post", "quote"]
    text: str
    dry_run: bool = False
    tweet_id: str | None = None
    target_tweet_id: str | None = None
    media_paths: tuple[str, ...] = ()
    payload: dict[str, Any] = field(default_factory=dict, repr=False)
    error_code: str | None = None
    error_message: str | None = None
