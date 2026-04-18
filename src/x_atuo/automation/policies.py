"""Policy helpers for dedupe, cooldown, and length guards."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from typing import Protocol, runtime_checkable

from x_atuo.automation.config import PolicyConfig
from x_atuo.automation.state import AutomationRequest, FeedCandidate, PolicyDecision, WorkflowKind


@runtime_checkable
class PolicyHooks(Protocol):
    """Optional persistence hooks consulted during policy evaluation."""

    def has_dedupe_key(self, dedupe_key: str) -> bool:
        """Return whether the dedupe key was already recorded."""

    def get_daily_execution_count(self, workflow: WorkflowKind, day: date) -> int:
        """Return the number of executions for the workflow on the given day."""

    def get_last_author_engagement(self, screen_name: str) -> datetime | None:
        """Return the last engagement timestamp for the author."""

    def has_target_tweet_id(self, target_tweet_id: str) -> bool:
        """Return whether the target tweet has already been engaged."""


def build_dedupe_key(
    request: AutomationRequest,
    *,
    candidate: FeedCandidate | None = None,
    text: str | None = None,
) -> str:
    """Create a stable dedupe key across workflow executions."""

    seed = "|".join(
        [
            request.workflow.value,
            request.job_name or "",
            candidate.tweet_id if candidate else request.repo_url or "",
            candidate.screen_name or "" if candidate else "",
            (text or request.reply_text or request.post_text or "").strip(),
        ]
    )
    digest = sha256(seed.encode("utf-8")).hexdigest()[:20]
    return f"{request.workflow.value}:{digest}"


def check_text_length(text: str | None, *, limit: int) -> PolicyDecision:
    """Validate text length against a hard limit."""

    decision = PolicyDecision()
    if not text:
        return decision
    if len(text) > limit:
        decision.allowed = False
        decision.reasons.append(f"text length {len(text)} exceeds limit {limit}")
    return decision


def check_daily_limit(*, count: int, limit: int | None) -> PolicyDecision:
    """Validate workflow usage against a daily cap."""

    decision = PolicyDecision()
    if limit is not None and count >= limit:
        decision.allowed = False
        decision.reasons.append(f"daily execution limit reached ({count}/{limit})")
    return decision


def check_cooldown(
    *,
    last_seen_at: datetime | None,
    cooldown_minutes: int | None,
    now: datetime | None = None,
) -> PolicyDecision:
    """Validate that a cooldown window has elapsed."""

    decision = PolicyDecision()
    if last_seen_at is None or cooldown_minutes is None:
        return decision
    now = now or datetime.now(UTC)
    remaining = timedelta(minutes=cooldown_minutes) - (now - last_seen_at)
    if remaining.total_seconds() > 0:
        minutes = int(remaining.total_seconds() // 60) + 1
        decision.allowed = False
        decision.reasons.append(f"author cooldown active ({minutes}m remaining)")
    return decision


def merge_decisions(*decisions: PolicyDecision) -> PolicyDecision:
    """Combine multiple policy decisions into one verdict."""

    merged = PolicyDecision()
    for decision in decisions:
        merged.allowed = merged.allowed and decision.allowed
        merged.reasons.extend(decision.reasons)
        merged.warnings.extend(decision.warnings)
        if decision.dedupe_key:
            merged.dedupe_key = decision.dedupe_key
    return merged


def evaluate_policy(
    request: AutomationRequest,
    config: PolicyConfig,
    *,
    candidate: FeedCandidate | None = None,
    text: str | None = None,
    hooks: PolicyHooks | None = None,
    now: datetime | None = None,
) -> PolicyDecision:
    """Run the basic deterministic policy checks for a workflow."""

    decisions: list[PolicyDecision] = []
    now = now or datetime.now(UTC)
    limit = config.max_reply_length if request.workflow in {
        WorkflowKind.FEED_ENGAGE,
        WorkflowKind.EXPLICIT_ENGAGE,
    } else config.max_post_length
    decisions.append(check_text_length(text, limit=limit))

    dedupe_key = build_dedupe_key(request, candidate=candidate, text=text)
    dedupe = PolicyDecision(dedupe_key=dedupe_key)
    if config.enforce_dedupe and hooks:
        if (
            request.workflow in {WorkflowKind.FEED_ENGAGE, WorkflowKind.EXPLICIT_ENGAGE}
            and candidate is not None
            and hooks.has_target_tweet_id(candidate.tweet_id)
        ):
            dedupe.allowed = False
            dedupe.reasons.append("target tweet already engaged")
        elif hooks.has_dedupe_key(dedupe_key):
            dedupe.allowed = False
            dedupe.reasons.append("duplicate execution detected")
    decisions.append(dedupe)

    if hooks:
        count = hooks.get_daily_execution_count(request.workflow, now.date())
        decisions.append(check_daily_limit(count=count, limit=config.daily_execution_limit))
        if candidate and candidate.screen_name:
            last_author_at = hooks.get_last_author_engagement(candidate.screen_name)
            decisions.append(
                check_cooldown(
                    last_seen_at=last_author_at,
                    cooldown_minutes=config.per_author_cooldown_minutes,
                    now=now,
                )
            )

    return merge_decisions(*decisions)
