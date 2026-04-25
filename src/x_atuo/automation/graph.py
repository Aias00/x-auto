"""LangGraph workflow definitions for automation runs."""

from __future__ import annotations

import asyncio
import inspect
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Any, Callable

from langgraph.graph import END, StateGraph

from x_atuo.automation.config import AutomationConfig
from x_atuo.automation.policies import (
    PolicyHooks,
    build_dedupe_key,
    check_cooldown,
    check_daily_limit,
    evaluate_policy,
    merge_decisions,
)
from x_atuo.automation.state import (
    AutomationGraphState,
    AutomationRequest,
    ExecutionResult,
    FeedCandidate,
    FeedOptions,
    PolicyDecision,
    RunStatus,
    WorkflowKind,
    WorkflowStateModel,
    make_initial_state,
)
from x_atuo.core.ai_client import (
    AIModerationResult,
    AIProviderError,
    build_draft_prompt_payload,
    build_moderation_cache_key,
    build_moderation_prompt_payload,
    build_ai_provider,
)
from x_atuo.core.twitter_client import TwitterClient
from x_atuo.core.twitter_engage_service import TwitterEngageService
from x_atuo.core.twitter_client import _reply_control_reason
from x_atuo.core.twitter_models import Candidate, TweetRecord

StateCallable = Callable[[WorkflowStateModel], Any]
_AI_MODERATION_METADATA_KEY = "_x_atuo_ai_moderation"


async def maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _is_moderation_exempt_candidate(candidate: FeedCandidate) -> bool:
    return (candidate.screen_name or "").strip().lower() == "elonmusk"


def _candidate_media_types(candidate: FeedCandidate) -> set[str]:
    metadata = candidate.metadata if isinstance(candidate.metadata, dict) else {}
    media = metadata.get("media") if isinstance(metadata.get("media"), list) else []
    media_types: set[str] = set()
    for item in media:
        if isinstance(item, dict) and isinstance(item.get("type"), str) and item.get("type"):
            media_types.add(str(item["type"]).lower())
    return media_types


def _rule_based_reply_style(candidate: FeedCandidate | None) -> str | None:
    if candidate is None:
        return None
    screen_name = (candidate.screen_name or "").strip().lower()
    if screen_name == "elonmusk":
        return "mixed"

    text = (candidate.text or "").strip().lower()
    media_types = _candidate_media_types(candidate)
    technical_markers = (
        "api", "sdk", "repo", "open source", "github", "deploy", "inference", "latency", "cache",
        "prompt", "llm", "model", "agent", "workflow", "code", "coding", "developer", "engineering",
        "gpu", "benchmark", "eval", "tooling", "artifact", "worker", "cloudflare", "ai studio",
    )
    nontechnical_markers = (
        "cat", "dog", "puppy", "hamster", "koala", "pet", "food", "coffee", "breakfast", "family",
        "travel", "scenic", "sunrise", "meme", "lol", "funny", "cute", "😂", "🤣", "😅", "❤️",
    )

    if any(marker in text for marker in technical_markers):
        return "technical"
    if any(marker in text for marker in nontechnical_markers):
        return "non_technical"
    if media_types and len(text) <= 120:
        return "non_technical"
    return None


def _normalized_selection_reason(
    candidate: FeedCandidate | None,
    selection_reason: str | None,
    reply_style: str,
) -> str | None:
    reason = str(selection_reason or "").strip()
    if candidate is None or reply_style not in {"non_technical", "mixed"}:
        return reason or None

    if reply_style == "mixed" and reason:
        return reason

    metadata = candidate.metadata if isinstance(candidate.metadata, dict) else {}
    metrics = metadata.get("metrics") if isinstance(metadata.get("metrics"), dict) else {}
    likes = metrics.get("likes") if isinstance(metrics.get("likes"), int) else None
    replies = metrics.get("replies") if isinstance(metrics.get("replies"), int) else None
    views = metrics.get("views") if isinstance(metrics.get("views"), int) else None
    media_types = _candidate_media_types(candidate)
    text = (candidate.text or "").strip().lower()

    descriptors: list[str] = []
    if any(marker in text for marker in ("cat", "dog", "puppy", "kitten", "pet", "hamster", "cute")):
        descriptors.append("warm pet post")
    elif any(marker in text for marker in ("wallpaper", "baby", "family", "food", "travel", "scenic", "sunrise")):
        descriptors.append("easygoing lifestyle post")
    elif any(marker in text for marker in ("meme", "lol", "funny", "😂", "🤣", "reaction")):
        descriptors.append("playful casual post")
    else:
        descriptors.append("casual visual post" if media_types else "casual post")

    if media_types:
        if "video" in media_types:
            descriptors.append("clear visual hook")
        elif "photo" in media_types:
            descriptors.append("strong image appeal")

    if isinstance(views, int) and isinstance(likes, int) and views > 0:
        engagement_ratio = likes / views
        if engagement_ratio >= 0.1:
            descriptors.append("healthy engagement")
    elif isinstance(likes, int) and likes >= 20:
        descriptors.append("good engagement")

    if isinstance(replies, int) and replies > 0:
        descriptors.append("easy opening for a friendly reply")
    else:
        descriptors.append("natural fit for a light reply")

    parts = descriptors[:3]
    if len(parts) == 1:
        phrase = parts[0]
    else:
        phrase = ", ".join(parts[:-1]) + f", and {parts[-1]}"
    return f"{phrase.capitalize()}."


def _call_with_optional_context(method: Callable[..., Any], *args: Any) -> Any:
    try:
        params = inspect.signature(method).parameters
    except (TypeError, ValueError):
        return method(*args[:1])
    if len(params) >= len(args):
        return method(*args)
    return method(*args[:1])


def _elapsed_ms(started_at: float) -> float:
    return round((perf_counter() - started_at) * 1000, 2)


def _moderation_payload_bytes(candidates: list[FeedCandidate]) -> int:
    return len(
        json.dumps(
            build_moderation_prompt_payload(candidates),
            ensure_ascii=False,
        ).encode("utf-8")
    )


def _draft_payload_bytes(candidate: FeedCandidate) -> int:
    return len(
        json.dumps(
            build_draft_prompt_payload(candidate),
            ensure_ascii=False,
        ).encode("utf-8")
    )


def _cached_ai_moderation(
    candidate: FeedCandidate,
    *,
    provider_name: str | None,
    model_name: str | None,
) -> AIModerationResult | None:
    metadata = candidate.metadata if isinstance(candidate.metadata, dict) else {}
    raw = metadata.get(_AI_MODERATION_METADATA_KEY)
    if not isinstance(raw, dict):
        return None
    if raw.get("tweet_id") != candidate.tweet_id:
        return None
    if raw.get("cache_key") != build_moderation_cache_key(candidate, provider=provider_name, model=model_name):
        return None
    allowed = raw.get("allowed")
    if not isinstance(allowed, bool):
        return None
    return AIModerationResult(
        tweet_id=candidate.tweet_id,
        allowed=allowed,
        category=str(raw["category"]) if raw.get("category") is not None else None,
        reason=str(raw.get("reason") or ""),
    )


def _store_ai_moderation(
    candidate: FeedCandidate,
    moderation: AIModerationResult,
    *,
    provider_name: str | None,
    model_name: str | None,
) -> None:
    metadata = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
    metadata[_AI_MODERATION_METADATA_KEY] = {
        "tweet_id": moderation.tweet_id,
        "cache_key": build_moderation_cache_key(candidate, provider=provider_name, model=model_name),
        "allowed": bool(moderation.allowed),
        "category": moderation.category,
        "reason": moderation.reason,
    }
    candidate.metadata = metadata


@dataclass(slots=True)
class WorkflowAdapters:
    fetch_feed: StateCallable | None = None
    select_candidate: StateCallable | None = None
    draft_reply: StateCallable | None = None
    execute_engage: StateCallable | None = None
    policy_hooks: PolicyHooks | None = None


class AutomationGraph:
    """Compiled LangGraph workflows with lightweight orchestration logic."""

    def __init__(self, config: AutomationConfig, adapters: WorkflowAdapters | None = None) -> None:
        self.config = config
        self.adapters = adapters or WorkflowAdapters()
        self.max_candidate_refresh_rounds = config.policies.candidate_refresh_rounds
        self.graph = self._build_graph()

    def _schedule_candidate_refresh(self, snapshot: WorkflowStateModel, *, node: str, reason: str) -> bool:
        if snapshot.request.workflow is not WorkflowKind.FEED_ENGAGE:
            return False
        if snapshot.candidate_refresh_count >= self.max_candidate_refresh_rounds:
            return False
        snapshot.candidate_refresh_count += 1
        snapshot.candidate_refresh_pending = True
        self._reset_for_feed_refresh(snapshot)
        snapshot.candidate_cache_persisted = False
        snapshot.log_event(
            node,
            "candidate pool empty, refreshing feed",
            reason=reason,
            attempt=snapshot.candidate_refresh_count,
            max_attempts=self.max_candidate_refresh_rounds,
        )
        return True

    @staticmethod
    def _reset_after_candidate_rejection(snapshot: WorkflowStateModel) -> None:
        snapshot.selected_candidate = None
        snapshot.selection_source = None
        snapshot.selection_reason = None
        snapshot.reply_context = {}
        snapshot.rendered_text = None
        snapshot.drafting_source = None
        snapshot.policy = PolicyDecision()

    def _reset_for_feed_refresh(self, snapshot: WorkflowStateModel) -> None:
        self._reset_after_candidate_rejection(snapshot)
        snapshot.candidates = []

    @staticmethod
    def _extract_execution_attempts(result: ExecutionResult) -> list[dict[str, Any]]:
        detail = result.detail if isinstance(result.detail, dict) else {}
        attempts = detail.get("attempts") if isinstance(detail.get("attempts"), list) else []
        return [attempt for attempt in attempts if isinstance(attempt, dict)]

    @staticmethod
    def _build_prefilter_empty_reasons(
        *,
        removed_unverified: int,
        removed_already_engaged: int,
        removed_reply_restricted: list[str],
    ) -> list[str]:
        reasons: list[str] = []
        if removed_unverified:
            reasons.append("author not verified")
        reasons.extend(reason for reason in removed_reply_restricted if reason not in reasons)
        if removed_already_engaged:
            reasons.append("target tweet already engaged")
        return reasons

    async def invoke(
        self,
        request: AutomationRequest,
        graph_config: dict[str, Any] | None = None,
    ) -> WorkflowStateModel:
        state = make_initial_state(request)
        try:
            if graph_config is not None:
                result = await self.graph.ainvoke(state, config=graph_config)
            else:
                result = await self.graph.ainvoke(state)
            return result["snapshot"]
        except Exception as exc:
            snapshot = state["snapshot"]
            snapshot.mark_failed(str(exc))
            return snapshot

    def _build_graph(self):
        workflow = StateGraph(AutomationGraphState)
        workflow.add_node("prepare", self.prepare)
        workflow.add_node("fetch_feed", self.fetch_feed)
        workflow.add_node("prefilter_candidates", self.prefilter_candidates)
        workflow.add_node("select_candidate", self.select_candidate)
        workflow.add_node("candidate_policy_guard", self.candidate_policy_guard)
        workflow.add_node("draft_text", self.draft_text)
        workflow.add_node("policy_guard", self.policy_guard)
        workflow.add_node("blocked", self.blocked)
        workflow.add_node("execute", self.execute)
        workflow.add_node("finalize", self.finalize)
        workflow.set_entry_point("prepare")
        workflow.add_conditional_edges(
            "prepare",
            self.route_after_prepare,
            {
                "fetch_feed": "fetch_feed",
            },
        )
        workflow.add_conditional_edges(
            "fetch_feed",
            self.route_after_fetch_feed,
            {
                "fetch_feed": "fetch_feed",
                "prefilter_candidates": "prefilter_candidates",
                "finalize": "finalize",
            },
        )
        workflow.add_conditional_edges(
            "prefilter_candidates",
            self.route_after_prefilter,
            {
                "fetch_feed": "fetch_feed",
                "select_candidate": "select_candidate",
                "blocked": "blocked",
                "finalize": "finalize",
            },
        )
        workflow.add_conditional_edges(
            "select_candidate",
            self.route_after_selection,
            {
                "candidate_policy_guard": "candidate_policy_guard",
                "finalize": "finalize",
            },
        )
        workflow.add_conditional_edges(
            "candidate_policy_guard",
            self.route_after_candidate_policy,
            {
                "retry_candidate": "select_candidate",
                "draft_text": "draft_text",
                "blocked": "blocked",
            },
        )
        workflow.add_edge("draft_text", "policy_guard")
        workflow.add_conditional_edges(
            "policy_guard",
            self.route_after_policy,
            {
                "execute": "execute",
                "retry_candidate": "select_candidate",
                "blocked": "blocked",
            },
        )
        workflow.add_edge("blocked", "finalize")
        workflow.add_conditional_edges(
            "execute",
            self.route_after_execute,
            {
                "fetch_feed": "fetch_feed",
                "finalize": "finalize",
            },
        )
        workflow.add_edge("finalize", END)
        return workflow.compile()

    async def prepare(self, state: AutomationGraphState) -> AutomationGraphState:
        snapshot = state["snapshot"]
        snapshot.log_event("prepare", "request prepared")
        return {"snapshot": snapshot}

    def route_after_prepare(self, state: AutomationGraphState) -> str:
        return "fetch_feed"

    async def fetch_feed(self, state: AutomationGraphState) -> AutomationGraphState:
        snapshot = state["snapshot"]
        started_at = perf_counter()
        snapshot.candidate_refresh_pending = False
        if snapshot.candidates:
            snapshot.log_event("fetch_feed", "using preloaded candidates", count=len(snapshot.candidates))
            return {"snapshot": snapshot}
        if self.adapters.fetch_feed is None:
            snapshot.mark_failed("feed-engage requires fetch_feed adapter", node="fetch_feed")
            return {"snapshot": snapshot}
        fetched = await maybe_await(self.adapters.fetch_feed(snapshot))
        snapshot.candidates = list(fetched or [])
        snapshot.candidate_cache_persisted = bool(snapshot.candidates) and all(
            isinstance(candidate.metadata, dict) and bool(candidate.metadata.get("_x_atuo_candidate_cache"))
            for candidate in snapshot.candidates
        )
        metrics = snapshot.pop_runtime_observability("fetch_feed")
        snapshot.log_event("fetch_feed", "feed fetched", count=len(snapshot.candidates))
        if isinstance(metrics, dict):
            snapshot.events[-1].payload.update(metrics)
        snapshot.events[-1].payload.setdefault("duration_ms", round((perf_counter() - started_at) * 1000, 2))
        if not snapshot.candidates:
            if not self._schedule_candidate_refresh(
                snapshot,
                node="fetch_feed",
                reason="feed returned no candidates",
            ):
                snapshot.log_event(
                    "fetch_feed",
                    "candidate refresh limit reached",
                    reason="feed returned no candidates",
                    attempts=snapshot.candidate_refresh_count,
                )
                snapshot.mark_failed("feed returned no candidates", node="fetch_feed")
        return {"snapshot": snapshot}

    def route_after_fetch_feed(self, state: AutomationGraphState) -> str:
        snapshot = state["snapshot"]
        if snapshot.status is RunStatus.FAILED:
            return "finalize"
        if snapshot.candidate_refresh_pending:
            return "fetch_feed"
        return "prefilter_candidates"

    async def prefilter_candidates(self, state: AutomationGraphState) -> AutomationGraphState:
        snapshot = state["snapshot"]
        if snapshot.status is RunStatus.FAILED:
            return {"snapshot": snapshot}
        if snapshot.request.workflow is not WorkflowKind.FEED_ENGAGE:
            return {"snapshot": snapshot}
        hooks = self.adapters.policy_hooks
        if not snapshot.candidates:
            return {"snapshot": snapshot}

        filtered_candidates: list[FeedCandidate] = []
        removed_count = 0
        removed_unverified = 0
        removed_already_engaged = 0
        removed_reply_restricted: list[str] = []
        for candidate in snapshot.candidates:
            if candidate.author_verified is False:
                removed_count += 1
                removed_unverified += 1
                snapshot.log_event(
                    "prefilter_candidates",
                    "candidate removed before selection",
                    tweet_id=candidate.tweet_id,
                    screen_name=candidate.screen_name,
                    reason="author not verified",
                )
                continue
            if candidate.can_reply is False:
                removed_count += 1
                restriction_reason = candidate.reply_limit_reason or candidate.reply_limit_headline or "reply restricted"
                if restriction_reason not in removed_reply_restricted:
                    removed_reply_restricted.append(restriction_reason)
                snapshot.log_event(
                    "prefilter_candidates",
                    "candidate removed before selection",
                    tweet_id=candidate.tweet_id,
                    screen_name=candidate.screen_name,
                    reason=restriction_reason,
                )
                continue
            if hooks is not None and hooks.has_target_tweet_id(candidate.tweet_id):
                removed_count += 1
                removed_already_engaged += 1
                snapshot.log_event(
                    "prefilter_candidates",
                    "candidate removed before selection",
                    tweet_id=candidate.tweet_id,
                    screen_name=candidate.screen_name,
                    reason="target tweet already engaged",
                )
                continue
            filtered_candidates.append(candidate)

        if removed_count:
            snapshot.candidates = filtered_candidates
            if removed_unverified:
                snapshot.log_event(
                    "prefilter_candidates",
                    "unverified candidates removed",
                    removed=removed_unverified,
                    remaining=len(snapshot.candidates),
                )
            if removed_already_engaged:
                snapshot.log_event(
                    "prefilter_candidates",
                    "already-engaged candidates removed",
                    removed=removed_already_engaged,
                    remaining=len(snapshot.candidates),
                )
        if not snapshot.candidates:
            empty_reasons = self._build_prefilter_empty_reasons(
                removed_unverified=removed_unverified,
                removed_already_engaged=removed_already_engaged,
                removed_reply_restricted=removed_reply_restricted,
            )
            refresh_reason = ", ".join(empty_reasons) if empty_reasons else "prefilter removed all candidates"
            if not self._schedule_candidate_refresh(
                snapshot,
                node="prefilter_candidates",
                reason=refresh_reason,
            ):
                snapshot.log_event(
                    "prefilter_candidates",
                    "candidate refresh limit reached",
                    reason=refresh_reason,
                    attempts=snapshot.candidate_refresh_count,
                )
                snapshot.mark_blocked(empty_reasons or ["prefilter removed all candidates"], node="prefilter_candidates")
        return {"snapshot": snapshot}

    def route_after_prefilter(self, state: AutomationGraphState) -> str:
        snapshot = state["snapshot"]
        if snapshot.status is RunStatus.BLOCKED:
            return "blocked"
        if snapshot.status is RunStatus.FAILED:
            return "finalize"
        if snapshot.candidate_refresh_pending:
            return "fetch_feed"
        return "select_candidate"

    async def select_candidate(self, state: AutomationGraphState) -> AutomationGraphState:
        snapshot = state["snapshot"]
        if snapshot.status is RunStatus.FAILED:
            return {"snapshot": snapshot}
        if snapshot.selected_candidate is None:
            if self.adapters.select_candidate is not None:
                try:
                    selected = await maybe_await(self.adapters.select_candidate(snapshot))
                except AIProviderError as exc:
                    metrics = snapshot.pop_runtime_observability("select_candidate")
                    snapshot.log_event("select_candidate", "candidate evaluation failed", error=str(exc), **metrics)
                    snapshot.mark_failed(f"candidate evaluation failed: {exc}", node="select_candidate")
                    return {"snapshot": snapshot}
                if selected is not None:
                    snapshot.selected_candidate = selected
            elif snapshot.candidates:
                snapshot.selected_candidate = snapshot.candidates[0]
        if snapshot.selected_candidate is None:
            if snapshot.request.workflow is WorkflowKind.FEED_ENGAGE and not snapshot.candidates:
                if self._schedule_candidate_refresh(
                    snapshot,
                    node="select_candidate",
                    reason="no selectable candidates after hydration",
                ):
                    return {"snapshot": snapshot}
            snapshot.mark_failed("no candidate available for engagement", node="select_candidate")
        else:
            if snapshot.selection_source is None:
                snapshot.selection_source = "unknown"
            metrics = snapshot.pop_runtime_observability("select_candidate")
            snapshot.log_event(
                "select_candidate",
                "candidate selected",
                tweet_id=snapshot.selected_candidate.tweet_id,
                screen_name=snapshot.selected_candidate.screen_name,
                selected_by=snapshot.selection_source or "unknown",
                **metrics,
            )
        return {"snapshot": snapshot}

    def route_after_selection(self, state: AutomationGraphState) -> str:
        snapshot = state["snapshot"]
        if snapshot.status is RunStatus.FAILED:
            return "finalize"
        return "candidate_policy_guard"

    async def candidate_policy_guard(self, state: AutomationGraphState) -> AutomationGraphState:
        snapshot = state["snapshot"]
        if snapshot.status is RunStatus.FAILED:
            return {"snapshot": snapshot}
        if snapshot.request.workflow is not WorkflowKind.FEED_ENGAGE or snapshot.selected_candidate is None:
            return {"snapshot": snapshot}

        snapshot.policy = self._evaluate_candidate_policy(snapshot)
        snapshot.log_event(
            "candidate_policy_guard",
            "candidate policy evaluated",
            allowed=snapshot.policy.allowed,
            reasons=snapshot.policy.reasons,
            dedupe_key=snapshot.policy.dedupe_key,
        )
        if not snapshot.policy.allowed:
            if not self._retry_blocked_candidate(snapshot, node="candidate_policy_guard"):
                snapshot.mark_blocked(snapshot.policy.reasons, node="candidate_policy_guard")
        return {"snapshot": snapshot}

    def route_after_candidate_policy(self, state: AutomationGraphState) -> str:
        snapshot = state["snapshot"]
        if snapshot.status in {RunStatus.BLOCKED, RunStatus.FAILED}:
            return "blocked"
        if snapshot.request.workflow is WorkflowKind.FEED_ENGAGE and snapshot.selected_candidate is None:
            return "retry_candidate"
        return "draft_text"

    async def draft_text(self, state: AutomationGraphState) -> AutomationGraphState:
        snapshot = state["snapshot"]
        if snapshot.status is RunStatus.FAILED:
            return {"snapshot": snapshot}
        if snapshot.request.reply_text:
            snapshot.rendered_text = snapshot.request.reply_text
        elif self.adapters.draft_reply is not None:
            try:
                snapshot.rendered_text = await maybe_await(self.adapters.draft_reply(snapshot))
            except AIProviderError as exc:
                snapshot.log_event("draft_text", "ai draft failed", error=str(exc))
                snapshot.mark_failed(f"ai draft failed: {exc}", node="draft_text")
                return {"snapshot": snapshot}
        else:
            snapshot.mark_failed("feed-engage requires ai draft adapter", node="draft_text")
            return {"snapshot": snapshot}
        if snapshot.rendered_text:
            snapshot.log_event("draft_text", "text ready", length=len(snapshot.rendered_text))
        return {"snapshot": snapshot}

    async def policy_guard(self, state: AutomationGraphState) -> AutomationGraphState:
        snapshot = state["snapshot"]
        if snapshot.status is RunStatus.FAILED:
            return {"snapshot": snapshot}
        snapshot.policy = evaluate_policy(
            snapshot.request,
            self.config.policies,
            candidate=snapshot.selected_candidate,
            text=snapshot.rendered_text,
            hooks=self.adapters.policy_hooks,
        )
        snapshot.log_event(
            "policy_guard",
            "policy evaluated",
            allowed=snapshot.policy.allowed,
            reasons=snapshot.policy.reasons,
            dedupe_key=snapshot.policy.dedupe_key,
        )
        if not snapshot.policy.allowed:
            if not self._retry_blocked_candidate(snapshot):
                snapshot.mark_blocked(snapshot.policy.reasons)
        else:
            self._release_unused_claimed_candidates(snapshot, node="policy_guard")
        return {"snapshot": snapshot}

    def route_after_policy(self, state: AutomationGraphState) -> str:
        snapshot = state["snapshot"]
        if snapshot.status in {RunStatus.BLOCKED, RunStatus.FAILED}:
            return "blocked"
        if snapshot.request.workflow is WorkflowKind.FEED_ENGAGE and snapshot.selected_candidate is None:
            return "retry_candidate"
        return "execute"

    def _evaluate_candidate_policy(self, snapshot: WorkflowStateModel) -> PolicyDecision:
        candidate = snapshot.selected_candidate
        hooks = self.adapters.policy_hooks
        if candidate is None:
            return PolicyDecision()

        decisions: list[PolicyDecision] = []

        dedupe = PolicyDecision()
        if self.config.policies.enforce_dedupe and hooks:
            if hooks.has_target_tweet_id(candidate.tweet_id):
                dedupe.allowed = False
                dedupe.reasons.append("target tweet already engaged")
            elif snapshot.request.reply_text:
                dedupe_key = build_dedupe_key(
                    snapshot.request,
                    candidate=candidate,
                    text=snapshot.request.reply_text,
                )
                dedupe.dedupe_key = dedupe_key
                if hooks.has_dedupe_key(dedupe_key):
                    dedupe.allowed = False
                    dedupe.reasons.append("duplicate execution detected")
        decisions.append(dedupe)

        if hooks:
            count = hooks.get_daily_execution_count(snapshot.request.workflow, datetime.now(UTC).date())
            decisions.append(check_daily_limit(count=count, limit=self.config.policies.daily_execution_limit))
            if candidate.screen_name:
                last_author_at = hooks.get_last_author_engagement(candidate.screen_name)
                decisions.append(
                    check_cooldown(
                        last_seen_at=last_author_at,
                        cooldown_minutes=self.config.policies.per_author_cooldown_minutes,
                    )
                )

        return merge_decisions(*decisions)

    def _retry_blocked_candidate(self, snapshot: WorkflowStateModel, *, node: str = "policy_guard") -> bool:
        if snapshot.request.workflow is not WorkflowKind.FEED_ENGAGE:
            return False
        if snapshot.selected_candidate is None or not snapshot.policy.reasons:
            return False

        retryable_reasons = {
            "target tweet already engaged",
            "duplicate execution detected",
        }
        if any(reason not in retryable_reasons for reason in snapshot.policy.reasons):
            return False

        remaining_candidates = [
            candidate
            for candidate in snapshot.candidates
            if candidate.tweet_id != snapshot.selected_candidate.tweet_id
        ]
        if len(remaining_candidates) == len(snapshot.candidates):
            return False
        if not remaining_candidates:
            return False

        blocked_candidate = snapshot.selected_candidate
        blocked_reasons = list(snapshot.policy.reasons)
        if not snapshot.request.dry_run and hasattr(self.adapters.policy_hooks, "reject_candidate_cache"):
            self.adapters.policy_hooks.reject_candidate_cache(
                workflow="feed_engage",
                tweet_id=blocked_candidate.tweet_id,
                reason=", ".join(blocked_reasons),
                expires_at=(datetime.now(UTC) + timedelta(minutes=self.config.policies.candidate_cache_rejected_ttl_minutes)).isoformat(),
            )
        snapshot.candidates = remaining_candidates
        self._reset_after_candidate_rejection(snapshot)
        snapshot.log_event(
            node,
            "candidate blocked, trying next",
            tweet_id=blocked_candidate.tweet_id,
            screen_name=blocked_candidate.screen_name,
            reasons=blocked_reasons,
            remaining_candidates=len(snapshot.candidates),
        )
        return True

    def _release_unused_claimed_candidates(self, snapshot: WorkflowStateModel, *, node: str) -> int:
        if snapshot.request.workflow is not WorkflowKind.FEED_ENGAGE or snapshot.selected_candidate is None:
            return 0
        release_claims = getattr(self.adapters.policy_hooks, "release_claimed_candidate_cache", None)
        if not callable(release_claims):
            return 0
        releasable = [
            candidate
            for candidate in snapshot.candidates
            if candidate.tweet_id != snapshot.selected_candidate.tweet_id
            and isinstance(candidate.metadata, dict)
            and candidate.metadata.get("_x_atuo_candidate_cache")
            and candidate.metadata.get("_x_atuo_claim_run_id") == snapshot.run_id
        ]
        if not releasable:
            return 0
        tweet_ids = [candidate.tweet_id for candidate in releasable]
        released = release_claims(
            workflow="feed_engage",
            run_id=snapshot.run_id,
            tweet_ids=tweet_ids,
        )
        if released:
            snapshot.candidates = [snapshot.selected_candidate]
            snapshot.log_event(
                node,
                "unused claimed candidates released",
                count=released,
                tweet_ids=tweet_ids,
            )
        return released

    async def blocked(self, state: AutomationGraphState) -> AutomationGraphState:
        snapshot = state["snapshot"]
        if snapshot.status is RunStatus.RUNNING:
            snapshot.status = RunStatus.SKIPPED
        snapshot.log_event("blocked", "workflow not executed", status=snapshot.status.value)
        return {"snapshot": snapshot}

    async def execute(self, state: AutomationGraphState) -> AutomationGraphState:
        snapshot = state["snapshot"]
        started_at = perf_counter()
        if self.adapters.execute_engage is None:
            snapshot.mark_failed("execute_engage adapter not configured", node="execute")
            return {"snapshot": snapshot}
        result = await maybe_await(self.adapters.execute_engage(snapshot))
        if not snapshot.request.dry_run and snapshot.selected_candidate is not None and hasattr(self.adapters.policy_hooks, "consume_candidate_cache"):
            if result.ok:
                self.adapters.policy_hooks.consume_candidate_cache(
                    workflow="feed_engage",
                    tweet_id=snapshot.selected_candidate.tweet_id,
                )
            elif result.error:
                self.adapters.policy_hooks.reject_candidate_cache(
                    workflow="feed_engage",
                    tweet_id=snapshot.selected_candidate.tweet_id,
                    reason=result.error,
                    expires_at=(datetime.now(UTC) + timedelta(minutes=self.config.policies.candidate_cache_rejected_ttl_minutes)).isoformat(),
                )
        attempts = self._extract_execution_attempts(result)
        if (
            not result.ok
            and result.error == "No candidate succeeded"
            and self._schedule_candidate_refresh(
                snapshot,
                node="execute",
                reason="No candidate succeeded",
            )
        ):
            prior_count = len(snapshot.execution_attempt_history)
            snapshot.execution_attempt_history.extend(attempts[prior_count:])
            snapshot.log_event(
                "execute",
                "execution deferred to candidate refresh",
                duration_ms=round((perf_counter() - started_at) * 1000, 2),
                attempt_count=len(attempts),
            )
            return {"snapshot": snapshot}
        snapshot.mark_completed(
            result,
            duration_ms=round((perf_counter() - started_at) * 1000, 2),
            attempt_count=len(self._extract_execution_attempts(result)),
        )
        return {"snapshot": snapshot}

    def route_after_execute(self, state: AutomationGraphState) -> str:
        snapshot = state["snapshot"]
        if snapshot.candidate_refresh_pending:
            return "fetch_feed"
        return "finalize"

    async def finalize(self, state: AutomationGraphState) -> AutomationGraphState:
        snapshot = state["snapshot"]
        snapshot.touch()
        snapshot.log_event("finalize", "workflow finished", status=snapshot.status.value)
        return {"snapshot": snapshot}


def _build_runtime_graph(config: AutomationConfig, storage: PolicyHooks | Any, *, proxy: str | None = None) -> AutomationGraph:
    client = TwitterClient.from_config(
        config.agent_reach_config_path,
        proxy=proxy or config.twitter.proxy_url,
        twitter_bin=config.twitter.cli_bin,
        timeout=120,
    )
    service = TwitterEngageService(client)
    ai_provider = build_ai_provider(config.ai)
    if ai_provider is None:
        raise AIProviderError("feed-engage requires an AI provider")

    def fetch_feed(snapshot: WorkflowStateModel):
        started_at = perf_counter()
        options = snapshot.request.feed_options or FeedOptions()
        cleanup_candidate_cache = getattr(storage, "cleanup_candidate_cache", None)
        if cleanup_candidate_cache is not None:
            cleanup_candidate_cache()

        claim_pending_candidate_cache = getattr(storage, "claim_pending_candidate_cache", None)
        list_pending_candidate_cache = getattr(storage, "list_pending_candidate_cache", None)
        cached: list[dict[str, Any]] = []
        if claim_pending_candidate_cache is not None:
            lease_expires_at = (
                datetime.now(UTC) + timedelta(minutes=config.policies.candidate_cache_claim_ttl_minutes)
            ).isoformat()
            cached = claim_pending_candidate_cache(
                workflow="feed_engage",
                limit=options.feed_count,
                run_id=snapshot.run_id,
                lease_expires_at=lease_expires_at,
            )
        elif list_pending_candidate_cache is not None:
            cached = list_pending_candidate_cache(workflow="feed_engage", limit=options.feed_count)

        if cached:
            candidates: list[FeedCandidate] = []
            for item in cached:
                created_at_raw = item.get("created_at")
                created_at = None
                if isinstance(created_at_raw, str) and created_at_raw:
                    try:
                        created_at = datetime.fromisoformat(created_at_raw)
                    except ValueError:
                        created_at = None
                metadata = {
                    key: value
                    for key, value in dict(item.get("metadata") or {}).items()
                    if not str(key).startswith("_x_atuo_")
                }
                metadata["_x_atuo_candidate_cache"] = True
                if isinstance(item.get("claim_run_id"), str) and item.get("claim_run_id"):
                    metadata["_x_atuo_claim_run_id"] = item["claim_run_id"]
                candidates.append(
                    FeedCandidate(
                        tweet_id=str(item.get("tweet_id") or ""),
                        screen_name=str(item.get("screen_name") or "") or None,
                        text=str(item.get("text") or "") or None,
                        created_at=created_at,
                        author_verified=metadata.get("author", {}).get("verified") if isinstance(metadata.get("author"), dict) else None,
                        can_reply=item.get("can_reply", item.get("canReply")),
                        reply_limit_reason=item.get("reply_limit_reason", item.get("replyLimitReason")),
                        reply_limit_headline=item.get("reply_limit_headline", item.get("replyLimitHeadline")),
                        reply_restriction_policy=item.get("reply_restriction_policy", item.get("replyRestrictionPolicy")),
                        metadata=metadata,
                    )
                )
            snapshot.stash_runtime_observability(
                "fetch_feed",
                candidate_source="cache",
                cache_hit_count=len(candidates),
                duration_ms=round((perf_counter() - started_at) * 1000, 2),
            )
            return candidates

        tweets = service.client.fetch_feed(max_items=options.feed_count, feed_type=options.feed_type)
        candidates = [
            FeedCandidate(
                tweet_id=tweet.tweet_id,
                screen_name=tweet.screen_name,
                text=tweet.text,
                created_at=getattr(tweet, "created_at", None),
                author_verified=tweet.verified,
                can_reply=getattr(tweet, "can_reply", None),
                reply_limit_reason=getattr(tweet, "reply_limit_reason", None),
                reply_limit_headline=getattr(tweet, "reply_limit_headline", None),
                reply_restriction_policy=getattr(tweet, "reply_restriction_policy", None),
                metadata=tweet.raw,
            )
            for tweet in tweets
        ]
        with_created_at = [candidate for candidate in candidates if candidate.created_at is not None]
        without_created_at = [candidate for candidate in candidates if candidate.created_at is None]
        with_created_at.sort(key=lambda candidate: candidate.created_at, reverse=True)
        snapshot.stash_runtime_observability(
            "fetch_feed",
            candidate_source="feed",
            cache_hit_count=0,
            duration_ms=round((perf_counter() - started_at) * 1000, 2),
        )
        return [*with_created_at, *without_created_at]

    def build_hydrated_metadata(candidate: FeedCandidate, tweet: TweetRecord) -> dict[str, Any]:
        previous = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
        raw = dict(tweet.raw) if isinstance(tweet.raw, dict) else {}
        for key, value in previous.items():
            if key.startswith("_x_atuo_") and key not in raw:
                raw[key] = value
        author = raw.get("author")
        if not isinstance(author, dict):
            raw["author"] = {
                "screenName": tweet.screen_name or candidate.screen_name or "",
                "verified": tweet.verified,
            }
        raw["_x_atuo_hydrated"] = True
        return raw

    def apply_hydrated_tweet(candidate: FeedCandidate, tweet: TweetRecord) -> None:
        candidate.screen_name = tweet.screen_name or candidate.screen_name
        candidate.text = tweet.text or candidate.text
        candidate.created_at = getattr(tweet, "created_at", None) or candidate.created_at
        candidate.author_verified = tweet.verified
        candidate.can_reply = getattr(tweet, "can_reply", None)
        candidate.reply_limit_reason = getattr(tweet, "reply_limit_reason", None)
        candidate.reply_limit_headline = getattr(tweet, "reply_limit_headline", None)
        candidate.reply_restriction_policy = getattr(tweet, "reply_restriction_policy", None)
        candidate.metadata = build_hydrated_metadata(candidate, tweet)

    def hydrate_selected_candidate(candidate: FeedCandidate | None) -> FeedCandidate | None:
        if candidate is None:
            return None
        metadata = candidate.metadata if isinstance(candidate.metadata, dict) else {}
        if metadata.get("_x_atuo_hydrated"):
            return candidate
        fetch_tweet = getattr(service.client, "fetch_tweet", None)
        if fetch_tweet is None:
            return candidate
        try:
            tweet = fetch_tweet(candidate.tweet_id)
        except Exception:
            return candidate
        apply_hydrated_tweet(candidate, tweet)
        return candidate

    async def hydrate_candidates(
        snapshot: WorkflowStateModel,
        candidates: list[FeedCandidate],
        *,
        node: str,
    ) -> dict[str, Any]:
        hydration_started_at = perf_counter()
        reused_hydrated_count = 0

        async def hydrate_candidate(candidate: FeedCandidate) -> tuple[FeedCandidate, TweetRecord | None, Exception | None]:
            metadata = candidate.metadata if isinstance(candidate.metadata, dict) else {}
            if metadata.get("_x_atuo_hydrated"):
                return candidate, None, None
            try:
                tweet = await asyncio.to_thread(service.client.fetch_tweet, candidate.tweet_id)
                return candidate, tweet, None
            except Exception as exc:
                return candidate, None, exc

        hydrated = await asyncio.gather(*(hydrate_candidate(candidate) for candidate in candidates))
        for candidate, tweet, exc in hydrated:
            if exc is not None:
                snapshot.log_event(
                    node,
                    "candidate hydration failed before selection, using preview",
                    tweet_id=candidate.tweet_id,
                    error=str(exc),
                )
                continue
            if tweet is None:
                metadata = candidate.metadata if isinstance(candidate.metadata, dict) else {}
                if metadata.get("_x_atuo_hydrated"):
                    reused_hydrated_count += 1
                continue
            apply_hydrated_tweet(candidate, tweet)

        return {
            "hydrated_count": len(candidates),
            "reused_hydrated_count": reused_hydrated_count,
            "hydration_duration_ms": _elapsed_ms(hydration_started_at),
        }

    async def draft_reply(snapshot: WorkflowStateModel):
        started_at = perf_counter()
        candidate = snapshot.selected_candidate
        if candidate is not None:
            await asyncio.to_thread(hydrate_selected_candidate, candidate)
        reply_context = snapshot.reply_context if isinstance(snapshot.reply_context, dict) else {}
        reply_style = _rule_based_reply_style(candidate)
        style_reason = "rule_based" if reply_style is not None else ""
        style_duration_ms: float | None = None
        if reply_style is None and candidate is not None:
            classifier = getattr(ai_provider, "classify_reply_style", None)
            if classifier is not None:
                style_started_at = perf_counter()
                try:
                    decision = await asyncio.to_thread(classifier, candidate)
                    reply_style = getattr(decision, "style", None) or "technical"
                    style_reason = getattr(decision, "reason", "") or "ai_classified"
                except AIProviderError as exc:
                    snapshot.log_event("draft_reply", "reply style classification failed", error=str(exc))
                    reply_style = "technical"
                    style_reason = "classification_failed"
                finally:
                    style_duration_ms = _elapsed_ms(style_started_at)
        if reply_style is None:
            reply_style = "technical"
            style_reason = "default"
        snapshot.selection_reason = _normalized_selection_reason(
            candidate,
            snapshot.selection_reason,
            reply_style,
        )
        snapshot.reply_context["reply_style"] = reply_style
        snapshot.log_event(
            "draft_reply",
            "reply style selected",
            reply_style=reply_style,
            reason=style_reason,
        )
        if snapshot.selected_candidate is None:
            raise AIProviderError("draft requested without a selected candidate")
        draft_started_at = perf_counter()
        draft = await asyncio.to_thread(
            _call_with_optional_context,
            ai_provider.draft_reply,
            snapshot.selected_candidate,
            {"reply_style": reply_style},
        )
        snapshot.drafting_source = "ai"
        payload = {
            "rationale": draft.rationale,
            "duration_ms": _elapsed_ms(started_at),
            "ai_draft_duration_ms": _elapsed_ms(draft_started_at),
            "ai_draft_input_bytes": _draft_payload_bytes(snapshot.selected_candidate),
        }
        if style_duration_ms is not None:
            payload["ai_style_classification_duration_ms"] = style_duration_ms
        snapshot.log_event("draft_reply", "base ai draft generated", **payload)
        return draft.text

    async def select_candidate(snapshot: WorkflowStateModel):
        moderate = getattr(ai_provider, "moderate_candidates", None)
        if moderate is None:
            raise AIProviderError("feed-engage requires ai moderation adapter")

        original_candidates = list(snapshot.candidates)
        if not original_candidates:
            snapshot.stash_runtime_observability("select_candidate")
            return None

        total_hydrated_count = 0
        total_reused_hydrated_count = 0
        total_hydration_duration_ms = 0.0
        ai_moderation_duration_total = 0.0
        ai_moderation_candidate_count = 0
        ai_moderation_cache_hits = 0
        ai_moderation_input_bytes = 0
        risky_candidates: list[FeedCandidate] = []
        reject_candidate_cache = getattr(storage, "reject_candidate_cache", None)

        def persist_candidate_pool() -> None:
            if (
                snapshot.request.dry_run
                or snapshot.request.workflow is not WorkflowKind.FEED_ENGAGE
                or snapshot.candidate_cache_persisted
                or not snapshot.candidates
                or not hasattr(storage, "upsert_candidate_cache_entries")
            ):
                return
            expires_at = (datetime.now(UTC) + timedelta(minutes=config.policies.candidate_cache_pending_ttl_minutes)).isoformat()
            storage.upsert_candidate_cache_entries(
                workflow="feed_engage",
                source_run_id=snapshot.run_id,
                candidates=[candidate.model_dump(mode="json") for candidate in snapshot.candidates],
                expires_at=expires_at,
            )
            snapshot.candidate_cache_persisted = True
            snapshot.log_event(
                "select_candidate",
                "candidate pool cached",
                count=len(snapshot.candidates),
                expires_at=expires_at,
            )

        async def hydrate_one(candidate: FeedCandidate) -> None:
            nonlocal total_hydrated_count
            nonlocal total_reused_hydrated_count
            nonlocal total_hydration_duration_ms
            metrics = await hydrate_candidates(snapshot, [candidate], node="select_candidate")
            total_hydrated_count += int(metrics.get("hydrated_count", 0))
            total_reused_hydrated_count += int(metrics.get("reused_hydrated_count", 0))
            total_hydration_duration_ms += float(metrics.get("hydration_duration_ms", 0.0))

        def candidate_reply_disposition(candidate: FeedCandidate) -> str:
            if candidate.can_reply is False:
                snapshot.log_event(
                    "select_candidate",
                    "candidate removed after hydration",
                    tweet_id=candidate.tweet_id,
                    screen_name=candidate.screen_name,
                    reason=(candidate.reply_limit_reason or candidate.reply_limit_headline or "reply restricted"),
                )
                return "skip"
            if candidate.reply_restriction_policy:
                snapshot.log_event(
                    "select_candidate",
                    "candidate de-prioritized after hydration",
                    tweet_id=candidate.tweet_id,
                    screen_name=candidate.screen_name,
                    policy=candidate.reply_restriction_policy,
                    reason=(candidate.reply_limit_reason or _reply_control_reason(candidate.reply_restriction_policy)),
                )
                return "risky"
            return "eligible"

        async def moderate_one(candidate: FeedCandidate) -> AIModerationResult | None:
            nonlocal ai_moderation_duration_total
            nonlocal ai_moderation_candidate_count
            nonlocal ai_moderation_cache_hits
            nonlocal ai_moderation_input_bytes
            if _is_moderation_exempt_candidate(candidate):
                return None
            cached = _cached_ai_moderation(
                candidate,
                provider_name=config.ai.provider,
                model_name=config.ai.model,
            )
            if cached is not None:
                ai_moderation_cache_hits += 1
                return cached
            moderation_started_at = perf_counter()
            ai_moderation_candidate_count += 1
            ai_moderation_input_bytes += _moderation_payload_bytes([candidate])
            results = list(await asyncio.to_thread(moderate, [candidate]))
            ai_moderation_duration_total += perf_counter() - moderation_started_at
            if not results:
                moderation = AIModerationResult(
                    tweet_id=candidate.tweet_id,
                    allowed=False,
                    category="missing_decision",
                    reason="selected candidate missing from ai moderation results",
                )
                _store_ai_moderation(
                    candidate,
                    moderation,
                    provider_name=config.ai.provider,
                    model_name=config.ai.model,
                )
                return moderation
            match = next((result for result in results if result.tweet_id == candidate.tweet_id), None)
            if match is None:
                moderation = AIModerationResult(
                    tweet_id=candidate.tweet_id,
                    allowed=False,
                    category="missing_decision",
                    reason="selected candidate missing from ai moderation results",
                )
                _store_ai_moderation(
                    candidate,
                    moderation,
                    provider_name=config.ai.provider,
                    model_name=config.ai.model,
                )
                return moderation
            _store_ai_moderation(
                candidate,
                match,
                provider_name=config.ai.provider,
                model_name=config.ai.model,
            )
            return match

        def stash_metrics() -> None:
            snapshot.stash_runtime_observability(
                "select_candidate",
                hydrated_count=total_hydrated_count,
                reused_hydrated_count=total_reused_hydrated_count,
                hydration_duration_ms=round(total_hydration_duration_ms, 2),
                ai_moderation_duration_ms=round(ai_moderation_duration_total * 1000, 2),
                ai_moderation_candidate_count=ai_moderation_candidate_count,
                ai_moderation_cache_hits=ai_moderation_cache_hits,
                ai_moderation_input_bytes=ai_moderation_input_bytes,
            )

        for index, candidate in enumerate(original_candidates):
            await hydrate_one(candidate)
            disposition = candidate_reply_disposition(candidate)
            if disposition == "skip":
                continue
            if disposition == "risky":
                risky_candidates.append(candidate)
                continue

            moderation = await moderate_one(candidate)
            if moderation is not None and not moderation.allowed:
                snapshot.log_event(
                    "select_candidate",
                    "candidate filtered by ai moderation",
                    tweet_id=candidate.tweet_id,
                    screen_name=candidate.screen_name,
                    category=moderation.category,
                    reason=moderation.reason,
                )
                if not snapshot.request.dry_run and callable(reject_candidate_cache):
                    reject_candidate_cache(
                        workflow="feed_engage",
                        tweet_id=candidate.tweet_id,
                        reason=moderation.reason,
                        expires_at=(datetime.now(UTC) + timedelta(minutes=config.policies.candidate_cache_rejected_ttl_minutes)).isoformat(),
                    )
                continue

            snapshot.selected_candidate = candidate
            snapshot.selection_source = "ordered_candidates"
            snapshot.selection_reason = "first ordered candidate passed sequential validation"
            snapshot.candidates = [candidate, *original_candidates[index + 1 :], *risky_candidates]
            persist_candidate_pool()
            stash_metrics()
            return candidate

        for index, candidate in enumerate(risky_candidates):
            moderation = await moderate_one(candidate)
            if moderation is not None and not moderation.allowed:
                snapshot.log_event(
                    "select_candidate",
                    "candidate filtered by ai moderation",
                    tweet_id=candidate.tweet_id,
                    screen_name=candidate.screen_name,
                    category=moderation.category,
                    reason=moderation.reason,
                )
                if not snapshot.request.dry_run and callable(reject_candidate_cache):
                    reject_candidate_cache(
                        workflow="feed_engage",
                        tweet_id=candidate.tweet_id,
                        reason=moderation.reason,
                        expires_at=(datetime.now(UTC) + timedelta(minutes=config.policies.candidate_cache_rejected_ttl_minutes)).isoformat(),
                    )
                continue

            snapshot.selected_candidate = candidate
            snapshot.selection_source = "ordered_candidates"
            snapshot.selection_reason = "first ordered candidate passed sequential validation"
            snapshot.candidates = [candidate, *risky_candidates[index + 1 :]]
            persist_candidate_pool()
            stash_metrics()
            return candidate

        snapshot.candidates = []
        stash_metrics()
        return None

    def execute_engage(snapshot: WorkflowStateModel):
        candidate = snapshot.selected_candidate
        if candidate is None:
            return ExecutionResult(action="engage", ok=False, dry_run=snapshot.request.dry_run, error="candidate missing")
        result = service.engage_candidates(
            [
                Candidate(
                    tweet_id=candidate.tweet_id,
                    screen_name=candidate.screen_name or "",
                    reply_text=snapshot.rendered_text or snapshot.request.reply_text or "",
                    tweet=(
                        TweetRecord.from_payload(candidate.metadata)
                        if isinstance(candidate.metadata, dict)
                        and candidate.metadata.get("id")
                        and candidate.metadata.get("author")
                        else None
                    ),
                )
            ],
            dry_run=snapshot.request.dry_run,
        )
        if result.selected_candidate is not None:
            snapshot.selected_candidate = FeedCandidate(
                tweet_id=result.selected_candidate.tweet_id,
                screen_name=result.selected_candidate.screen_name,
                text=candidate.text,
                created_at=candidate.created_at,
                author_verified=candidate.author_verified,
                can_reply=candidate.can_reply,
                reply_limit_reason=candidate.reply_limit_reason,
                reply_limit_headline=candidate.reply_limit_headline,
                metadata=candidate.metadata,
            )
        target_author = result.selected_candidate.screen_name if result.selected_candidate else None
        target_tweet_id = result.selected_candidate.tweet_id if result.selected_candidate else None
        target_tweet_url = (
            f"https://x.com/{target_author}/status/{target_tweet_id}"
            if target_author and target_tweet_id
            else None
        )
        reply_url = (
            f"https://x.com/i/status/{result.reply_result.tweet_id}"
            if result.reply_result and result.reply_result.tweet_id
            else None
        )
        attempts = [
            {
                "tweet_id": attempt.tweet_id,
                "screen_name": attempt.screen_name,
                "outcome": attempt.outcome,
                "detail": attempt.detail,
            }
            for attempt in result.attempts
        ]
        combined_attempts = [*snapshot.execution_attempt_history, *attempts]
        return ExecutionResult(
            action="engage",
            ok=result.ok,
            dry_run=snapshot.request.dry_run,
            target_tweet_id=target_tweet_id,
            target_tweet_url=target_tweet_url,
            created_tweet_id=result.reply_result.tweet_id if result.reply_result else None,
            reply_url=reply_url,
            followed=bool(result.follow_result and result.follow_result.ok),
            error=result.error,
            detail={
                "status": result.status,
                "selected_by": snapshot.selection_source or "unknown",
                "selection_reason": snapshot.selection_reason,
                "drafted_by": snapshot.drafting_source or "unknown",
                "attempts": combined_attempts,
            },
        )

    return AutomationGraph(
        config,
        WorkflowAdapters(
            fetch_feed=fetch_feed,
            select_candidate=select_candidate,
            draft_reply=draft_reply,
            execute_engage=execute_engage,
            policy_hooks=storage,
        ),
    )


def _persist_snapshot(storage: Any, snapshot: WorkflowStateModel) -> None:
    if snapshot.policy.dedupe_key and getattr(storage, "store_dedupe_key", None):
        expires_at = (datetime.now(UTC) + timedelta(minutes=240)).isoformat()
        storage.store_dedupe_key(snapshot.policy.dedupe_key, snapshot.request.workflow.value, expires_at)
    if snapshot.result and snapshot.selected_candidate and getattr(storage, "record_engagement", None):
        storage.record_engagement(
            run_id=snapshot.run_id,
            target_tweet_id=snapshot.selected_candidate.tweet_id,
            target_author=snapshot.selected_candidate.screen_name,
            target_tweet_url=snapshot.result.target_tweet_url,
            reply_tweet_id=snapshot.result.created_tweet_id,
            reply_url=snapshot.result.reply_url,
            followed=bool(snapshot.result.followed),
        )


async def _run_request(request: AutomationRequest, *, storage: Any, proxy: str | None = None) -> dict[str, Any]:
    config = AutomationConfig()
    graph = _build_runtime_graph(config, storage, proxy=proxy)
    snapshot = await graph.invoke(request)
    _persist_snapshot(storage, snapshot)
    return {
        "status": snapshot.status.value,
        "run_id": snapshot.run_id,
        "result": snapshot.result.model_dump(mode="json") if snapshot.result else None,
        "candidate_refresh_count": snapshot.candidate_refresh_count,
        "selected_candidate": snapshot.selected_candidate.model_dump(mode="json") if snapshot.selected_candidate else None,
        "rendered_text": snapshot.rendered_text,
        "selection_source": snapshot.selection_source,
        "selection_reason": snapshot.selection_reason,
        "drafted_by": snapshot.drafting_source,
        "errors": snapshot.errors,
        "events": [event.model_dump(mode="json") for event in snapshot.events],
    }


def build_request_binding(
    function_name: str,
    *,
    run_id: str,
    job_id: str,
    payload: dict[str, Any],
) -> tuple[AutomationRequest, str | None] | None:
    config = AutomationConfig()
    if function_name != "run_feed_engage":
        return None
    request = AutomationRequest.for_feed_engage(
        job_name=job_id,
        run_id=run_id,
        dry_run=bool(payload.get("dry_run", False)),
        approval_mode=payload.get("mode", "ai_auto"),
        reply_text=payload.get("reply_text") or payload.get("reply_template"),
        feed_options=FeedOptions(
            feed_type=payload.get("feed_type", config.twitter.default_feed_type),
            feed_count=payload.get("feed_count", config.twitter.default_feed_count),
        ),
        metadata=payload.get("metadata", {}),
        idempotency_key=payload.get("idempotency_key"),
    )
    return request, payload.get("proxy") or config.twitter.proxy_url


async def run_feed_engage(*, run_id: str, job_id: str, payload: dict[str, Any], storage: Any) -> dict[str, Any]:
    request, proxy = build_request_binding("run_feed_engage", run_id=run_id, job_id=job_id, payload=payload) or (
        None,
        None,
    )
    if request is None:
        raise ValueError("unsupported function binding: run_feed_engage")
    return await _run_request(request, storage=storage, proxy=proxy)
