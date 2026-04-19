"""LangGraph workflow definitions for automation runs."""

from __future__ import annotations

import asyncio
import inspect
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from html import unescape
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import quote_plus, unquote
from urllib.request import Request, urlopen

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
    RepoContext,
    RunStatus,
    WorkflowKind,
    WorkflowStateModel,
    make_initial_state,
)
from x_atuo.core.ai_client import (
    AIEnhancementDecision,
    AIReplyContextPlan,
    AIModerationResult,
    AIProviderError,
    build_ai_provider,
    compose_reply_text,
)
from x_atuo.core.github_repo_client import fetch_repo_context as fetch_github_repo_context
from x_atuo.core.github_repo_client import render_repo_post_text
from x_atuo.core.twitter_client import TwitterClient
from x_atuo.core.twitter_engage_service import TwitterEngageService
from x_atuo.core.twitter_models import Candidate, TweetRecord

StateCallable = Callable[[WorkflowStateModel], Any]


async def maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def fetch_repo_context(repo_url: str) -> dict[str, Any]:
    context = fetch_github_repo_context(repo_url)
    return {
        "repo_url": context.repo_url,
        "repo_name": context.repo_name,
        "description": context.description,
        "readme_excerpt": context.readme_excerpt,
        "stars": context.stars,
        "metadata": context.metadata or {},
    }


def _strip_html_tags(value: str) -> str:
    return re.sub(r"<[^>]+>", " ", value)


def search_web_context(query: str, max_results: int = 3) -> list[dict[str, str]]:
    if not query.strip():
        return []
    request = Request(
        f"https://html.duckduckgo.com/html/?q={quote_plus(query)}",
        headers={"User-Agent": "Mozilla/5.0"},
    )
    try:
        with urlopen(request, timeout=10) as response:
            html = response.read().decode("utf-8", errors="ignore")
    except (HTTPError, URLError, TimeoutError):
        return []

    pattern = re.compile(
        r'<a[^>]+class="result__a"[^>]+href="(?P<url>[^"]+)"[^>]*>(?P<title>.*?)</a>(?P<body>.*?)(?:</div>|<a[^>]+class="result__a")',
        re.DOTALL,
    )
    results: list[dict[str, str]] = []
    for match in pattern.finditer(html):
        body = match.group("body")
        snippet_match = re.search(r'class="result__snippet"[^>]*>(?P<snippet>.*?)</', body, re.DOTALL)
        snippet = unescape(_strip_html_tags(snippet_match.group("snippet"))).strip() if snippet_match else ""
        title = unescape(_strip_html_tags(match.group("title"))).strip()
        url = unquote(match.group("url"))
        if url.startswith("//duckduckgo.com/l/?uddg="):
            url = url.split("uddg=", 1)[-1].split("&", 1)[0]
            url = unquote(url)
        results.append({"title": title, "snippet": snippet, "url": url})
        if len(results) >= max_results:
            break
    return results


def _tweet_to_context(tweet: TweetRecord) -> dict[str, Any]:
    author = getattr(tweet, "author", None)
    return {
        "tweet_id": tweet.tweet_id,
        "screen_name": tweet.screen_name,
        "text": tweet.text,
        "verified": tweet.verified,
        "author_name": getattr(author, "name", None),
        "url": f"https://x.com/{tweet.screen_name}/status/{tweet.tweet_id}" if tweet.screen_name else None,
    }


def _summarize_replies(replies: list[TweetRecord]) -> str:
    lines: list[str] = []
    seen: set[str] = set()
    for reply in replies:
        text = (reply.text or "").strip()
        if not text:
            continue
        normalized = text.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        handle = f"@{reply.screen_name}" if reply.screen_name else "reply"
        lines.append(f"{handle}: {text}")
        if len(lines) >= 3:
            break
    return " | ".join(lines)


def _summarize_author_pattern(posts: list[dict[str, Any]]) -> str | None:
    snippets = [str(post.get("text") or "").strip() for post in posts if str(post.get("text") or "").strip()]
    if not snippets:
        return None
    summary = " ".join(snippets[:3])
    return summary[:280].strip()


def _compact_reply_context(context: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(context, dict) or not context:
        return context

    target_post = context.get("target_post") if isinstance(context.get("target_post"), dict) else {}
    author_profile = context.get("author_profile") if isinstance(context.get("author_profile"), dict) else {}
    reply_brief = context.get("reply_brief") if isinstance(context.get("reply_brief"), dict) else {}
    evidence = context.get("knowledge_evidence") if isinstance(context.get("knowledge_evidence"), list) else []
    compact: dict[str, Any] = {
        "target_text": str(target_post.get("text") or "")[:400],
        "author_profile_compact": {
            "screen_name": author_profile.get("screen_name"),
            "name": author_profile.get("name"),
            "verified": author_profile.get("verified"),
            "description": author_profile.get("description"),
        },
        "author_pattern": _summarize_author_pattern(
            context.get("author_recent_posts") if isinstance(context.get("author_recent_posts"), list) else []
        ),
        "existing_replies_summary": str(context.get("existing_replies_summary") or "")[:280],
        "knowledge_evidence_top1": evidence[0] if evidence else None,
        "reply_brief": {
            "acknowledgment": reply_brief.get("acknowledgment"),
            "fuller_angle": reply_brief.get("fuller_angle"),
            "rationale": reply_brief.get("rationale"),
        },
    }
    return compact


def _compose_fallback_reply_from_tweet(tweet_text: str | None, *, max_length: int = 280) -> str:
    normalized = re.sub(r"\s+", " ", str(tweet_text or "")).strip()
    normalized = re.sub(r"https?://\S+", "", normalized).strip()
    normalized = normalized.rstrip(" .!?")
    if not normalized:
        return "The real win here is how much complexity this strips out."

    suffix = " The real test is whether it holds up in production."
    if len(normalized) + len(suffix) + 1 <= max_length:
        return f"{normalized}.{suffix}"

    available = max_length - len(suffix)
    if available <= 1:
        return suffix[:max_length].rstrip()
    clipped = normalized[: available - 1].rstrip(" ,.;:") + "…"
    return f"{clipped}{suffix}"


def _call_with_optional_context(method: Callable[..., Any], *args: Any) -> Any:
    try:
        params = inspect.signature(method).parameters
    except (TypeError, ValueError):
        return method(*args[:1])
    if len(params) >= len(args):
        return method(*args)
    return method(*args[:1])


@dataclass(slots=True)
class WorkflowAdapters:
    fetch_feed: StateCallable | None = None
    moderate_candidates: StateCallable | None = None
    select_candidate: StateCallable | None = None
    plan_reply_strategy: StateCallable | None = None
    enrich_reply_context: StateCallable | None = None
    load_repo_context: StateCallable | None = None
    draft_reply: StateCallable | None = None
    draft_post: StateCallable | None = None
    execute_engage: StateCallable | None = None
    execute_post: StateCallable | None = None
    policy_hooks: PolicyHooks | None = None


class AutomationGraph:
    """Compiled LangGraph workflows with lightweight orchestration logic."""

    def __init__(self, config: AutomationConfig, adapters: WorkflowAdapters | None = None) -> None:
        self.config = config
        self.adapters = adapters or WorkflowAdapters()
        self.graph = self._build_graph()

    async def invoke(self, request: AutomationRequest) -> WorkflowStateModel:
        state = make_initial_state(request)
        try:
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
        workflow.add_node("moderate_candidates", self.moderate_candidates)
        workflow.add_node("select_candidate", self.select_candidate)
        workflow.add_node("candidate_policy_guard", self.candidate_policy_guard)
        workflow.add_node("plan_reply_strategy", self.plan_reply_strategy)
        workflow.add_node("enrich_reply_context", self.enrich_reply_context)
        workflow.add_node("load_repo_context", self.load_repo_context)
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
                "select_candidate": "select_candidate",
                "load_repo_context": "load_repo_context",
                "draft_text": "draft_text",
            },
        )
        workflow.add_conditional_edges(
            "fetch_feed",
            self.route_after_fetch_feed,
            {
                "moderate_candidates": "moderate_candidates",
                "select_candidate": "select_candidate",
                "finalize": "finalize",
            },
        )
        workflow.add_conditional_edges(
            "moderate_candidates",
            self.route_after_moderation,
            {
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
                "plan_reply_strategy": "plan_reply_strategy",
                "blocked": "blocked",
            },
        )
        workflow.add_conditional_edges(
            "plan_reply_strategy",
            self.route_after_reply_strategy,
            {
                "enrich_reply_context": "enrich_reply_context",
                "draft_text": "draft_text",
                "blocked": "blocked",
            },
        )
        workflow.add_edge("enrich_reply_context", "draft_text")
        workflow.add_edge("load_repo_context", "draft_text")
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
        workflow.add_edge("execute", "finalize")
        workflow.add_edge("finalize", END)
        return workflow.compile()

    async def prepare(self, state: AutomationGraphState) -> AutomationGraphState:
        snapshot = state["snapshot"]
        snapshot.status = RunStatus.RUNNING
        if snapshot.request.candidates:
            snapshot.candidates = list(snapshot.request.candidates)
        if snapshot.request.metadata.get("frozen_selection_source"):
            snapshot.selection_source = str(snapshot.request.metadata["frozen_selection_source"])
        if snapshot.request.metadata.get("frozen_selection_reason"):
            snapshot.selection_reason = str(snapshot.request.metadata["frozen_selection_reason"])
        if snapshot.request.metadata.get("frozen_drafted_by"):
            snapshot.drafting_source = str(snapshot.request.metadata["frozen_drafted_by"])
        snapshot.log_event("prepare", "request prepared")
        return {"snapshot": snapshot}

    def route_after_prepare(self, state: AutomationGraphState) -> str:
        workflow = state["snapshot"].request.workflow
        if workflow is WorkflowKind.FEED_ENGAGE:
            return "fetch_feed"
        if workflow is WorkflowKind.REPO_POST:
            return "load_repo_context"
        return "draft_text"

    async def fetch_feed(self, state: AutomationGraphState) -> AutomationGraphState:
        snapshot = state["snapshot"]
        if snapshot.candidates:
            snapshot.log_event("fetch_feed", "using preloaded candidates", count=len(snapshot.candidates))
            return {"snapshot": snapshot}
        if self.adapters.fetch_feed is None:
            snapshot.mark_failed("feed-engage requires fetch_feed adapter", node="fetch_feed")
            return {"snapshot": snapshot}
        fetched = await maybe_await(self.adapters.fetch_feed(snapshot))
        snapshot.candidates = list(fetched or [])
        snapshot.log_event("fetch_feed", "feed fetched", count=len(snapshot.candidates))
        if not snapshot.candidates:
            snapshot.mark_failed("feed returned no candidates", node="fetch_feed")
        return {"snapshot": snapshot}

    def route_after_fetch_feed(self, state: AutomationGraphState) -> str:
        snapshot = state["snapshot"]
        if snapshot.status is RunStatus.FAILED:
            return "finalize"
        if self.adapters.moderate_candidates is not None:
            return "moderate_candidates"
        return "select_candidate"

    async def moderate_candidates(self, state: AutomationGraphState) -> AutomationGraphState:
        snapshot = state["snapshot"]
        if snapshot.status is RunStatus.FAILED:
            return {"snapshot": snapshot}
        if self.adapters.moderate_candidates is None or not snapshot.candidates:
            return {"snapshot": snapshot}
        try:
            moderation_results = await maybe_await(self.adapters.moderate_candidates(snapshot))
        except AIProviderError as exc:
            snapshot.mark_blocked([f"ai moderation failed: {exc}"], node="moderate_candidates")
            return {"snapshot": snapshot}
        moderation_by_tweet_id = {result.tweet_id: result for result in moderation_results or []}
        filtered_candidates: list[FeedCandidate] = []
        filtered_count = 0
        for candidate in snapshot.candidates:
            moderation = moderation_by_tweet_id.get(candidate.tweet_id)
            if moderation is None:
                filtered_count += 1
                snapshot.log_event(
                    "moderate_candidates",
                    "candidate filtered by ai moderation",
                    tweet_id=candidate.tweet_id,
                    screen_name=candidate.screen_name,
                    category="missing_decision",
                    reason="candidate missing from ai moderation results",
                )
                continue
            if not moderation.allowed:
                filtered_count += 1
                snapshot.log_event(
                    "moderate_candidates",
                    "candidate filtered by ai moderation",
                    tweet_id=candidate.tweet_id,
                    screen_name=candidate.screen_name,
                    category=moderation.category,
                    reason=moderation.reason,
                )
                continue
            filtered_candidates.append(candidate)
        snapshot.candidates = filtered_candidates
        snapshot.log_event(
            "moderate_candidates",
            "ai moderation completed",
            kept=len(filtered_candidates),
            filtered=filtered_count,
        )
        if not snapshot.candidates:
            snapshot.mark_blocked(["all candidates filtered by ai moderation"], node="moderate_candidates")
        return {"snapshot": snapshot}

    def route_after_moderation(self, state: AutomationGraphState) -> str:
        snapshot = state["snapshot"]
        if snapshot.status is RunStatus.BLOCKED:
            return "blocked"
        if snapshot.status is RunStatus.FAILED:
            return "finalize"
        return "select_candidate"

    async def select_candidate(self, state: AutomationGraphState) -> AutomationGraphState:
        snapshot = state["snapshot"]
        if snapshot.status is RunStatus.FAILED:
            return {"snapshot": snapshot}
        if snapshot.selected_candidate is None:
            if self.adapters.select_candidate is not None:
                selected = await maybe_await(self.adapters.select_candidate(snapshot))
                if selected is not None:
                    snapshot.selected_candidate = selected
            elif snapshot.request.candidate is not None:
                snapshot.selected_candidate = snapshot.request.candidate
            elif snapshot.candidates:
                snapshot.selected_candidate = snapshot.candidates[0]
        if snapshot.selected_candidate is None:
            snapshot.mark_failed("no candidate available for engagement", node="select_candidate")
        else:
            if snapshot.selection_source is None:
                snapshot.selection_source = "deterministic"
            snapshot.log_event(
                "select_candidate",
                "candidate selected",
                tweet_id=snapshot.selected_candidate.tweet_id,
                screen_name=snapshot.selected_candidate.screen_name,
                selected_by=snapshot.selection_source,
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
        return "plan_reply_strategy"

    async def plan_reply_strategy(self, state: AutomationGraphState) -> AutomationGraphState:
        snapshot = state["snapshot"]
        if snapshot.status is RunStatus.FAILED:
            return {"snapshot": snapshot}
        if snapshot.request.workflow is not WorkflowKind.FEED_ENGAGE or snapshot.selected_candidate is None:
            return {"snapshot": snapshot}
        if snapshot.request.reply_text:
            snapshot.reply_context = {}
            return {"snapshot": snapshot}

        reply_context = dict(snapshot.reply_context) if isinstance(snapshot.reply_context, dict) else {}
        reply_context["should_enrich"] = False
        reply_context["enhancement_reason"] = "base reply is the default path"

        if self.adapters.plan_reply_strategy is not None:
            try:
                decision = await maybe_await(self.adapters.plan_reply_strategy(snapshot))
                reply_context["should_enrich"] = bool(getattr(decision, "should_enrich", False))
                reply_context["enhancement_reason"] = str(getattr(decision, "reason", "") or "")
            except AIProviderError as exc:
                snapshot.log_event("plan_reply_strategy", "enhancement decision failed", error=str(exc))

        reply_context["base_reply_text"] = _compose_fallback_reply_from_tweet(
            snapshot.selected_candidate.text if snapshot.selected_candidate is not None else None,
            max_length=self.config.policies.max_reply_length,
        )

        snapshot.reply_context = reply_context
        snapshot.log_event(
            "plan_reply_strategy",
            "reply strategy planned",
            should_enrich=bool(reply_context.get("should_enrich")),
            reason=reply_context.get("enhancement_reason"),
        )
        return {"snapshot": snapshot}

    def route_after_reply_strategy(self, state: AutomationGraphState) -> str:
        snapshot = state["snapshot"]
        if snapshot.status in {RunStatus.BLOCKED, RunStatus.FAILED}:
            return "blocked"
        if self._should_enrich_reply_context(snapshot):
            return "enrich_reply_context"
        return "draft_text"

    async def enrich_reply_context(self, state: AutomationGraphState) -> AutomationGraphState:
        snapshot = state["snapshot"]
        if snapshot.status is RunStatus.FAILED:
            return {"snapshot": snapshot}
        if not self._should_enrich_reply_context(snapshot):
            return {"snapshot": snapshot}
        existing = dict(snapshot.reply_context) if isinstance(snapshot.reply_context, dict) else {}
        enriched = await maybe_await(self.adapters.enrich_reply_context(snapshot)) or {}
        snapshot.reply_context = {**existing, **enriched}
        if snapshot.reply_context:
            snapshot.log_event(
                "enrich_reply_context",
                "reply context ready",
                has_search=bool(snapshot.reply_context.get("knowledge_evidence")),
                author_posts=len(snapshot.reply_context.get("author_recent_posts") or []),
            )
        return {"snapshot": snapshot}

    async def load_repo_context(self, state: AutomationGraphState) -> AutomationGraphState:
        snapshot = state["snapshot"]
        if snapshot.status is RunStatus.FAILED:
            return {"snapshot": snapshot}
        if self.adapters.load_repo_context is not None:
            snapshot.repo_context = await maybe_await(self.adapters.load_repo_context(snapshot))
        else:
            repo_url = snapshot.request.repo_url or ""
            repo_name = repo_url.rstrip("/").split("/")[-1] if repo_url else None
            snapshot.repo_context = RepoContext(
                repo_url=repo_url,
                repo_name=repo_name,
                description="repo-post adapter not configured",
            )
        snapshot.log_event("load_repo_context", "repo context ready", repo_url=snapshot.repo_context.repo_url)
        return {"snapshot": snapshot}

    async def draft_text(self, state: AutomationGraphState) -> AutomationGraphState:
        snapshot = state["snapshot"]
        if snapshot.status is RunStatus.FAILED:
            return {"snapshot": snapshot}
        workflow = snapshot.request.workflow
        if workflow is WorkflowKind.FEED_ENGAGE:
            if snapshot.request.reply_text:
                snapshot.rendered_text = snapshot.request.reply_text
            elif self.adapters.draft_reply is not None:
                snapshot.rendered_text = await maybe_await(self.adapters.draft_reply(snapshot))
            else:
                snapshot.rendered_text = "Interesting work here."
        elif workflow is WorkflowKind.REPO_POST:
            if snapshot.request.post_text:
                snapshot.rendered_text = snapshot.request.post_text
            elif self.adapters.draft_post is not None:
                snapshot.rendered_text = await maybe_await(self.adapters.draft_post(snapshot))
            elif snapshot.repo_context is not None:
                repo_name = snapshot.repo_context.repo_name or snapshot.repo_context.repo_url
                description = snapshot.repo_context.description or "Open-source repository"
                snapshot.rendered_text = f"{repo_name}: {description}"
            else:
                snapshot.mark_failed("repo-post requires repo context", node="draft_text")
        else:
            snapshot.rendered_text = snapshot.request.post_text
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
        return {"snapshot": snapshot}

    def route_after_policy(self, state: AutomationGraphState) -> str:
        snapshot = state["snapshot"]
        if snapshot.status in {RunStatus.BLOCKED, RunStatus.FAILED}:
            return "blocked"
        if snapshot.request.workflow is WorkflowKind.FEED_ENGAGE and snapshot.selected_candidate is None:
            return "retry_candidate"
        return "execute"

    def _should_enrich_reply_context(self, snapshot: WorkflowStateModel) -> bool:
        if snapshot.request.workflow is not WorkflowKind.FEED_ENGAGE:
            return False
        if snapshot.selected_candidate is None or self.adapters.enrich_reply_context is None:
            return False
        return bool(snapshot.reply_context.get("should_enrich"))

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
        snapshot.candidates = remaining_candidates
        snapshot.selected_candidate = None
        snapshot.selection_source = None
        snapshot.selection_reason = None
        snapshot.reply_context = {}
        snapshot.rendered_text = None
        snapshot.drafting_source = None
        snapshot.policy = PolicyDecision()
        snapshot.log_event(
            node,
            "candidate blocked, trying next",
            tweet_id=blocked_candidate.tweet_id,
            screen_name=blocked_candidate.screen_name,
            reasons=blocked_reasons,
            remaining_candidates=len(snapshot.candidates),
        )
        return True

    async def blocked(self, state: AutomationGraphState) -> AutomationGraphState:
        snapshot = state["snapshot"]
        if snapshot.status is RunStatus.RUNNING:
            snapshot.status = RunStatus.SKIPPED
        snapshot.log_event("blocked", "workflow not executed", status=snapshot.status.value)
        return {"snapshot": snapshot}

    async def execute(self, state: AutomationGraphState) -> AutomationGraphState:
        snapshot = state["snapshot"]
        workflow = snapshot.request.workflow
        if workflow is WorkflowKind.FEED_ENGAGE:
            if self.adapters.execute_engage is None:
                snapshot.mark_failed("execute_engage adapter not configured", node="execute")
                return {"snapshot": snapshot}
            result = await maybe_await(self.adapters.execute_engage(snapshot))
        else:
            if self.adapters.execute_post is None:
                snapshot.mark_failed("execute_post adapter not configured", node="execute")
                return {"snapshot": snapshot}
            result = await maybe_await(self.adapters.execute_post(snapshot))
        snapshot.mark_completed(result)
        return {"snapshot": snapshot}

    async def finalize(self, state: AutomationGraphState) -> AutomationGraphState:
        snapshot = state["snapshot"]
        snapshot.touch()
        snapshot.log_event("finalize", "workflow finished", status=snapshot.status.value)
        return {"snapshot": snapshot}


def _build_runtime_graph(config: AutomationConfig, storage: PolicyHooks | Any, *, proxy: str | None = None) -> AutomationGraph:
    client = TwitterClient.from_config(
        config.agent_reach_config_path,
        proxy=proxy or config.twitter.proxy_url,
        twitter_bin="twitter",
        timeout=120,
    )
    service = TwitterEngageService(client)
    ai_provider = build_ai_provider(config.ai)

    def fetch_feed(snapshot: WorkflowStateModel):
        options = snapshot.request.feed_options or FeedOptions()
        tweets = service.client.fetch_feed(max_items=options.feed_count, feed_type=options.feed_type)
        return [
            FeedCandidate(
                tweet_id=tweet.tweet_id,
                screen_name=tweet.screen_name,
                text=tweet.text,
                author_verified=tweet.verified,
                metadata=tweet.raw,
            )
            for tweet in tweets
        ]

    def load_repo_context(snapshot: WorkflowStateModel):
        raw = fetch_repo_context(snapshot.request.repo_url or "")
        return RepoContext(**raw)

    def build_hydrated_metadata(candidate: FeedCandidate, tweet: TweetRecord) -> dict[str, Any]:
        raw = dict(tweet.raw) if isinstance(tweet.raw, dict) else {}
        author = raw.get("author")
        if not isinstance(author, dict):
            raw["author"] = {
                "screenName": tweet.screen_name or candidate.screen_name or "",
                "verified": tweet.verified,
            }
        raw["_x_atuo_hydrated"] = True
        return raw

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
        candidate.screen_name = tweet.screen_name or candidate.screen_name
        candidate.text = tweet.text or candidate.text
        candidate.author_verified = tweet.verified
        candidate.metadata = build_hydrated_metadata(candidate, tweet)
        return candidate

    def enrich_reply_context(snapshot: WorkflowStateModel):
        candidate = snapshot.selected_candidate
        if candidate is None or snapshot.request.workflow is not WorkflowKind.FEED_ENGAGE:
            return {}

        replies: list[TweetRecord] = []
        metadata = candidate.metadata if isinstance(candidate.metadata, dict) else {}
        if metadata.get("id") and metadata.get("author"):
            main_tweet = TweetRecord.from_payload(metadata)
        else:
            main_tweet = TweetRecord.from_payload(
                {
                    "id": candidate.tweet_id,
                    "text": candidate.text or "",
                    "author": {
                        "screenName": candidate.screen_name or "",
                        "verified": bool(candidate.author_verified),
                    },
                }
            )

        candidate.screen_name = main_tweet.screen_name or candidate.screen_name
        candidate.text = main_tweet.text or candidate.text
        candidate.author_verified = main_tweet.verified
        if main_tweet.raw:
            candidate.metadata = main_tweet.raw

        author_profile: dict[str, Any] = {}
        if candidate.screen_name and getattr(service.client, "fetch_user_profile", None) is not None:
            try:
                author_profile = service.client.fetch_user_profile(candidate.screen_name)
            except Exception as exc:
                snapshot.log_event("enrich_reply_context", "author profile unavailable", error=str(exc))

        author_recent_posts: list[dict[str, Any]] = []
        if candidate.screen_name and getattr(service.client, "fetch_user_posts", None) is not None:
            try:
                recent_posts = service.client.fetch_user_posts(candidate.screen_name, max_items=5)
                author_recent_posts = [
                    _tweet_to_context(tweet)
                    for tweet in recent_posts
                    if tweet.tweet_id != candidate.tweet_id
                ][:5]
            except Exception as exc:
                snapshot.log_event("enrich_reply_context", "author history unavailable", error=str(exc))

        context: dict[str, Any] = {
            "target_post": _tweet_to_context(main_tweet),
            "author_profile": author_profile,
            "author_recent_posts": author_recent_posts,
            "existing_replies": [_tweet_to_context(reply) for reply in replies[:5]],
            "existing_replies_summary": _summarize_replies(replies),
            "knowledge_evidence": [],
        }

        if ai_provider and snapshot.request.approval_mode == "ai_auto":
            planner = getattr(ai_provider, "plan_reply_context", None)
            if planner is not None:
                try:
                    plan = _call_with_optional_context(planner, candidate, context)
                    context["reply_brief"] = {
                        "acknowledgment": getattr(plan, "acknowledgment", "") or "",
                        "fuller_angle": getattr(plan, "fuller_angle", "") or "",
                        "rationale": getattr(plan, "rationale", "") or "",
                        "needs_live_search": bool(getattr(plan, "needs_live_search", False)),
                        "search_query": getattr(plan, "search_query", None),
                    }
                    snapshot.log_event(
                        "enrich_reply_context",
                        "reply context planned",
                        needs_live_search=context["reply_brief"]["needs_live_search"],
                        search_query=context["reply_brief"]["search_query"],
                    )
                    if context["reply_brief"]["needs_live_search"] and context["reply_brief"]["search_query"]:
                        context["knowledge_evidence"] = search_web_context(str(context["reply_brief"]["search_query"]), max_results=3)
                        snapshot.log_event(
                            "enrich_reply_context",
                            "live search enriched reply context",
                            query=context["reply_brief"]["search_query"],
                            results=len(context["knowledge_evidence"]),
                        )
                except AIProviderError as exc:
                    snapshot.log_event("enrich_reply_context", "reply context planning failed", error=str(exc))

        return context

    def draft_reply(snapshot: WorkflowStateModel):
        candidate = snapshot.selected_candidate
        reply_context = snapshot.reply_context if isinstance(snapshot.reply_context, dict) else {}
        brief = reply_context.get("reply_brief") if isinstance(reply_context.get("reply_brief"), dict) else None
        fallback_text = str(reply_context.get("base_reply_text") or "")
        if not fallback_text:
            fallback_text = _compose_fallback_reply_from_tweet(
                candidate.text if candidate is not None else None,
                max_length=config.policies.max_reply_length,
            )
        if not bool(reply_context.get("should_enrich")):
            snapshot.drafting_source = "deterministic"
            snapshot.log_event(
                "draft_reply",
                "base reply used without enhancement",
                rationale=str(reply_context.get("enhancement_reason") or ""),
            )
            return fallback_text
        if isinstance(brief, dict) and not (reply_context.get("knowledge_evidence") or []):
            snapshot.drafting_source = "ai"
            snapshot.log_event(
                "draft_reply",
                "enhanced reply composed from reply brief",
                rationale=str(brief.get("rationale") or ""),
            )
            return compose_reply_text(
                str(brief.get("acknowledgment") or ""),
                str(brief.get("fuller_angle") or ""),
                max_length=config.policies.max_reply_length,
            )
        if ai_provider and snapshot.request.approval_mode == "ai_auto" and snapshot.selected_candidate is not None:
            try:
                draft = _call_with_optional_context(
                    ai_provider.draft_reply,
                    snapshot.selected_candidate,
                    _compact_reply_context(reply_context),
                )
                snapshot.drafting_source = "ai"
                snapshot.log_event("draft_reply", "ai draft generated", rationale=draft.rationale)
                return draft.text
            except AIProviderError as exc:
                snapshot.log_event("draft_reply", "ai draft failed, using fallback", error=str(exc))
        snapshot.drafting_source = "deterministic"
        return snapshot.request.reply_text or fallback_text

    def draft_post(snapshot: WorkflowStateModel):
        if snapshot.request.post_text:
            snapshot.drafting_source = "request"
            return snapshot.request.post_text
        if snapshot.repo_context:
            if ai_provider and snapshot.request.approval_mode == "ai_auto":
                try:
                    draft = ai_provider.draft_repo_post(snapshot.repo_context)
                    snapshot.drafting_source = "ai"
                    snapshot.log_event("draft_post", "ai repo draft generated", rationale=draft.rationale)
                    return draft.text
                except AIProviderError as exc:
                    snapshot.log_event("draft_post", "ai repo draft failed, using fallback", error=str(exc))
            snapshot.drafting_source = "deterministic"
            return render_repo_post_text(
                type(
                    "RepoCtx",
                    (),
                    {
                        "repo_url": snapshot.repo_context.repo_url,
                        "repo_name": snapshot.repo_context.repo_name or snapshot.repo_context.repo_url,
                        "description": snapshot.repo_context.description,
                        "readme_excerpt": snapshot.repo_context.readme_excerpt,
                        "stars": snapshot.repo_context.stars,
                    },
                )(),
                max_length=280,
            )
        snapshot.drafting_source = "deterministic"
        return "Repository worth checking out."

    def select_candidate(snapshot: WorkflowStateModel):
        if snapshot.request.candidate is not None:
            if snapshot.selection_source is None:
                snapshot.selection_source = "request"
            return snapshot.request.candidate
        if ai_provider and snapshot.request.approval_mode == "ai_auto" and snapshot.candidates:
            try:
                selection = ai_provider.select_candidate(snapshot.candidates)
                for candidate in snapshot.candidates:
                    if candidate.tweet_id == selection.tweet_id:
                        snapshot.selection_source = "ai"
                        snapshot.selection_reason = selection.reason
                        return candidate
            except AIProviderError as exc:
                snapshot.log_event("select_candidate", "ai selection failed, using fallback", error=str(exc))
        snapshot.selection_source = "deterministic"
        return snapshot.candidates[0] if snapshot.candidates else None

    def plan_reply_strategy(snapshot: WorkflowStateModel):
        if snapshot.selected_candidate is not None:
            hydrate_selected_candidate(snapshot.selected_candidate)
        if snapshot.request.approval_mode != "ai_auto":
            return AIEnhancementDecision(should_enrich=False, reason="deterministic mode skips enhancement")
        if ai_provider is None or snapshot.selected_candidate is None:
            return AIEnhancementDecision(should_enrich=False, reason="no ai provider")
        decider = getattr(ai_provider, "decide_reply_enhancement", None)
        if decider is None:
            return AIEnhancementDecision(should_enrich=True, reason="provider does not expose lightweight gate")
        base_reply_text = _compose_fallback_reply_from_tweet(
            snapshot.selected_candidate.text,
            max_length=config.policies.max_reply_length,
        )
        return _call_with_optional_context(decider, snapshot.selected_candidate, base_reply_text)

    async def moderate_candidates(snapshot: WorkflowStateModel):
        if ai_provider is None or not snapshot.candidates:
            return []
        moderate = getattr(ai_provider, "moderate_candidates", None)
        if moderate is None:
            return []
        fetch_failures: list[AIModerationResult] = []

        async def hydrate_candidate(candidate: FeedCandidate) -> tuple[FeedCandidate, TweetRecord | None, Exception | None]:
            try:
                tweet = await asyncio.to_thread(service.client.fetch_tweet, candidate.tweet_id)
                return candidate, tweet, None
            except Exception as exc:
                return candidate, None, exc

        hydrated = await asyncio.gather(*(hydrate_candidate(candidate) for candidate in snapshot.candidates))
        for candidate, tweet, exc in hydrated:
            if exc is not None:
                fetch_failures.append(
                    AIModerationResult(
                        tweet_id=candidate.tweet_id,
                        allowed=False,
                        category="tweet_fetch_failed",
                        reason=f"could not fetch full tweet for moderation: {exc}",
                    )
                )
                continue
            if tweet is None:
                continue
            candidate.screen_name = tweet.screen_name or candidate.screen_name
            candidate.text = tweet.text or candidate.text
            candidate.author_verified = tweet.verified
            candidate.metadata = build_hydrated_metadata(candidate, tweet)
        return [*moderate(snapshot.candidates), *fetch_failures]

    def execute_engage(snapshot: WorkflowStateModel):
        candidate = snapshot.selected_candidate
        if candidate is None:
            return ExecutionResult(action="engage", ok=False, dry_run=snapshot.request.dry_run, error="candidate missing")
        ordered_candidates = [candidate] + [
            candidate_item
            for candidate_item in snapshot.candidates
            if candidate_item.tweet_id != candidate.tweet_id
        ]
        result = service.engage_candidates(
            [
                Candidate(
                    tweet_id=candidate_item.tweet_id,
                    screen_name=candidate_item.screen_name or "",
                    reply_text=snapshot.rendered_text or snapshot.request.reply_text or "",
                    tweet=(
                        TweetRecord.from_payload(candidate_item.metadata)
                        if isinstance(candidate_item.metadata, dict)
                        and candidate_item.metadata.get("id")
                        and candidate_item.metadata.get("author")
                        else None
                    ),
                )
                for candidate_item in ordered_candidates
            ],
            dry_run=snapshot.request.dry_run,
        )
        if result.selected_candidate is not None:
            snapshot.selected_candidate = FeedCandidate(
                tweet_id=result.selected_candidate.tweet_id,
                screen_name=result.selected_candidate.screen_name,
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
                "selected_by": snapshot.selection_source or "deterministic",
                "selection_reason": snapshot.selection_reason,
                "drafted_by": snapshot.drafting_source or "deterministic",
                "attempts": [
                    {
                        "tweet_id": attempt.tweet_id,
                        "screen_name": attempt.screen_name,
                        "outcome": attempt.outcome,
                        "detail": attempt.detail,
                    }
                    for attempt in result.attempts
                ],
            },
        )

    def execute_post(snapshot: WorkflowStateModel):
        if snapshot.request.workflow is WorkflowKind.DIRECT_POST:
            result = service.post_tweet(
                text=snapshot.rendered_text or snapshot.request.post_text or "",
                images=snapshot.request.media_paths,
                dry_run=snapshot.request.dry_run,
            )
        else:
            result = service.post_tweet(
                text=snapshot.rendered_text or snapshot.request.post_text or "",
                dry_run=snapshot.request.dry_run,
            )
        return ExecutionResult(
            action=result.action,
            ok=result.ok,
            dry_run=result.dry_run,
            target_tweet_id=result.target_tweet_id,
            created_tweet_id=result.tweet_id,
            error=result.error_message,
            detail={
                "payload": result.payload,
                "drafted_by": snapshot.drafting_source or "deterministic",
            },
        )

    return AutomationGraph(
        config,
        WorkflowAdapters(
            fetch_feed=fetch_feed,
            moderate_candidates=moderate_candidates if getattr(ai_provider, "moderate_candidates", None) is not None else None,
            select_candidate=select_candidate,
            plan_reply_strategy=plan_reply_strategy,
            enrich_reply_context=enrich_reply_context,
            load_repo_context=load_repo_context,
            draft_reply=draft_reply,
            draft_post=draft_post,
            execute_engage=execute_engage,
            execute_post=execute_post,
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
        "selected_candidate": snapshot.selected_candidate.model_dump(mode="json") if snapshot.selected_candidate else None,
        "rendered_text": snapshot.rendered_text,
        "selection_source": snapshot.selection_source,
        "selection_reason": snapshot.selection_reason,
        "drafted_by": snapshot.drafting_source,
        "errors": snapshot.errors,
        "events": [event.model_dump(mode="json") for event in snapshot.events],
    }


async def run_feed_engage(*, run_id: str, job_id: str, payload: dict[str, Any], storage: Any) -> dict[str, Any]:
    candidate_payload = payload.get("candidate")
    candidates_payload = payload.get("candidates") or []
    config = AutomationConfig()
    request = AutomationRequest.for_feed_engage(
        job_name=job_id,
        run_id=run_id,
        dry_run=bool(payload.get("dry_run", False)),
        approval_mode=payload.get("mode", "ai_auto"),
        reply_text=payload.get("reply_text") or payload.get("reply_template"),
        candidate=FeedCandidate.model_validate(candidate_payload) if candidate_payload else None,
        candidates=[FeedCandidate.model_validate(item) for item in candidates_payload],
        feed_options=FeedOptions(
            feed_type=payload.get("feed_type", config.twitter.default_feed_type),
            feed_count=payload.get("feed_count", config.twitter.default_feed_count),
        ),
        metadata=payload.get("metadata", {}),
        idempotency_key=payload.get("idempotency_key"),
    )
    return await _run_request(request, storage=storage, proxy=payload.get("proxy") or config.twitter.proxy_url)


async def run_repo_post(*, run_id: str, job_id: str, payload: dict[str, Any], storage: Any) -> dict[str, Any]:
    request = AutomationRequest.for_repo_post(
        job_name=job_id,
        run_id=run_id,
        dry_run=bool(payload.get("dry_run", False)),
        approval_mode=payload.get("mode", "ai_auto"),
        repo_url=payload["repo_url"],
        post_text=payload.get("post_text"),
        metadata=payload.get("metadata", {}),
        idempotency_key=payload.get("idempotency_key"),
    )
    return await _run_request(request, storage=storage)


async def run_direct_post(*, run_id: str, job_id: str, payload: dict[str, Any], storage: Any) -> dict[str, Any]:
    request = AutomationRequest.for_direct_post(
        job_name=job_id,
        run_id=run_id,
        dry_run=bool(payload.get("dry_run", False)),
        approval_mode=payload.get("mode", "ai_auto"),
        post_text=payload["text"],
        media_paths=payload.get("images", []),
        metadata=payload.get("metadata", {}),
        idempotency_key=payload.get("idempotency_key"),
    )
    return await _run_request(request, storage=storage)
