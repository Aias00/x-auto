"""LangGraph workflow definitions for automation runs."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from langgraph.graph import END, StateGraph

from x_atuo.automation.config import AutomationConfig
from x_atuo.automation.policies import PolicyHooks, evaluate_policy
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
from x_atuo.core.ai_client import AIProviderError, build_ai_provider
from x_atuo.core.github_repo_client import fetch_repo_context as fetch_github_repo_context
from x_atuo.core.github_repo_client import render_repo_post_text
from x_atuo.core.twitter_client import TwitterClient
from x_atuo.core.twitter_engage_service import TwitterEngageService
from x_atuo.core.twitter_models import Candidate

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


@dataclass(slots=True)
class WorkflowAdapters:
    fetch_feed: StateCallable | None = None
    select_candidate: StateCallable | None = None
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
        workflow.add_node("select_candidate", self.select_candidate)
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
        workflow.add_edge("fetch_feed", "select_candidate")
        workflow.add_edge("select_candidate", "draft_text")
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
        if snapshot.request.workflow is WorkflowKind.EXPLICIT_ENGAGE and snapshot.request.candidate:
            snapshot.selected_candidate = snapshot.request.candidate
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
        if workflow is WorkflowKind.EXPLICIT_ENGAGE:
            return "select_candidate"
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
        if workflow in {WorkflowKind.FEED_ENGAGE, WorkflowKind.EXPLICIT_ENGAGE}:
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

    def _retry_blocked_candidate(self, snapshot: WorkflowStateModel) -> bool:
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
        snapshot.rendered_text = None
        snapshot.drafting_source = None
        snapshot.policy = PolicyDecision()
        snapshot.log_event(
            "policy_guard",
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
        if workflow in {WorkflowKind.FEED_ENGAGE, WorkflowKind.EXPLICIT_ENGAGE}:
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

    def draft_reply(snapshot: WorkflowStateModel):
        if ai_provider and snapshot.request.approval_mode == "ai_auto" and snapshot.selected_candidate is not None:
            try:
                draft = ai_provider.draft_reply(snapshot.selected_candidate)
                snapshot.drafting_source = "ai"
                snapshot.log_event("draft_reply", "ai draft generated", rationale=draft.rationale)
                return draft.text
            except AIProviderError as exc:
                snapshot.log_event("draft_reply", "ai draft failed, using fallback", error=str(exc))
        snapshot.drafting_source = "deterministic"
        return snapshot.request.reply_text or "Interesting work here."

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
            select_candidate=select_candidate,
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


async def run_explicit_engage(*, run_id: str, job_id: str, payload: dict[str, Any], storage: Any) -> dict[str, Any]:
    config = AutomationConfig()
    request = AutomationRequest.for_explicit_engage(
        job_name=job_id,
        run_id=run_id,
        dry_run=bool(payload.get("dry_run", False)),
        approval_mode=payload.get("mode", "ai_auto"),
        reply_text=payload["reply_text"],
        candidate=FeedCandidate(tweet_id=payload["tweet_id"], screen_name=payload.get("screen_name")),
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
