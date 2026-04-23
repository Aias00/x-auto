"""Workflow state and request models for automation orchestration."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Any, TypedDict
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""

    return datetime.now(UTC)


class WorkflowKind(str, Enum):
    """Supported workflow types."""

    FEED_ENGAGE = "feed-engage"
    REPO_POST = "repo-post"
    DIRECT_POST = "direct-post"


class RunStatus(str, Enum):
    """High-level workflow lifecycle states."""

    PENDING = "pending"
    RUNNING = "running"
    BLOCKED = "blocked"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class FeedCandidate(BaseModel):
    """Normalized tweet/feed candidate."""

    tweet_id: str
    screen_name: str | None = None
    text: str | None = None
    created_at: datetime | None = None
    url: str | None = None
    author_verified: bool | None = None
    can_reply: bool | None = None
    reply_limit_reason: str | None = None
    reply_limit_headline: str | None = None
    reply_restriction_policy: str | None = None
    score: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class FeedOptions(BaseModel):
    """Inputs for live feed fetching."""

    feed_type: str = "following"
    feed_count: int = 20


class RepoContext(BaseModel):
    """Minimal repo context used by repo-post workflows."""

    repo_url: str
    repo_name: str | None = None
    description: str | None = None
    readme_excerpt: str | None = None
    stars: int | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class PolicyDecision(BaseModel):
    """Outcome of policy evaluation."""

    allowed: bool = True
    reasons: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    dedupe_key: str | None = None


class ExecutionResult(BaseModel):
    """Structured outcome from an execution runner."""

    action: str
    ok: bool = True
    dry_run: bool = True
    target_tweet_id: str | None = None
    target_tweet_url: str | None = None
    created_tweet_id: str | None = None
    reply_url: str | None = None
    followed: bool | None = None
    error: str | None = None
    detail: dict[str, Any] = Field(default_factory=dict)


class StateEvent(BaseModel):
    """Lightweight audit event for graph nodes."""

    node: str
    message: str
    payload: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=utc_now)


class AutomationRequest(BaseModel):
    """Normalized workflow request accepted by the automation graph."""

    workflow: WorkflowKind
    job_name: str | None = None
    run_id: str | None = None
    dry_run: bool = True
    approval_mode: str = "deterministic"
    idempotency_key: str | None = None
    reply_text: str | None = None
    candidate: FeedCandidate | None = None
    candidates: list[FeedCandidate] = Field(default_factory=list)
    feed_options: FeedOptions | None = None
    repo_url: str | None = None
    post_text: str | None = None
    media_paths: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_for_workflow(self) -> "AutomationRequest":
        """Enforce workflow-specific payload requirements."""

        if self.workflow is WorkflowKind.FEED_ENGAGE and self.feed_options is None:
            self.feed_options = FeedOptions()
        if self.workflow is WorkflowKind.REPO_POST and not self.repo_url:
            raise ValueError("repo-post requires repo_url")
        if self.workflow is WorkflowKind.DIRECT_POST and not self.post_text:
            raise ValueError("direct-post requires post_text")
        return self

    @classmethod
    def for_feed_engage(
        cls,
        *,
        reply_text: str | None = None,
        candidate: FeedCandidate | None = None,
        candidates: list[FeedCandidate] | None = None,
        feed_options: FeedOptions | None = None,
        **kwargs: Any,
    ) -> "AutomationRequest":
        return cls(
            workflow=WorkflowKind.FEED_ENGAGE,
            reply_text=reply_text,
            candidate=candidate,
            candidates=candidates or [],
            feed_options=feed_options or FeedOptions(),
            **kwargs,
        )

    @classmethod
    def for_repo_post(cls, *, repo_url: str, post_text: str | None = None, **kwargs: Any) -> "AutomationRequest":
        return cls(
            workflow=WorkflowKind.REPO_POST,
            repo_url=repo_url,
            post_text=post_text,
            **kwargs,
        )

    @classmethod
    def for_direct_post(
        cls,
        *,
        post_text: str,
        media_paths: list[str] | None = None,
        **kwargs: Any,
    ) -> "AutomationRequest":
        return cls(
            workflow=WorkflowKind.DIRECT_POST,
            post_text=post_text,
            media_paths=media_paths or [],
            **kwargs,
        )


class WorkflowStateModel(BaseModel):
    """Complete per-run state shared across LangGraph nodes."""

    run_id: str = Field(default_factory=lambda: str(uuid4()))
    request: AutomationRequest
    status: RunStatus = RunStatus.PENDING
    policy: PolicyDecision = Field(default_factory=PolicyDecision)
    candidate_refresh_count: int = 0
    candidate_refresh_pending: bool = False
    candidate_cache_persisted: bool = False
    candidates: list[FeedCandidate] = Field(default_factory=list)
    selected_candidate: FeedCandidate | None = None
    selection_source: str | None = None
    selection_reason: str | None = None
    repo_context: RepoContext | None = None
    reply_context: dict[str, Any] = Field(default_factory=dict)
    rendered_text: str | None = None
    drafting_source: str | None = None
    result: ExecutionResult | None = None
    execution_attempt_history: list[dict[str, Any]] = Field(default_factory=list)
    runtime_observability: dict[str, dict[str, Any]] = Field(default_factory=dict, exclude=True)
    errors: list[str] = Field(default_factory=list)
    events: list[StateEvent] = Field(default_factory=list)
    started_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    def touch(self) -> None:
        self.updated_at = utc_now()

    def stash_runtime_observability(self, key: str, **payload: Any) -> None:
        self.runtime_observability[key] = payload

    def pop_runtime_observability(self, key: str) -> dict[str, Any]:
        value = self.runtime_observability.pop(key, {})
        return value if isinstance(value, dict) else {}

    def log_event(self, node: str, message: str, **payload: Any) -> None:
        self.events.append(StateEvent(node=node, message=message, payload=payload))
        self.touch()

    def mark_failed(self, message: str, *, node: str = "graph") -> None:
        self.status = RunStatus.FAILED
        self.errors.append(message)
        self.log_event(node, message)

    def mark_blocked(self, reasons: list[str], *, node: str = "policy") -> None:
        self.status = RunStatus.BLOCKED
        self.errors.extend(reasons)
        self.log_event(node, "policy blocked execution", reasons=reasons)

    def mark_completed(self, result: ExecutionResult, *, node: str = "execute", **payload: Any) -> None:
        self.result = result
        self.status = RunStatus.COMPLETED if result.ok else RunStatus.FAILED
        message = "execution completed" if result.ok else "execution failed"
        self.log_event(node, message, result=result.model_dump(mode="json"), **payload)


class AutomationGraphState(TypedDict):
    """LangGraph-compatible wrapper around the workflow snapshot."""

    snapshot: WorkflowStateModel


def make_initial_state(request: AutomationRequest) -> AutomationGraphState:
    """Create a graph state payload from an external request."""

    snapshot = WorkflowStateModel(
        run_id=request.run_id or str(uuid4()),
        request=request,
        status=RunStatus.RUNNING,
    )
    snapshot.log_event("prepare", "workflow initialized", workflow=request.workflow.value)
    return {"snapshot": snapshot}
