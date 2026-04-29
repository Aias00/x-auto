from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

RunStatus = Literal["queued", "running", "completed", "failed", "blocked"]
JobType = Literal["feed_engage", "author_alpha_engage", "author_alpha_sync"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class HealthResponse(StrictModel):
    status: Literal["ok"]
    db_path: str
    checked_at: datetime


class AuditEventRecord(StrictModel):
    id: int
    run_id: str
    level: str
    event_type: str
    node: str | None = None
    payload: dict[str, Any] | list[Any] | str | None = None
    created_at: datetime


class RunRecord(StrictModel):
    id: str
    job_id: str
    job_type: str
    endpoint: str
    status: RunStatus
    request_payload: dict[str, Any]
    response_payload: dict[str, Any] | list[Any] | str | None = None
    error: str | None = None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None


class RunLookupResponse(StrictModel):
    run: RunRecord
    audit_events: list[AuditEventRecord] = Field(default_factory=list)


class AccountAnalyticsResponse(StrictModel):
    account: dict[str, Any]
    window: dict[str, Any]
    summary: dict[str, int]
    posts: list[dict[str, Any]] = Field(default_factory=list)


class AccountContentAnalyticsResponse(StrictModel):
    account: dict[str, Any]
    filters: dict[str, Any]
    total_matches: int
    returned_count: int
    posts: list[dict[str, Any]] = Field(default_factory=list)


class NotificationsResponse(StrictModel):
    timeline_type: str
    count: int
    cursor: str | None = None
    top_cursor: str | None = None
    bottom_cursor: str | None = None
    entries: list[dict[str, Any]] = Field(default_factory=list)


class DeviceFollowFeedResponse(StrictModel):
    count: int
    timeline_id: str | None = None
    top_cursor: str | None = None
    bottom_cursor: str | None = None
    posts: list[dict[str, Any]] = Field(default_factory=list)


class TwitterTweetsResponse(StrictModel):
    count: int
    items: list[dict[str, Any]] = Field(default_factory=list)


class TwitterUsersResponse(StrictModel):
    count: int
    items: list[dict[str, Any]] = Field(default_factory=list)


class TwitterBookmarkFoldersResponse(StrictModel):
    count: int
    items: list[dict[str, Any]] = Field(default_factory=list)


class TwitterTweetResponse(StrictModel):
    tweet: dict[str, Any]


class FeedEngageExecuteRequest(StrictModel):
    dry_run: bool = False
    reply_text: str | None = None
    feed_count: int = Field(default=10, ge=1, le=100)
    feed_type: str = "for-you"


class FeedEngageExecuteResponse(StrictModel):
    run_id: str
    job_id: str
    job_type: Literal["feed_engage"]
    endpoint: Literal["manual:feed-engage"]
    status: str
    result: dict[str, Any] | list[Any] | str | None = None


class AuthorAlphaSyncRunRecord(StrictModel):
    run_id: str
    run_type: str
    status: str
    from_date: str | None = None
    to_date: str | None = None
    current_date: str | None = None
    days_completed: int | None = None
    days_total: int | None = None
    resume_from_date: str | None = None
    error: str | None = None
    created_at: str | None = None
    started_at: str | None = None
    finished_at: str | None = None


class AuthorAlphaBootstrapRequest(StrictModel):
    from_date: str
    to_date: str
    resume: bool = False
    max_days: int | None = None


class AuthorAlphaReconcileRequest(StrictModel):
    target_date: str | None = None


class AuthorAlphaSyncAcceptedResponse(StrictModel):
    run_id: str
    run_type: Literal["bootstrap", "reconcile"]
    status: Literal["accepted"]
    from_date: str | None = None
    to_date: str | None = None
    resume: bool | None = None
    max_days: int | None = None
    target_date: str | None = None


class AuthorAlphaSyncStopResponse(StrictModel):
    run_id: str
    run_type: Literal["bootstrap", "reconcile"]
    status: Literal["stop_requested"]


class AuthorAlphaResetResponse(StrictModel):
    status: Literal["cleared"]


class AuthorAlphaExecuteRequest(StrictModel):
    dry_run: bool = False


class AuthorAlphaExecuteResponse(StrictModel):
    run_id: str
    job_id: str
    job_type: Literal["author_alpha_engage"]
    endpoint: Literal["manual:author-alpha-engage"]
    status: str
    result: dict[str, Any] | list[Any] | str | None = None


class AuthorAlphaSyncStatusResponse(StrictModel):
    active: bool
    bootstrap_required: bool
    active_run: AuthorAlphaSyncRunRecord | None = None
    latest_run: AuthorAlphaSyncRunRecord | None = None
    bootstrap_checkpoint: dict[str, Any] | None = None
    reconcile_checkpoint: dict[str, Any] | None = None


class AuthorAlphaSyncHistoryResponse(StrictModel):
    runs: list[AuthorAlphaSyncRunRecord] = Field(default_factory=list)


class AuthorAlphaRunLookupResponse(StrictModel):
    run: dict[str, Any]
    audit_events: list[dict[str, Any]] = Field(default_factory=list)
