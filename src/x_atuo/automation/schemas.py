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


class AuthorAlphaScoreRecord(StrictModel):
    screen_name: str
    author_name: str | None = None
    rest_id: str | None = None
    author_score: float
    reply_count_7d: int
    impressions_total_7d: int
    avg_impressions_7d: float
    max_impressions_7d: int
    last_replied_at: str | None = None
    last_post_seen_at: str | None = None
    last_scored_at: str | None = None
    source: str | None = None
    created_at: str
    updated_at: str


class AuthorAlphaReplyDailyMetricRecord(StrictModel):
    metric_date: str
    reply_tweet_id: str
    target_tweet_id: str | None = None
    target_author: str | None = None
    impressions: int
    likes: int
    replies: int
    reposts: int
    sampled_at: str


class AuthorAlphaAuthorDailyRollupRecord(StrictModel):
    metric_date: str
    target_author: str
    reply_count: int
    impressions_total: int
    likes_total: int
    replies_total: int
    reposts_total: int
    avg_impressions: float
    max_impressions: int
    computed_at: str


class AuthorAlphaSyncCheckpointRecord(StrictModel):
    sync_scope: str
    last_completed_date: str | None = None
    next_pending_date: str | None = None
    last_run_id: str | None = None
    updated_at: str


class AuthorAlphaEngagementRecord(StrictModel):
    id: int | None = None
    run_id: str
    target_author: str
    target_tweet_id: str
    target_tweet_url: str | None = None
    reply_tweet_id: str
    reply_url: str | None = None
    burst_id: str | None = None
    burst_index: int | None = None
    burst_size: int | None = None
    metric_date: str
    created_at: str


class AuthorAlphaExecutionRunRecord(StrictModel):
    id: str
    job_id: str
    job_type: str
    endpoint: str
    status: str
    request_payload: dict[str, Any]
    response_payload: dict[str, Any] | list[Any] | str | None = None
    error: str | None = None
    created_at: str
    updated_at: str
    started_at: str | None = None
    finished_at: str | None = None


class AuthorAlphaExecutionAuditEventRecord(StrictModel):
    id: int | None = None
    run_id: str
    level: str
    event_type: str
    node: str | None = None
    payload: dict[str, Any] | list[Any] | str | None = None
    created_at: str


class SharedEngagementRecord(StrictModel):
    id: int | None = None
    workflow: str
    run_id: str
    target_tweet_id: str | None = None
    target_author: str | None = None
    target_tweet_url: str | None = None
    reply_tweet_id: str | None = None
    reply_url: str | None = None
    followed: int | bool
    created_at: str


class AuthorAlphaScoreSnapshotResponse(StrictModel):
    schema_version: Literal[1]
    exported_at: datetime
    author_count: int
    reply_metric_count: int
    rollup_count: int
    sync_run_count: int = 0
    sync_checkpoint_count: int = 0
    engagement_count: int = 0
    execution_run_count: int = 0
    execution_audit_event_count: int = 0
    shared_engagement_count: int = 0
    latest_scored_at: str | None = None
    authors: list[AuthorAlphaScoreRecord] = Field(default_factory=list)
    reply_daily_metrics: list[AuthorAlphaReplyDailyMetricRecord] = Field(default_factory=list)
    author_daily_rollups: list[AuthorAlphaAuthorDailyRollupRecord] = Field(default_factory=list)
    sync_runs: list[AuthorAlphaSyncRunRecord] = Field(default_factory=list)
    sync_checkpoints: list[AuthorAlphaSyncCheckpointRecord] = Field(default_factory=list)
    engagements: list[AuthorAlphaEngagementRecord] = Field(default_factory=list)
    execution_runs: list[AuthorAlphaExecutionRunRecord] = Field(default_factory=list)
    execution_audit_events: list[AuthorAlphaExecutionAuditEventRecord] = Field(default_factory=list)
    shared_engagements: list[SharedEngagementRecord] = Field(default_factory=list)


class AuthorAlphaScoreImportResponse(StrictModel):
    status: Literal["imported"]
    schema_version: Literal[1]
    replace_existing: bool
    imported_count: int
    imported_reply_metric_count: int
    imported_rollup_count: int
    imported_sync_run_count: int
    imported_sync_checkpoint_count: int
    imported_engagement_count: int
    imported_execution_run_count: int
    imported_execution_audit_event_count: int
    imported_shared_engagement_count: int
    latest_scored_at: str | None = None


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
