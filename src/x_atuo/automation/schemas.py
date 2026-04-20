from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

JobMode = Literal["deterministic", "ai_auto"]
RunStatus = Literal["queued", "running", "completed", "failed", "blocked"]
FeedType = Literal["following", "for-you"]
JobType = Literal["feed_engage", "repo_post", "direct_post"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class BaseWebhookRequest(StrictModel):
    job_id: str | None = None
    mode: JobMode = "ai_auto"
    dry_run: bool = False
    proxy: str | None = "http://127.0.0.1:7890"
    metadata: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = None


class FeedEngageRequest(BaseWebhookRequest):
    feed_count: int = Field(default=10, ge=1, le=100)
    feed_type: FeedType = "for-you"
    reply_template: str | None = None


class RepoPostRequest(BaseWebhookRequest):
    repo_url: str = Field(min_length=1, max_length=2048)
    style: str = Field(default="summary", min_length=1, max_length=64)
    post_text: str | None = Field(default=None, max_length=280)


class DirectPostRequest(BaseWebhookRequest):
    text: str = Field(min_length=1, max_length=280)
    images: list[str] = Field(default_factory=list, max_length=4)

    @field_validator("images")
    @classmethod
    def validate_images(cls, value: list[str]) -> list[str]:
        for item in value:
            if not item or not item.strip():
                raise ValueError("images entries must be non-empty paths")
        return value


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


class WebhookAcceptedResponse(StrictModel):
    run_id: str
    job_id: str
    job_type: JobType
    endpoint: str
    status: RunStatus
    accepted_at: datetime
    result: dict[str, Any] | list[Any] | str | None = None
