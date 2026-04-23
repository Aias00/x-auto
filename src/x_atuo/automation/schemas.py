from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

RunStatus = Literal["queued", "running", "completed", "failed", "blocked"]
JobType = Literal["feed_engage"]


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
