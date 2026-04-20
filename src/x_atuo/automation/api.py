from __future__ import annotations

import inspect
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request, status

from x_atuo.automation.config import AutomationConfig
from x_atuo.automation.schemas import (
    FeedEngageRequest,
    DirectPostRequest,
    HealthResponse,
    RepoPostRequest,
    RunLookupResponse,
    WebhookAcceptedResponse,
)
from x_atuo.automation.scheduler import AutomationScheduler, ScheduledWorkflow
from x_atuo.automation.state import AutomationRequest, FeedOptions, WorkflowKind
from x_atuo.automation.storage import AutomationStorage, utcnow


def _resolve_db_path() -> Path:
    return Path(os.getenv("X_ATUO_DB_PATH", "data/x_atuo.sqlite3"))


def _workflow_binding(request_obj: AutomationRequest) -> tuple[str, str, dict[str, Any], str]:
    if request_obj.workflow is WorkflowKind.FEED_ENGAGE:
        feed_options = request_obj.feed_options or FeedOptions()
        payload = {
            "feed_count": feed_options.feed_count,
            "feed_type": feed_options.feed_type,
            "mode": request_obj.approval_mode,
            "dry_run": request_obj.dry_run,
            "reply_text": request_obj.reply_text,
            "metadata": request_obj.metadata,
            "idempotency_key": request_obj.idempotency_key,
            "proxy": request_obj.metadata.get("proxy"),
        }
        return "feed_engage", "run_feed_engage", payload, "scheduler:feed-engage"
    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="unsupported scheduled workflow")


async def _execute_job(
    *,
    storage: AutomationStorage,
    endpoint: str,
    job_type: str,
    function_name: str,
    payload: dict[str, Any],
    requested_job_id: str | None,
) -> dict[str, Any]:
    run_id = str(uuid4())
    job_id = requested_job_id or f"{job_type}-{run_id}"
    normalized_result: Any = None
    run_status = "failed"
    error_message: str | None = None

    storage.upsert_job(job_id, job_type, config=payload)
    storage.create_run(
        run_id=run_id,
        job_id=job_id,
        job_type=job_type,
        endpoint=endpoint,
        request_payload=payload,
    )
    storage.add_audit_event(
        run_id=run_id,
        event_type="trigger_received",
        node="service",
        payload={"endpoint": endpoint, "job_id": job_id},
    )
    storage.update_run(run_id, status="running", started_at=utcnow())

    try:
        result = await _call_graph(
            function_name,
            run_id=run_id,
            job_id=job_id,
            payload=payload,
            storage=storage,
        )
        normalized_result = _normalize_result(result)
        run_status = _derive_status(normalized_result)
    except Exception as exc:
        error_message = str(exc)
        normalized_result = {"status": "failed", "error": error_message}

    storage.update_run(
        run_id,
        status=run_status,
        response_payload=normalized_result,
        error=error_message,
        finished_at=utcnow(),
    )
    storage.add_audit_event(
        run_id=run_id,
        event_type="orchestration_finished",
        node="service",
        payload={"status": run_status, "error": error_message},
    )
    return {
        "run_id": run_id,
        "job_id": job_id,
        "job_type": job_type,
        "endpoint": endpoint,
        "status": run_status,
        "result": normalized_result,
    }


async def _dispatch_scheduled_request(request_obj: AutomationRequest, storage: AutomationStorage) -> dict[str, Any]:
    job_type, function_name, payload, endpoint = _workflow_binding(request_obj)
    return await _execute_job(
        storage=storage,
        endpoint=endpoint,
        job_type=job_type,
        function_name=function_name,
        payload=payload,
        requested_job_id=request_obj.job_name,
    )


def _record_dropped_scheduled_request(
    request_obj: AutomationRequest,
    storage: AutomationStorage,
    *,
    reason: str,
) -> str:
    job_type, _function_name, payload, endpoint = _workflow_binding(request_obj)
    run_id = str(uuid4())
    job_id = request_obj.job_name or f"{job_type}-{run_id}"
    storage.upsert_job(job_id, job_type, config=payload)
    storage.create_run(
        run_id=run_id,
        job_id=job_id,
        job_type=job_type,
        endpoint=endpoint,
        request_payload=payload,
        status="blocked",
    )
    storage.add_audit_event(
        run_id=run_id,
        event_type="scheduler_queue_dropped",
        node="service",
        payload={"endpoint": endpoint, "job_id": job_id, "reason": reason},
    )
    storage.update_run(
        run_id,
        status="blocked",
        response_payload={"status": "blocked", "error": reason},
        error=reason,
        finished_at=utcnow(),
    )
    return run_id


def _build_scheduled_feed_engage(settings: AutomationConfig) -> ScheduledWorkflow | None:
    if not (settings.scheduler.enabled and settings.scheduler.feed_engage_enabled):
        return None
    trigger = settings.scheduler.feed_engage_trigger
    trigger_args: dict[str, Any]
    if trigger == "interval":
        trigger_args = {
            "seconds": settings.scheduler.feed_engage_seconds,
            "jitter": settings.scheduler.feed_engage_jitter_seconds,
        }
    else:
        trigger_args = {"jitter": settings.scheduler.feed_engage_jitter_seconds}
        if settings.scheduler.feed_engage_minute is not None:
            trigger_args["minute"] = settings.scheduler.feed_engage_minute
        if settings.scheduler.feed_engage_hour is not None:
            trigger_args["hour"] = settings.scheduler.feed_engage_hour
        if settings.scheduler.feed_engage_day is not None:
            trigger_args["day"] = settings.scheduler.feed_engage_day
        if settings.scheduler.feed_engage_day_of_week is not None:
            trigger_args["day_of_week"] = settings.scheduler.feed_engage_day_of_week
    defaults = FeedEngageRequest()
    request_obj = AutomationRequest.for_feed_engage(
        job_name="scheduled-feed-engage",
        dry_run=defaults.dry_run,
        approval_mode=defaults.mode,
        reply_text=defaults.reply_template,
        feed_options=FeedOptions(feed_type=defaults.feed_type, feed_count=defaults.feed_count),
        metadata={"proxy": defaults.proxy, "trigger": "scheduler"},
    )
    return ScheduledWorkflow(
        job_id="scheduled-feed-engage",
        request=request_obj,
        trigger=trigger,
        trigger_args=trigger_args,
        enabled=True,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = AutomationConfig()
    storage = AutomationStorage(_resolve_db_path())
    storage.initialize()
    storage.clear_stale_running_runs(reason="stale running cleared on service startup")
    app.state.storage = storage
    app.state.settings = settings

    scheduler = AutomationScheduler(
        settings.scheduler,
        lambda request_obj: _dispatch_scheduled_request(request_obj, storage),
        on_queue_full=lambda request_obj: _record_dropped_scheduled_request(
            request_obj,
            storage,
            reason="scheduler backlog full",
        ),
    )
    definition = _build_scheduled_feed_engage(settings)
    if definition is not None:
        scheduler.register_job(definition)
        app.state.scheduled_feed_engage = definition
    app.state.scheduler = scheduler
    scheduler.maybe_start()
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)


app = FastAPI(title="x-atuo automation API", lifespan=lifespan)


def get_storage(request: Request) -> AutomationStorage:
    return request.app.state.storage


def _load_graph_module() -> ModuleType:
    try:
        return import_module("x_atuo.automation.graph")
    except ImportError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="automation.graph is not available",
        ) from exc


def _build_invoke_kwargs(function: Any, **candidates: Any) -> dict[str, Any]:
    signature = inspect.signature(function)
    supports_kwargs = any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )
    if supports_kwargs:
        return candidates
    return {
        name: value
        for name, value in candidates.items()
        if name in signature.parameters
    }


async def _call_graph(function_name: str, **kwargs: Any) -> Any:
    module = _load_graph_module()
    function = getattr(module, function_name, None)
    if function is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"automation.graph.{function_name} is not available",
        )
    result = function(**_build_invoke_kwargs(function, **kwargs))
    if inspect.isawaitable(result):
        return await result
    return result


def _normalize_result(result: Any) -> Any:
    if result is None:
        return None
    if hasattr(result, "model_dump"):
        return result.model_dump(mode="json")
    if isinstance(result, dict):
        return result
    if isinstance(result, (list, str, int, float, bool)):
        return result
    return str(result)


def _derive_status(result: Any) -> str:
    if isinstance(result, dict):
        if result.get("status") in {"queued", "running", "completed", "failed", "blocked"}:
            return str(result["status"])
        if result.get("ok") is False:
            return "failed"
    return "completed"


async def _execute_webhook(
    *,
    request: Request,
    endpoint: str,
    job_type: str,
    function_name: str,
    payload: dict[str, Any],
    requested_job_id: str | None,
) -> WebhookAcceptedResponse:
    accepted_at = datetime.now(timezone.utc)
    storage = get_storage(request)

    try:
        record = await _execute_job(
            storage=storage,
            endpoint=endpoint,
            job_type=job_type,
            function_name=function_name,
            payload=payload,
            requested_job_id=requested_job_id,
        )
        return WebhookAcceptedResponse(
            run_id=record["run_id"],
            job_id=record["job_id"],
            job_type=job_type,
            endpoint=endpoint,
            status=record["status"],
            accepted_at=accepted_at,
            result=record["result"],
        )
    except HTTPException as exc:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="orchestration failed",
        ) from exc


@app.get("/healthz", response_model=HealthResponse)
async def healthz(request: Request) -> HealthResponse:
    status_payload = get_storage(request).healthcheck()
    return HealthResponse(**status_payload)


@app.get("/runs/{run_id}", response_model=RunLookupResponse)
async def get_run(run_id: str, request: Request) -> RunLookupResponse:
    run_payload = get_storage(request).get_run(run_id)
    if run_payload is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="run not found")
    return RunLookupResponse(**run_payload)


@app.post(
    "/hooks/twitter/feed-engage",
    response_model=WebhookAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def feed_engage(payload: FeedEngageRequest, request: Request) -> WebhookAcceptedResponse:
    return await _execute_webhook(
        request=request,
        endpoint="/hooks/twitter/feed-engage",
        job_type="feed_engage",
        function_name="run_feed_engage",
        payload=payload.model_dump(mode="json", exclude_none=True),
        requested_job_id=payload.job_id,
    )


@app.post(
    "/hooks/twitter/repo-post",
    response_model=WebhookAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def repo_post(payload: RepoPostRequest, request: Request) -> WebhookAcceptedResponse:
    return await _execute_webhook(
        request=request,
        endpoint="/hooks/twitter/repo-post",
        job_type="repo_post",
        function_name="run_repo_post",
        payload=payload.model_dump(mode="json", exclude_none=True),
        requested_job_id=payload.job_id,
    )


@app.post(
    "/hooks/twitter/direct-post",
    response_model=WebhookAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def direct_post(payload: DirectPostRequest, request: Request) -> WebhookAcceptedResponse:
    return await _execute_webhook(
        request=request,
        endpoint="/hooks/twitter/direct-post",
        job_type="direct_post",
        function_name="run_direct_post",
        payload=payload.model_dump(mode="json", exclude_none=True),
        requested_job_id=payload.job_id,
    )
