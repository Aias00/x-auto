from __future__ import annotations

import asyncio
import inspect
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query, Request, status

from x_atuo.automation.author_alpha_storage import AuthorAlphaStorage
from x_atuo.automation.author_alpha_sync import (
    AuthorAlphaSync,
    AuthorAlphaSyncActiveError,
    AuthorAlphaSyncManager,
)
from x_atuo.automation.author_alpha_graph import run_author_alpha_engage
from x_atuo.automation.config import AutomationConfig
from x_atuo.automation.observability import LangfuseRuntime, build_langfuse_runtime
from x_atuo.automation.schemas import (
    AccountAnalyticsResponse,
    AccountContentAnalyticsResponse,
    AuthorAlphaBootstrapRequest,
    AuthorAlphaExecuteRequest,
    AuthorAlphaExecuteResponse,
    AuthorAlphaResetResponse,
    AuthorAlphaReconcileRequest,
    AuthorAlphaRunLookupResponse,
    AuthorAlphaSyncAcceptedResponse,
    AuthorAlphaSyncHistoryResponse,
    AuthorAlphaSyncRunRecord,
    AuthorAlphaSyncStopResponse,
    AuthorAlphaSyncStatusResponse,
    DeviceFollowFeedResponse,
    FeedEngageExecuteRequest,
    FeedEngageExecuteResponse,
    HealthResponse,
    NotificationsResponse,
    RunLookupResponse,
    TwitterBookmarkFoldersResponse,
    TwitterTweetResponse,
    TwitterTweetsResponse,
    TwitterUsersResponse,
)
from x_atuo.automation.scheduler import AutomationScheduler, ScheduledWorkflow
from x_atuo.automation.state import AutomationRequest, FeedOptions, WorkflowKind
from x_atuo.automation.storage import AutomationStorage, utcnow
from x_atuo.core.x_web_analytics import (
    AnalyticsGranularity,
    ContentSortField,
    ContentType,
    SortDirection,
    XWebAnalyticsClient,
    XWebClientConfigError,
    XWebClientError,
    build_account_analytics_snapshot,
    build_account_content_snapshot,
)
from x_atuo.core.x_web_notifications import (
    NotificationsTimelineType,
    XWebNotificationsClient,
    XWebNotificationsConfigError,
    XWebNotificationsError,
    build_device_follow_feed_snapshot,
    build_notifications_snapshot,
)
from x_atuo.core.ai_client import AIProviderError, build_ai_provider
from x_atuo.core.twitter_client import TwitterClient, TwitterClientError


def _resolve_db_path() -> Path:
    return Path(os.getenv("X_ATUO_DB_PATH", "data/x_atuo.sqlite3"))


def _resolve_author_alpha_db_path(settings: AutomationConfig) -> Path:
    return Path(settings.author_alpha.db_path).expanduser()


class _LazyAuthorAlphaSyncManager:
    def __init__(self, *, settings: AutomationConfig, storage: AuthorAlphaStorage) -> None:
        self.settings = settings
        self.storage = storage
        self._manager: AuthorAlphaSyncManager | None = None

    def _build_runtime_manager(self) -> AuthorAlphaSyncManager:
        analytics_client = XWebAnalyticsClient.from_settings(self.settings)
        twitter_client = TwitterClient.from_config(
            self.settings.agent_reach_config_path,
            proxy=self.settings.twitter.proxy_url,
            timeout=max(30, self.settings.author_alpha.posts_per_author * 30),
        )
        sync = AuthorAlphaSync(
            storage=self.storage,
            analytics_client=analytics_client,
            twitter_client=twitter_client,
            timezone=self.settings.author_alpha.timezone,
            excluded_authors=self.settings.author_alpha.excluded_authors,
            score_lookback_days=self.settings.author_alpha.score_lookback_days,
            score_min_daily_replies=self.settings.author_alpha.score_min_daily_replies,
            score_prior_weight=self.settings.author_alpha.score_prior_weight,
            score_penalty_constant=self.settings.author_alpha.score_penalty_constant,
        )
        return AuthorAlphaSyncManager(storage=self.storage, sync=sync)

    def _delegate(self) -> AuthorAlphaSyncManager:
        if self._manager is None:
            self._manager = self._build_runtime_manager()
        return self._manager

    def start_bootstrap(self, **kwargs: Any) -> dict[str, object]:
        return self._delegate().start_bootstrap(**kwargs)

    def start_reconcile(self, **kwargs: Any) -> dict[str, object]:
        return self._delegate().start_reconcile(**kwargs)

    def stop_active_run(self) -> dict[str, object]:
        if self._manager is not None:
            return self._manager.stop_active_run()
        active_run = self.storage.get_active_sync_run()
        if active_run is None:
            raise AuthorAlphaSyncActiveError("no active author-alpha sync run")
        return self._delegate().stop_active_run()

    def get_status(self) -> dict[str, object]:
        active_run = self.storage.get_active_sync_run()
        latest_runs = self.storage.list_sync_runs(limit=1)
        latest_run = latest_runs[0] if latest_runs else None
        return {
            "active": active_run is not None,
            "bootstrap_required": self.storage.count_authors() == 0,
            "active_run": active_run,
            "latest_run": active_run or latest_run,
            "bootstrap_checkpoint": self.storage.read_checkpoint("bootstrap"),
            "reconcile_checkpoint": self.storage.read_checkpoint("reconcile"),
        }

    def list_history(self, *, limit: int = 20) -> list[dict[str, object]]:
        return self.storage.list_sync_runs(limit=limit)

    def get_run(self, run_id: str) -> dict[str, object] | None:
        return self.storage.get_sync_run(run_id)


def _build_author_alpha_sync_manager(
    settings: AutomationConfig,
    storage: AuthorAlphaStorage,
) -> _LazyAuthorAlphaSyncManager:
    return _LazyAuthorAlphaSyncManager(settings=settings, storage=storage)


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
    if request_obj.workflow is WorkflowKind.AUTHOR_ALPHA_ENGAGE:
        metadata = request_obj.metadata if isinstance(request_obj.metadata, dict) else {}
        payload = {
            "mode": request_obj.approval_mode,
            "dry_run": request_obj.dry_run,
            "metadata": metadata,
            "idempotency_key": request_obj.idempotency_key,
            "proxy": metadata.get("proxy"),
        }
        return "author_alpha_engage", "run_author_alpha_engage", payload, "scheduler:author-alpha-engage"
    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="unsupported scheduled workflow")


async def _execute_job(
    *,
    storage: AutomationStorage,
    endpoint: str,
    job_type: str,
    function_name: str,
    payload: dict[str, Any],
    requested_job_id: str | None,
    request_obj: AutomationRequest | None = None,
    observability_runtime: Any | None = None,
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
        if request_obj is not None:
            request_for_run = request_obj.model_copy(update={"run_id": run_id, "job_name": job_id})
            proxy = request_for_run.metadata.get("proxy") if isinstance(request_for_run.metadata, dict) else None
            result = await _run_request(
                request_for_run,
                storage=storage,
                endpoint=endpoint,
                proxy=proxy,
                observability_runtime=observability_runtime,
            )
        else:
            result = await _call_graph(
                function_name,
                run_id=run_id,
                job_id=job_id,
                endpoint=endpoint,
                payload=payload,
                storage=storage,
                observability_runtime=observability_runtime,
            )
        normalized_result = _normalize_result(result)
        run_status = _derive_status(normalized_result)
        error_message = _derive_error_message(
            normalized_result,
            run_status=run_status,
            current_error=error_message,
        )
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


async def _run_author_alpha_request(
    request: AutomationRequest,
    *,
    settings: AutomationConfig,
    storage: AuthorAlphaStorage,
    shared_storage: AutomationStorage,
    endpoint: str,
    proxy: str | None = None,
) -> dict[str, Any]:
    notifications_client = XWebNotificationsClient.from_settings(settings)
    twitter_client = TwitterClient.from_config(
        settings.agent_reach_config_path,
        proxy=proxy or settings.twitter.proxy_url,
        twitter_bin=settings.twitter.cli_bin,
        timeout=120,
    )
    ai_provider = build_ai_provider(settings.ai)
    if ai_provider is None:
        raise AIProviderError("author-alpha-engage requires an AI provider")
    snapshot = await run_author_alpha_engage(
        request,
        config=settings,
        storage=storage,
        shared_storage=shared_storage,
        candidate_source=notifications_client,
        drafter=ai_provider,
        reply_client=twitter_client,
        sleep=asyncio.sleep,
    )
    return _snapshot_response(snapshot)


async def _execute_author_alpha_job(
    *,
    settings: AutomationConfig,
    storage: AuthorAlphaStorage,
    shared_storage: AutomationStorage,
    endpoint: str,
    job_type: str,
    payload: dict[str, Any],
    requested_job_id: str | None,
    request_obj: AutomationRequest,
) -> dict[str, Any]:
    run_id = str(uuid4())
    job_id = requested_job_id or f"{job_type}-{run_id}"
    normalized_result: Any = None
    run_status = "failed"
    error_message: str | None = None

    storage.create_execution_run(
        run_id=run_id,
        job_id=job_id,
        job_type=job_type,
        endpoint=endpoint,
        request_payload=payload,
    )
    storage.add_execution_audit_event(
        run_id=run_id,
        event_type="trigger_received",
        node="service",
        payload={"endpoint": endpoint, "job_id": job_id},
    )
    storage.update_execution_run(run_id, status="running", started_at=utcnow())

    try:
        request_for_run = request_obj.model_copy(update={"run_id": run_id, "job_name": job_id})
        proxy = request_for_run.metadata.get("proxy") if isinstance(request_for_run.metadata, dict) else None
        normalized_result = await _run_author_alpha_request(
            request_for_run,
            settings=settings,
            storage=storage,
            shared_storage=shared_storage,
            endpoint=endpoint,
            proxy=proxy,
        )
        run_status = _derive_status(normalized_result)
        error_message = _derive_error_message(
            normalized_result,
            run_status=run_status,
            current_error=error_message,
        )
    except Exception as exc:
        error_message = str(exc)
        normalized_result = {"status": "failed", "error": error_message}

    storage.update_execution_run(
        run_id,
        status=run_status,
        response_payload=normalized_result,
        error=error_message,
        finished_at=utcnow(),
    )
    storage.add_execution_audit_event(
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


async def _dispatch_scheduled_request(
    request_obj: AutomationRequest,
    storage: AutomationStorage,
    author_alpha_storage: AuthorAlphaStorage | None = None,
    *,
    settings: AutomationConfig | None = None,
    observability_runtime: Any | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or AutomationConfig()
    job_type, function_name, payload, endpoint = _workflow_binding(request_obj)
    if request_obj.workflow is WorkflowKind.AUTHOR_ALPHA_ENGAGE:
        if author_alpha_storage is None:
            author_alpha_storage = AuthorAlphaStorage(_resolve_author_alpha_db_path(resolved_settings))
            author_alpha_storage.initialize()
        return await _execute_author_alpha_job(
            settings=resolved_settings,
            storage=author_alpha_storage,
            shared_storage=storage,
            endpoint=endpoint,
            job_type=job_type,
            payload=payload,
            requested_job_id=request_obj.job_name,
            request_obj=request_obj,
        )
    return await _execute_job(
        storage=storage,
        endpoint=endpoint,
        job_type=job_type,
        function_name=function_name,
        payload=payload,
        requested_job_id=request_obj.job_name,
        request_obj=request_obj,
        observability_runtime=observability_runtime,
    )


def _record_dropped_scheduled_request(
    request_obj: AutomationRequest,
    storage: AutomationStorage,
    *,
    author_alpha_storage: AuthorAlphaStorage | None = None,
    reason: str,
) -> str:
    job_type, _function_name, payload, endpoint = _workflow_binding(request_obj)
    if request_obj.workflow is WorkflowKind.AUTHOR_ALPHA_ENGAGE:
        if author_alpha_storage is None:
            raise RuntimeError("author-alpha storage is required to record dropped author-alpha requests")
        run_id = str(uuid4())
        job_id = request_obj.job_name or f"{job_type}-{run_id}"
        author_alpha_storage.create_execution_run(
            run_id=run_id,
            job_id=job_id,
            job_type=job_type,
            endpoint=endpoint,
            request_payload=payload,
            status="blocked",
        )
        author_alpha_storage.add_execution_audit_event(
            run_id=run_id,
            event_type="scheduler_queue_dropped",
            node="service",
            payload={"endpoint": endpoint, "job_id": job_id, "reason": reason},
        )
        author_alpha_storage.update_execution_run(
            run_id,
            status="blocked",
            response_payload={"status": "blocked", "error": reason},
            error=reason,
            finished_at=utcnow(),
        )
        return run_id
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
    request_obj = AutomationRequest.for_feed_engage(
        job_name="scheduled-feed-engage",
        dry_run=False,
        approval_mode="ai_auto",
        reply_text=None,
        feed_options=FeedOptions(
            feed_type=settings.twitter.default_feed_type,
            feed_count=settings.twitter.default_feed_count,
        ),
        metadata={"proxy": settings.twitter.proxy_url, "trigger": "scheduler"},
    )
    return ScheduledWorkflow(
        job_id="scheduled-feed-engage",
        request=request_obj,
        trigger=trigger,
        trigger_args=trigger_args,
        enabled=True,
    )


def _build_scheduled_author_alpha(settings: AutomationConfig) -> ScheduledWorkflow | None:
    if not (settings.scheduler.enabled and settings.author_alpha.enabled):
        return None
    trigger = settings.author_alpha.trigger
    trigger_args: dict[str, Any]
    if trigger == "interval":
        trigger_args = {
            "seconds": settings.author_alpha.seconds,
            "jitter": settings.author_alpha.jitter_seconds,
        }
    else:
        trigger_args = {"jitter": settings.author_alpha.jitter_seconds}
        if settings.author_alpha.minute is not None:
            trigger_args["minute"] = settings.author_alpha.minute
        if settings.author_alpha.hour is not None:
            trigger_args["hour"] = settings.author_alpha.hour
    request_obj = AutomationRequest.for_author_alpha_engage(
        job_name="scheduled-author-alpha-engage",
        dry_run=False,
        approval_mode="ai_auto",
        metadata={"proxy": settings.twitter.proxy_url, "trigger": "scheduler"},
    )
    return ScheduledWorkflow(
        job_id="scheduled-author-alpha-engage",
        request=request_obj,
        trigger=trigger,
        trigger_args=trigger_args,
        enabled=True,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = AutomationConfig()
    storage = AutomationStorage(_resolve_db_path())
    author_alpha_storage = AuthorAlphaStorage(_resolve_author_alpha_db_path(settings))
    observability_runtime = build_langfuse_runtime(settings)
    storage.initialize()
    author_alpha_storage.initialize()
    storage.clear_stale_running_runs(reason="stale running cleared on service startup")
    app.state.storage = storage
    app.state.author_alpha_storage = author_alpha_storage
    app.state.settings = settings
    app.state.observability_runtime = observability_runtime
    app.state.author_alpha_sync_manager = _build_author_alpha_sync_manager(settings, author_alpha_storage)

    scheduler = AutomationScheduler(
        settings.scheduler,
        lambda request_obj: _dispatch_scheduled_request(
            request_obj,
            storage,
            author_alpha_storage,
            settings=settings,
            observability_runtime=observability_runtime,
        ),
        on_queue_full=lambda request_obj: _record_dropped_scheduled_request(
            request_obj,
            storage,
            author_alpha_storage=author_alpha_storage,
            reason="scheduler backlog full",
        ),
    )
    definitions = [
        ("scheduled_feed_engage", _build_scheduled_feed_engage(settings)),
        ("scheduled_author_alpha_engage", _build_scheduled_author_alpha(settings)),
    ]
    for attr_name, definition in definitions:
        if definition is None:
            continue
        scheduler.register_job(definition)
        setattr(app.state, attr_name, definition)
    app.state.scheduler = scheduler
    scheduler.maybe_start()
    try:
        yield
    finally:
        try:
            scheduler.shutdown(wait=False)
        finally:
            observability_runtime.shutdown()


app = FastAPI(title="x-atuo automation API", lifespan=lifespan)


def get_storage(request: Request) -> AutomationStorage:
    return request.app.state.storage


def get_author_alpha_storage(request: Request) -> AuthorAlphaStorage:
    return request.app.state.author_alpha_storage


def get_author_alpha_sync_manager(request: Request) -> AuthorAlphaSyncManager:
    return request.app.state.author_alpha_sync_manager


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


def _build_runtime_graph(config: AutomationConfig, storage: Any, *, proxy: str | None = None) -> Any:
    module = _load_graph_module()
    builder = getattr(module, "_build_runtime_graph", None)
    if builder is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="automation.graph._build_runtime_graph is not available",
        )
    return builder(config, storage, proxy=proxy)


def _persist_snapshot(storage: Any, snapshot: Any) -> None:
    module = _load_graph_module()
    persist = getattr(module, "_persist_snapshot", None)
    if persist is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="automation.graph._persist_snapshot is not available",
        )
    persist(storage, snapshot)


def _workflow_observation_metadata(
    request_obj: AutomationRequest,
    config: AutomationConfig,
    *,
    endpoint: str,
) -> dict[str, Any]:
    return {
        "run_id": request_obj.run_id,
        "job_id": request_obj.job_name,
        "workflow": request_obj.workflow.value,
        "endpoint": endpoint,
        "dry_run": request_obj.dry_run,
        "approval_mode": request_obj.approval_mode,
        "environment": config.environment,
    }


def _snapshot_response(snapshot: Any) -> dict[str, Any]:
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


def _workflow_failure_marker(snapshot: Any) -> Exception | None:
    status_value = getattr(getattr(snapshot, "status", None), "value", None)
    if status_value != "failed":
        return None

    errors = getattr(snapshot, "errors", None)
    if isinstance(errors, list):
        messages = [str(item).strip() for item in errors if str(item).strip()]
        if messages:
            return RuntimeError("; ".join(messages))
    return RuntimeError("workflow ended with failed status")


async def _run_request(
    request: AutomationRequest,
    *,
    storage: Any,
    endpoint: str,
    proxy: str | None = None,
    observability_runtime: Any | None = None,
) -> dict[str, Any]:
    config = AutomationConfig()
    runtime = observability_runtime if observability_runtime is not None else LangfuseRuntime()
    graph = _build_runtime_graph(config, storage, proxy=proxy)
    run_name = f"x-atuo.{request.workflow.value}"
    observation = runtime.start_workflow_observation(
        run_name=run_name,
        metadata=_workflow_observation_metadata(request, config, endpoint=endpoint),
    )
    graph_config = runtime.build_graph_config(run_name=run_name, observation=observation)

    snapshot: Any | None = None
    error: Exception | None = None
    try:
        snapshot = await graph.invoke(request, graph_config=graph_config)
        error = _workflow_failure_marker(snapshot)
        _persist_snapshot(storage, snapshot)
        return _snapshot_response(snapshot)
    except Exception as exc:
        error = exc
        raise
    finally:
        runtime.finish_workflow_observation(
            observation,
            output=None if snapshot is None else {"status": snapshot.status.value, "run_id": snapshot.run_id},
            error=error,
        )


async def _call_graph(function_name: str, **kwargs: Any) -> Any:
    module = _load_graph_module()
    bind_request = getattr(module, "build_request_binding", None)
    request_binding = None
    if callable(bind_request):
        request_binding = bind_request(
            function_name,
            run_id=kwargs["run_id"],
            job_id=kwargs["job_id"],
            payload=kwargs["payload"],
        )
    if request_binding is not None:
        request_obj, proxy = request_binding
        return await _run_request(
            request_obj,
            storage=kwargs["storage"],
            endpoint=kwargs["endpoint"],
            proxy=proxy,
            observability_runtime=kwargs.get("observability_runtime"),
        )

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


def _derive_error_message(result: Any, *, run_status: str, current_error: str | None) -> str | None:
    if current_error is not None:
        return current_error
    if run_status != "blocked" or not isinstance(result, dict):
        return None
    errors = result.get("errors")
    if not isinstance(errors, list):
        return None
    messages = [str(item).strip() for item in errors if str(item).strip()]
    return messages[0] if messages else None


def _get_account_analytics_snapshot(
    *,
    request: Request,
    days: int,
    post_limit: int,
    granularity: AnalyticsGranularity,
) -> dict[str, Any]:
    settings = request.app.state.settings
    client = XWebAnalyticsClient.from_settings(settings)
    return build_account_analytics_snapshot(
        client,
        days=days,
        post_limit=post_limit,
        granularity=granularity,
    )


def _get_account_content_snapshot(
    *,
    request: Request,
    from_date: str | None,
    to_date: str | None,
    content_type: ContentType,
    sort_field: ContentSortField,
    sort_direction: SortDirection,
    limit: int,
) -> dict[str, Any]:
    settings = request.app.state.settings
    client = XWebAnalyticsClient.from_settings(settings)
    return build_account_content_snapshot(
        client,
        from_date=from_date,
        to_date=to_date,
        content_type=content_type,
        sort_field=sort_field,
        sort_direction=sort_direction,
        limit=limit,
    )


def _get_notifications_snapshot(
    *,
    request: Request,
    timeline_type: NotificationsTimelineType,
    count: int,
    cursor: str | None,
) -> dict[str, Any]:
    settings = request.app.state.settings
    client = XWebNotificationsClient.from_settings(settings)
    return build_notifications_snapshot(
        client,
        timeline_type=timeline_type,
        count=count,
        cursor=cursor,
    )


def _get_device_follow_feed_snapshot(
    *,
    request: Request,
    count: int,
) -> dict[str, Any]:
    settings = request.app.state.settings
    client = XWebNotificationsClient.from_settings(settings)
    return build_device_follow_feed_snapshot(
        client,
        count=count,
    )


def _build_runtime_twitter_client(settings: AutomationConfig) -> TwitterClient:
    return TwitterClient.from_config(
        settings.agent_reach_config_path,
        proxy=settings.twitter.proxy_url,
        twitter_bin=settings.twitter.cli_bin,
        timeout=120,
    )


def _handle_twitter_read_error(exc: Exception) -> HTTPException:
    message = str(exc)
    lowered = message.lower()
    if "twitter auth_token" in lowered or "twitter_ct0" in lowered or "not configured" in lowered:
        return HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=message)
    return HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=message)


def _normalize_tweet_record(tweet: Any) -> dict[str, Any]:
    if hasattr(tweet, "model_dump"):
        return tweet.model_dump(mode="json")
    if isinstance(tweet, dict):
        return dict(tweet)
    author = getattr(tweet, "author", None)
    return {
        "tweet_id": getattr(tweet, "tweet_id", None),
        "text": getattr(tweet, "text", None),
        "created_at": getattr(tweet, "created_at", None),
        "article_title": getattr(tweet, "article_title", None),
        "article_text": getattr(tweet, "article_text", None),
        "conversation_id": getattr(tweet, "conversation_id", None),
        "reply_to_tweet_id": getattr(tweet, "reply_to_tweet_id", None),
        "reply_to_screen_name": getattr(tweet, "reply_to_screen_name", None),
        "target_tweet_id": getattr(tweet, "target_tweet_id", None),
        "target_screen_name": getattr(tweet, "target_screen_name", None),
        "can_reply": getattr(tweet, "can_reply", None),
        "reply_limit_reason": getattr(tweet, "reply_limit_reason", None),
        "reply_limit_headline": getattr(tweet, "reply_limit_headline", None),
        "reply_restriction_policy": getattr(tweet, "reply_restriction_policy", None),
        "screen_name": getattr(tweet, "screen_name", None),
        "verified": getattr(tweet, "verified", None),
        "author": None
        if author is None
        else {
            "screen_name": getattr(author, "screen_name", None),
            "verified": getattr(author, "verified", None),
            "name": getattr(author, "name", None),
            "user_id": getattr(author, "user_id", None),
        },
        "raw": getattr(tweet, "raw", {}),
    }


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


@app.get("/analytics/account", response_model=AccountAnalyticsResponse)
async def get_account_analytics(
    request: Request,
    days: int = Query(default=28, ge=1, le=365),
    post_limit: int = Query(default=10, ge=1, le=100),
    granularity: AnalyticsGranularity = Query(default="total"),
) -> AccountAnalyticsResponse:
    try:
        payload = _get_account_analytics_snapshot(
            request=request,
            days=days,
            post_limit=post_limit,
            granularity=granularity,
        )
    except XWebClientConfigError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except XWebClientError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
    return AccountAnalyticsResponse(**payload)


@app.get("/analytics/account/content", response_model=AccountContentAnalyticsResponse)
async def get_account_analytics_content(
    request: Request,
    content_type: ContentType = Query(default="all", alias="type"),
    sort_field: ContentSortField = Query(default="date", alias="sort"),
    sort_direction: SortDirection = Query(default="desc", alias="dir"),
    from_date: str | None = Query(default=None, alias="from"),
    to_date: str | None = Query(default=None, alias="to"),
    limit: int = Query(default=50, ge=1, le=500),
) -> AccountContentAnalyticsResponse:
    try:
        payload = _get_account_content_snapshot(
            request=request,
            from_date=from_date,
            to_date=to_date,
            content_type=content_type,
            sort_field=sort_field,
            sort_direction=sort_direction,
            limit=limit,
        )
    except XWebClientConfigError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except XWebClientError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
    return AccountContentAnalyticsResponse(**payload)


@app.get("/notifications", response_model=NotificationsResponse)
async def get_notifications(
    request: Request,
    timeline_type: NotificationsTimelineType = Query(default="All"),
    count: int = Query(default=20, ge=1, le=100),
    cursor: str | None = Query(default=None),
) -> NotificationsResponse:
    try:
        payload = _get_notifications_snapshot(
            request=request,
            timeline_type=timeline_type,
            count=count,
            cursor=cursor,
        )
    except XWebNotificationsConfigError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except XWebNotificationsError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
    return NotificationsResponse(**payload)


@app.get("/notifications/device-follow-feed", response_model=DeviceFollowFeedResponse)
async def get_device_follow_feed(
    request: Request,
    count: int = Query(default=20, ge=1, le=100),
) -> DeviceFollowFeedResponse:
    try:
        payload = _get_device_follow_feed_snapshot(
            request=request,
            count=count,
        )
    except XWebNotificationsConfigError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except XWebNotificationsError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
    return DeviceFollowFeedResponse(**payload)


@app.get("/twitter/search", response_model=TwitterTweetsResponse)
async def get_twitter_search(
    request: Request,
    q: str = Query(..., min_length=1),
    limit: int = Query(default=20, ge=1, le=100),
    product: str = Query(default="Top"),
) -> TwitterTweetsResponse:
    client = _build_runtime_twitter_client(request.app.state.settings)
    try:
        items = [_normalize_tweet_record(tweet) for tweet in client.fetch_search(q, max_items=limit, product=product)]
    except TwitterClientError as exc:
        raise _handle_twitter_read_error(exc) from exc
    return TwitterTweetsResponse(count=len(items), items=items)


@app.get("/twitter/bookmarks", response_model=TwitterTweetsResponse)
async def get_twitter_bookmarks(
    request: Request,
    limit: int = Query(default=50, ge=1, le=100),
) -> TwitterTweetsResponse:
    client = _build_runtime_twitter_client(request.app.state.settings)
    try:
        items = [_normalize_tweet_record(tweet) for tweet in client.fetch_bookmarks(max_items=limit)]
    except TwitterClientError as exc:
        raise _handle_twitter_read_error(exc) from exc
    return TwitterTweetsResponse(count=len(items), items=items)


@app.get("/twitter/bookmarks/folders", response_model=TwitterBookmarkFoldersResponse)
async def get_twitter_bookmark_folders(request: Request) -> TwitterBookmarkFoldersResponse:
    client = _build_runtime_twitter_client(request.app.state.settings)
    try:
        items = client.fetch_bookmark_folders()
    except TwitterClientError as exc:
        raise _handle_twitter_read_error(exc) from exc
    return TwitterBookmarkFoldersResponse(count=len(items), items=items)


@app.get("/twitter/bookmarks/folders/{folder_id}", response_model=TwitterTweetsResponse)
async def get_twitter_bookmark_folder_posts(
    request: Request,
    folder_id: str,
    limit: int = Query(default=50, ge=1, le=100),
) -> TwitterTweetsResponse:
    client = _build_runtime_twitter_client(request.app.state.settings)
    try:
        items = [_normalize_tweet_record(tweet) for tweet in client.fetch_bookmark_folder_posts(folder_id, max_items=limit)]
    except TwitterClientError as exc:
        raise _handle_twitter_read_error(exc) from exc
    return TwitterTweetsResponse(count=len(items), items=items)


@app.get("/twitter/users/{screen_name}/likes", response_model=TwitterTweetsResponse)
async def get_twitter_user_likes(
    request: Request,
    screen_name: str,
    limit: int = Query(default=20, ge=1, le=100),
) -> TwitterTweetsResponse:
    client = _build_runtime_twitter_client(request.app.state.settings)
    try:
        items = [_normalize_tweet_record(tweet) for tweet in client.fetch_user_likes(screen_name, max_items=limit)]
    except TwitterClientError as exc:
        raise _handle_twitter_read_error(exc) from exc
    return TwitterTweetsResponse(count=len(items), items=items)


@app.get("/twitter/users/{screen_name}/followers", response_model=TwitterUsersResponse)
async def get_twitter_followers(
    request: Request,
    screen_name: str,
    limit: int = Query(default=20, ge=1, le=100),
) -> TwitterUsersResponse:
    client = _build_runtime_twitter_client(request.app.state.settings)
    try:
        items = client.fetch_followers(screen_name, max_items=limit)
    except TwitterClientError as exc:
        raise _handle_twitter_read_error(exc) from exc
    return TwitterUsersResponse(count=len(items), items=items)


@app.get("/twitter/users/{screen_name}/following", response_model=TwitterUsersResponse)
async def get_twitter_following(
    request: Request,
    screen_name: str,
    limit: int = Query(default=20, ge=1, le=100),
) -> TwitterUsersResponse:
    client = _build_runtime_twitter_client(request.app.state.settings)
    try:
        items = client.fetch_following(screen_name, max_items=limit)
    except TwitterClientError as exc:
        raise _handle_twitter_read_error(exc) from exc
    return TwitterUsersResponse(count=len(items), items=items)


@app.get("/twitter/articles/{tweet_id}", response_model=TwitterTweetResponse)
async def get_twitter_article(request: Request, tweet_id: str) -> TwitterTweetResponse:
    client = _build_runtime_twitter_client(request.app.state.settings)
    try:
        tweet = _normalize_tweet_record(client.fetch_article(tweet_id))
    except TwitterClientError as exc:
        raise _handle_twitter_read_error(exc) from exc
    return TwitterTweetResponse(tweet=tweet)


@app.post("/feed-engage/execute", response_model=FeedEngageExecuteResponse)
async def post_feed_engage_execute(
    request: Request,
    body: FeedEngageExecuteRequest,
) -> FeedEngageExecuteResponse:
    settings = request.app.state.settings
    storage = get_storage(request)
    request_obj = AutomationRequest.for_feed_engage(
        job_name="manual-feed-engage",
        dry_run=body.dry_run,
        reply_text=body.reply_text,
        feed_options=FeedOptions(feed_count=body.feed_count, feed_type=body.feed_type),
        approval_mode="ai_auto",
        metadata={
            "proxy": settings.twitter.proxy_url,
            "trigger": "manual",
        },
    )
    job_type, function_name, payload, _endpoint = _workflow_binding(request_obj)
    result = await _execute_job(
        storage=storage,
        endpoint="manual:feed-engage",
        job_type=job_type,
        function_name=function_name,
        payload=payload,
        requested_job_id=request_obj.job_name,
        request_obj=request_obj,
        observability_runtime=request.app.state.observability_runtime,
    )
    return FeedEngageExecuteResponse(**result)

@app.post("/author-alpha/sync/bootstrap", response_model=AuthorAlphaSyncAcceptedResponse, status_code=status.HTTP_202_ACCEPTED)
async def post_author_alpha_sync_bootstrap(
    request: Request,
    body: AuthorAlphaBootstrapRequest,
) -> AuthorAlphaSyncAcceptedResponse:
    manager = get_author_alpha_sync_manager(request)
    try:
        payload = manager.start_bootstrap(
            from_date=body.from_date,
            to_date=body.to_date,
            resume=body.resume,
            max_days=body.max_days,
        )
    except AuthorAlphaSyncActiveError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return AuthorAlphaSyncAcceptedResponse(**payload)


@app.post("/author-alpha/sync/reconcile", response_model=AuthorAlphaSyncAcceptedResponse, status_code=status.HTTP_202_ACCEPTED)
async def post_author_alpha_sync_reconcile(
    request: Request,
    body: AuthorAlphaReconcileRequest,
) -> AuthorAlphaSyncAcceptedResponse:
    manager = get_author_alpha_sync_manager(request)
    try:
        payload = manager.start_reconcile(target_date=body.target_date)
    except AuthorAlphaSyncActiveError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return AuthorAlphaSyncAcceptedResponse(**payload)


@app.post("/author-alpha/sync/stop", response_model=AuthorAlphaSyncStopResponse, status_code=status.HTTP_202_ACCEPTED)
async def post_author_alpha_sync_stop(request: Request) -> AuthorAlphaSyncStopResponse:
    manager = get_author_alpha_sync_manager(request)
    try:
        payload = manager.stop_active_run()
    except AuthorAlphaSyncActiveError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return AuthorAlphaSyncStopResponse(**payload)


@app.get("/author-alpha/sync/status", response_model=AuthorAlphaSyncStatusResponse)
async def get_author_alpha_sync_status(request: Request) -> AuthorAlphaSyncStatusResponse:
    payload = get_author_alpha_sync_manager(request).get_status()
    return AuthorAlphaSyncStatusResponse(**payload)


@app.get("/author-alpha/sync/history", response_model=AuthorAlphaSyncHistoryResponse)
async def get_author_alpha_sync_history(
    request: Request,
    limit: int = Query(default=20, ge=1, le=200),
) -> AuthorAlphaSyncHistoryResponse:
    runs = get_author_alpha_sync_manager(request).list_history(limit=limit)
    return AuthorAlphaSyncHistoryResponse(runs=[AuthorAlphaSyncRunRecord(**run) for run in runs])


@app.get("/author-alpha/runs/{run_id}", response_model=AuthorAlphaRunLookupResponse)
async def get_author_alpha_run(run_id: str, request: Request) -> AuthorAlphaRunLookupResponse:
    execution_payload = get_author_alpha_storage(request).get_execution_run(run_id)
    if execution_payload is not None:
        return AuthorAlphaRunLookupResponse(**execution_payload)
    sync_payload = get_author_alpha_sync_manager(request).get_run(run_id)
    if sync_payload is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="run not found")
    return AuthorAlphaRunLookupResponse(run=sync_payload, audit_events=[])


@app.post("/author-alpha/reset", response_model=AuthorAlphaResetResponse)
async def post_author_alpha_reset(request: Request) -> AuthorAlphaResetResponse:
    manager = get_author_alpha_sync_manager(request)
    if manager.get_status().get("active"):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="author-alpha sync run already active")
    get_author_alpha_storage(request).reset_all()
    return AuthorAlphaResetResponse(status="cleared")


@app.post("/author-alpha/execute", response_model=AuthorAlphaExecuteResponse)
async def post_author_alpha_execute(
    request: Request,
    body: AuthorAlphaExecuteRequest,
) -> AuthorAlphaExecuteResponse:
    settings = request.app.state.settings
    storage = get_author_alpha_storage(request)
    request_obj = AutomationRequest.for_author_alpha_engage(
        job_name="manual-author-alpha-engage",
        dry_run=body.dry_run,
        metadata={
            "proxy": settings.twitter.proxy_url,
            "trigger": "manual",
        },
    )
    job_type, _function_name, payload, _endpoint = _workflow_binding(request_obj)
    result = await _execute_author_alpha_job(
        settings=settings,
        storage=storage,
        shared_storage=get_storage(request),
        endpoint="manual:author-alpha-engage",
        job_type=job_type,
        payload=payload,
        requested_job_id=request_obj.job_name,
        request_obj=request_obj,
    )
    return AuthorAlphaExecuteResponse(**result)
