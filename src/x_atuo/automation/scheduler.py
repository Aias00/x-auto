"""Scheduling helpers for automation workflows."""

from __future__ import annotations

import asyncio
import inspect
import logging
import queue
import threading
from typing import Any, Callable, Literal

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from pydantic import BaseModel, Field

from x_atuo.automation.config import SchedulerSettings
from x_atuo.automation.state import AutomationRequest

TriggerKind = Literal["cron", "interval", "date"]
DispatchCallable = Callable[[AutomationRequest], Any]
QueueFullCallable = Callable[[AutomationRequest], Any]
logger = logging.getLogger(__name__)


class ScheduledWorkflow(BaseModel):
    """Serializable definition for a scheduled workflow trigger."""

    job_id: str
    request: AutomationRequest
    trigger: TriggerKind
    trigger_args: dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True
    replace_existing: bool = True


class AutomationScheduler:
    """Thin APScheduler wrapper for workflow dispatch."""

    def __init__(
        self,
        settings: SchedulerSettings,
        dispatcher: DispatchCallable,
        *,
        on_queue_full: QueueFullCallable | None = None,
        scheduler: BackgroundScheduler | None = None,
    ) -> None:
        self._max_backlog = 5
        self.settings = settings
        self.dispatcher = dispatcher
        self.on_queue_full = on_queue_full
        self.scheduler = scheduler or BackgroundScheduler(
            timezone=settings.timezone,
            job_defaults={
                "coalesce": settings.coalesce,
                "misfire_grace_time": settings.misfire_grace_time,
            },
        )
        self._dispatch_queue: queue.Queue[AutomationRequest | None] = queue.Queue(maxsize=self._max_backlog)
        self._worker_thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the underlying scheduler if not already running."""

        self._ensure_worker_started()
        if not self.scheduler.running:
            self.scheduler.start()

    def shutdown(self, *, wait: bool = False) -> None:
        """Stop the scheduler."""

        if self.scheduler.running:
            self.scheduler.shutdown(wait=wait)
        self._stop_worker(wait=wait)

    def maybe_start(self) -> None:
        """Start only when the config explicitly allows it."""

        if self.settings.enabled and self.settings.autostart:
            self.start()

    def register_job(self, definition: ScheduledWorkflow) -> str:
        """Register a workflow job without forcing the scheduler to start."""

        if not definition.enabled:
            return definition.job_id
        self.scheduler.add_job(
            self._dispatch_job,
            id=definition.job_id,
            replace_existing=definition.replace_existing,
            trigger=self._build_trigger(definition.trigger, definition.trigger_args),
            kwargs={"request": definition.request},
        )
        return definition.job_id

    def remove_job(self, job_id: str) -> None:
        """Remove a scheduled job if it exists."""

        self.scheduler.remove_job(job_id)

    def list_job_ids(self) -> list[str]:
        """List registered scheduler job IDs."""

        return [job.id for job in self.scheduler.get_jobs()]

    def _build_trigger(self, trigger: TriggerKind, trigger_args: dict[str, Any]):
        if trigger == "cron":
            return CronTrigger(**trigger_args)
        if trigger == "interval":
            return IntervalTrigger(**trigger_args)
        if trigger == "date":
            return DateTrigger(**trigger_args)
        raise ValueError(f"unsupported trigger type: {trigger}")

    def _dispatch_job(self, request: AutomationRequest) -> None:
        self._ensure_worker_started()
        try:
            self._dispatch_queue.put_nowait(request)
        except queue.Full:
            logger.warning("scheduler backlog full; dropping queued job", extra={"job_name": request.job_name})
            if self.on_queue_full is not None:
                try:
                    result = self.on_queue_full(request)
                    if inspect.isawaitable(result):
                        asyncio.run(result)
                except Exception:
                    logger.exception("scheduler queue-full handler failed", extra={"job_name": request.job_name})

    def _ensure_worker_started(self) -> None:
        if self._worker_thread is not None and self._worker_thread.is_alive():
            return
        self._worker_thread = threading.Thread(
            target=self._worker_loop,
            name="x-atuo-scheduler-worker",
            daemon=True,
        )
        self._worker_thread.start()

    def _stop_worker(self, *, wait: bool) -> None:
        thread = self._worker_thread
        if thread is None:
            return
        if wait:
            self._dispatch_queue.put(None)
        else:
            try:
                self._dispatch_queue.put_nowait(None)
            except queue.Full:
                pass
        thread.join(timeout=5.0 if wait else 0.1)
        if not thread.is_alive():
            self._worker_thread = None

    def _worker_loop(self) -> None:
        while True:
            request = self._dispatch_queue.get()
            try:
                if request is None:
                    return
                try:
                    result = self.dispatcher(request)
                    if inspect.isawaitable(result):
                        asyncio.run(result)
                except Exception:
                    logger.exception("scheduler worker failed to dispatch job", extra={"job_name": request.job_name})
            finally:
                self._dispatch_queue.task_done()
