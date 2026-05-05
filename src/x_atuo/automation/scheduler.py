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
        self._dispatch_queues: dict[str, queue.Queue[AutomationRequest | None]] = {}
        self._worker_threads: dict[str, threading.Thread] = {}
        self._worker_lock = threading.Lock()

    def start(self) -> None:
        """Start the underlying scheduler if not already running."""

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
        try:
            lane_key = self._lane_key(request)
            dispatch_queue = self._ensure_lane(lane_key)
            dispatch_queue.put_nowait(request)
        except queue.Full:
            logger.warning("scheduler backlog full; dropping queued job", extra={"job_name": request.job_name})
            if self.on_queue_full is not None:
                try:
                    result = self.on_queue_full(request)
                    if inspect.isawaitable(result):
                        asyncio.run(result)
                except Exception:
                    logger.exception("scheduler queue-full handler failed", extra={"job_name": request.job_name})

    def _lane_key(self, request: AutomationRequest) -> str:
        return request.workflow.value

    def _ensure_lane(self, lane_key: str) -> queue.Queue[AutomationRequest | None]:
        with self._worker_lock:
            dispatch_queue = self._dispatch_queues.get(lane_key)
            if dispatch_queue is None:
                dispatch_queue = queue.Queue(maxsize=self._max_backlog)
                self._dispatch_queues[lane_key] = dispatch_queue

            worker_thread = self._worker_threads.get(lane_key)
            if worker_thread is None or not worker_thread.is_alive():
                worker_thread = threading.Thread(
                    target=self._worker_loop,
                    args=(lane_key, dispatch_queue),
                    name=f"x-atuo-scheduler-worker-{lane_key}",
                    daemon=True,
                )
                self._worker_threads[lane_key] = worker_thread
                worker_thread.start()
            return dispatch_queue

    def _stop_worker(self, *, wait: bool) -> None:
        with self._worker_lock:
            lane_items = list(self._dispatch_queues.items())
            worker_items = list(self._worker_threads.items())

        for lane_key, dispatch_queue in lane_items:
            if wait:
                dispatch_queue.put(None)
            else:
                try:
                    dispatch_queue.put_nowait(None)
                except queue.Full:
                    pass

        for lane_key, worker_thread in worker_items:
            worker_thread.join(timeout=5.0 if wait else 0.1)
            if not worker_thread.is_alive():
                with self._worker_lock:
                    self._worker_threads.pop(lane_key, None)
                    self._dispatch_queues.pop(lane_key, None)

    def _worker_loop(self, lane_key: str, dispatch_queue: queue.Queue[AutomationRequest | None]) -> None:
        while True:
            request = dispatch_queue.get()
            try:
                if request is None:
                    with self._worker_lock:
                        self._worker_threads.pop(lane_key, None)
                        self._dispatch_queues.pop(lane_key, None)
                    return
                try:
                    result = self.dispatcher(request)
                    if inspect.isawaitable(result):
                        asyncio.run(result)
                except Exception:
                    logger.exception("scheduler worker failed to dispatch job", extra={"job_name": request.job_name})
            finally:
                dispatch_queue.task_done()
