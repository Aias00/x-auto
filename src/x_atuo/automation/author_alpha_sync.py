"""Author alpha reply analytics bootstrap and reconcile helpers."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
import threading
import math
import re
from typing import Any, Protocol
from uuid import uuid4
from zoneinfo import ZoneInfo

from x_atuo.core.twitter_models import TweetRecord


class AnalyticsClientProtocol(Protocol):
    def fetch_content_posts(
        self,
        *,
        start_time: str,
        end_time: str,
        post_limit: int,
        query_page_size: int | None = None,
    ) -> dict[str, object]: ...


class TwitterReadClientProtocol(Protocol):
    def fetch_tweet(self, tweet_id: str) -> TweetRecord: ...


class AuthorAlphaStorageProtocol(Protocol):
    def count_authors(self) -> int: ...

    def record_sync_run(self, **values: Any) -> None: ...

    def update_sync_run(self, run_id: str, **updates: Any) -> None: ...

    def read_checkpoint(self, sync_scope: str) -> dict[str, object] | None: ...

    def write_checkpoint(self, **values: Any) -> None: ...

    def upsert_reply_daily_metrics(self, **values: Any) -> None: ...

    def upsert_author_daily_rollup(self, **values: Any) -> None: ...

    def replace_day_sync_snapshot(
        self,
        *,
        metric_date: str,
        reply_metrics: list[dict[str, object]],
        author_rollups: list[dict[str, object]],
    ) -> None: ...

    def list_author_daily_rollups(self, start_date: str, end_date: str) -> list[dict[str, object]]: ...

    def list_reply_daily_metrics(self, start_date: str, end_date: str) -> list[dict[str, object]]: ...

    def upsert_author(self, **values: Any) -> None: ...

    def zero_out_stale_authors(self, scored_screen_names: set[str], *, scored_at: str) -> int: ...

    def get_active_sync_run(self) -> dict[str, object] | None: ...

    def list_sync_runs(self, *, limit: int = 20) -> list[dict[str, object]]: ...

    def get_sync_run(self, run_id: str) -> dict[str, object] | None: ...


@dataclass(slots=True)
class _AuthorAccumulator:
    reply_count: int = 0
    impressions_total: int = 0
    likes_total: int = 0
    replies_total: int = 0
    reposts_total: int = 0
    max_impressions: int = 0

    @property
    def avg_impressions(self) -> float:
        if self.reply_count <= 0:
            return 0.0
        return self.impressions_total / self.reply_count


class AuthorAlphaSyncActiveError(RuntimeError):
    """Raised when a second sync run is requested while one is already active."""


class AuthorAlphaSyncCancellationError(RuntimeError):
    """Raised when a sync run is cancelled by operator request."""


class AuthorAlphaSyncManager:
    """Single-flight background execution manager for bootstrap/reconcile sync runs."""

    def __init__(self, *, storage: AuthorAlphaStorageProtocol, sync: "AuthorAlphaSync") -> None:
        self.storage = storage
        self.sync = sync
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._active_run_id: str | None = None
        self._active_run_type: str | None = None
        self._cancel_event: threading.Event | None = None

    def start_bootstrap(
        self,
        *,
        from_date: str,
        to_date: str,
        resume: bool = False,
        max_days: int | None = None,
    ) -> dict[str, object]:
        run_id = f"bootstrap-{uuid4().hex}"
        self._start_thread(
            run_id=run_id,
            run_type="bootstrap",
            target=self.sync.bootstrap,
            kwargs={
                "from_date": from_date,
                "to_date": to_date,
                "resume": resume,
                "max_days": max_days,
                "run_id": run_id,
            },
        )
        return {
            "run_id": run_id,
            "run_type": "bootstrap",
            "status": "accepted",
            "from_date": from_date,
            "to_date": to_date,
            "resume": resume,
            "max_days": max_days,
        }

    def start_reconcile(self, *, target_date: str | None = None) -> dict[str, object]:
        run_id = f"reconcile-{uuid4().hex}"
        self._start_thread(
            run_id=run_id,
            run_type="reconcile",
            target=self.sync.reconcile,
            kwargs={
                "target_date": target_date,
                "run_id": run_id,
            },
        )
        return {
            "run_id": run_id,
            "run_type": "reconcile",
            "status": "accepted",
            "target_date": target_date,
        }

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

    def stop_active_run(self) -> dict[str, object]:
        with self._lock:
            active_run = self.storage.get_active_sync_run()
            if active_run is None or self._cancel_event is None:
                raise AuthorAlphaSyncActiveError("no active author-alpha sync run")
            self._cancel_event.set()
            return {
                "run_id": str(active_run["run_id"]),
                "run_type": str(active_run["run_type"]),
                "status": "stop_requested",
            }

    def _start_thread(
        self,
        *,
        run_id: str,
        run_type: str,
        target: Any,
        kwargs: dict[str, object],
    ) -> None:
        with self._lock:
            active_run = self.storage.get_active_sync_run()
            if active_run is not None or (self._thread is not None and self._thread.is_alive()):
                raise AuthorAlphaSyncActiveError("author-alpha sync run already active")
            self._cancel_event = threading.Event()
            self.sync.cancel_event = self._cancel_event
            self._active_run_id = run_id
            self._active_run_type = run_type
            self._thread = threading.Thread(
                target=self._run_in_background,
                name=f"author-alpha-sync-{run_type}",
                daemon=True,
                kwargs={"target": target, "kwargs": kwargs},
            )
            self._thread.start()

    def _run_in_background(self, *, target: Any, kwargs: dict[str, object]) -> None:
        try:
            target(**kwargs)
        finally:
            with self._lock:
                self._thread = None
                self._active_run_id = None
                self._active_run_type = None
                self._cancel_event = None
                self.sync.cancel_event = None


class AuthorAlphaSync:
    def __init__(
        self,
        *,
        storage: AuthorAlphaStorageProtocol,
        analytics_client: AnalyticsClientProtocol,
        twitter_client: TwitterReadClientProtocol,
        timezone: str = "UTC",
        excluded_authors: list[str] | None = None,
        score_lookback_days: int = 7,
        score_min_daily_replies: int = 400,
        score_prior_weight: float = 7.0,
        score_penalty_constant: float = 200.0,
        post_limit: int = 1000,
        query_page_size: int = 100,
        max_day_attempts: int = 2,
        cancel_event: threading.Event | None = None,
    ) -> None:
        self.storage = storage
        self.analytics_client = analytics_client
        self.twitter_client = twitter_client
        self.timezone = ZoneInfo(timezone)
        self.excluded_authors = {
            str(author).strip()
            for author in (excluded_authors or [])
            if str(author).strip()
        }
        self.score_lookback_days = max(1, int(score_lookback_days))
        self.score_min_daily_replies = max(0, int(score_min_daily_replies))
        self.score_prior_weight = float(score_prior_weight)
        self.score_penalty_constant = float(score_penalty_constant)
        self.post_limit = max(1, int(post_limit))
        self.query_page_size = max(1, int(query_page_size))
        self.max_day_attempts = max(1, int(max_day_attempts))
        self.cancel_event = cancel_event

    def bootstrap(
        self,
        *,
        from_date: str,
        to_date: str,
        resume: bool = False,
        max_days: int | None = None,
        run_id: str | None = None,
    ) -> dict[str, object]:
        start_day = _parse_iso_date(from_date)
        end_day = _parse_iso_date(to_date)
        if end_day < start_day:
            raise ValueError("to_date must be on or after from_date")

        checkpoint = self.storage.read_checkpoint("bootstrap") if resume else None
        if checkpoint and checkpoint.get("next_pending_date"):
            checkpoint_day = _parse_iso_date(str(checkpoint["next_pending_date"]))
            if checkpoint_day > start_day:
                start_day = checkpoint_day

        requested_days = list(_iter_days(start_day, end_day))
        if max_days is not None:
            requested_days = requested_days[: max(0, max_days)]

        run_id = run_id or f"bootstrap-{uuid4().hex}"
        start_marker = requested_days[0].isoformat() if requested_days else None
        self.storage.record_sync_run(
            run_id=run_id,
            run_type="bootstrap",
            status="running",
            from_date=from_date,
            to_date=to_date,
            current_date=start_marker,
            days_completed=0,
            days_total=len(requested_days),
            resume_from_date=start_marker,
        )

        days_completed = 0
        last_completed: date | None = None
        try:
            for day in requested_days:
                self._check_cancelled()
                self._sync_day_with_retries(day)
                days_completed += 1
                last_completed = day
                next_pending = requested_days[days_completed].isoformat() if days_completed < len(requested_days) else None
                self.storage.write_checkpoint(
                    sync_scope="bootstrap",
                    last_completed_date=day.isoformat(),
                    next_pending_date=next_pending,
                    last_run_id=run_id,
                    updated_at=_utcnow_iso(),
                )
                self.storage.update_sync_run(
                    run_id,
                    current_date=day.isoformat(),
                    days_completed=days_completed,
                    resume_from_date=next_pending,
                )
        except Exception as exc:
            failing_day = requested_days[days_completed] if days_completed < len(requested_days) else last_completed
            self.storage.update_sync_run(
                run_id,
                status="failed",
                current_date=failing_day.isoformat() if failing_day else None,
                days_completed=days_completed,
                resume_from_date=failing_day.isoformat() if failing_day else None,
                error=str(exc),
                finished_at=_utcnow_iso(),
            )
            self.storage.write_checkpoint(
                sync_scope="bootstrap",
                last_completed_date=last_completed.isoformat() if last_completed else None,
                next_pending_date=failing_day.isoformat() if failing_day else None,
                last_run_id=run_id,
                updated_at=_utcnow_iso(),
            )
            raise

        score_summary = self._recompute_scores(
            reference_date=last_completed or end_day,
            source="bootstrap",
        )
        self.storage.update_sync_run(
            run_id,
            status="completed",
            current_date=last_completed.isoformat() if last_completed else None,
            days_completed=days_completed,
            resume_from_date=None,
            finished_at=_utcnow_iso(),
        )
        return {
            "run_id": run_id,
            "run_type": "bootstrap",
            "from_date": from_date,
            "to_date": to_date,
            "days_completed": days_completed,
            "score_summary": score_summary,
        }

    def reconcile(self, *, target_date: str | None = None, run_id: str | None = None) -> dict[str, object]:
        metric_day = _parse_iso_date(target_date) if target_date else self._default_reconcile_day()
        run_id = run_id or f"reconcile-{uuid4().hex}"
        metric_date = metric_day.isoformat()
        self.storage.record_sync_run(
            run_id=run_id,
            run_type="reconcile",
            status="running",
            from_date=metric_date,
            to_date=metric_date,
            current_date=metric_date,
            days_completed=0,
            days_total=1,
            resume_from_date=metric_date,
        )
        try:
            self._check_cancelled()
            sync_summary = self._sync_day_with_retries(metric_day)
            self.storage.write_checkpoint(
                sync_scope="reconcile",
                last_completed_date=metric_date,
                next_pending_date=None,
                last_run_id=run_id,
                updated_at=_utcnow_iso(),
            )
            score_summary = self._recompute_scores(reference_date=metric_day, source="reconcile")
        except Exception as exc:
            self.storage.update_sync_run(
                run_id,
                status="failed",
                current_date=metric_date,
                days_completed=0,
                resume_from_date=metric_date,
                error=str(exc),
                finished_at=_utcnow_iso(),
            )
            raise

        self.storage.update_sync_run(
            run_id,
            status="completed",
            current_date=metric_date,
            days_completed=1,
            resume_from_date=None,
            finished_at=_utcnow_iso(),
        )
        return {
            "run_id": run_id,
            "run_type": "reconcile",
            "metric_date": metric_date,
            "sync_summary": sync_summary,
            "score_summary": score_summary,
        }

    def _sync_day_with_retries(self, metric_day: date) -> dict[str, object]:
        last_error: Exception | None = None
        for attempt in range(1, self.max_day_attempts + 1):
            self._check_cancelled()
            try:
                return self._sync_day(metric_day)
            except Exception as exc:
                last_error = exc
                if _is_rate_limited(exc) or attempt >= self.max_day_attempts:
                    raise
        if last_error is not None:
            raise last_error
        raise RuntimeError("unreachable")

    def _sync_day(self, metric_day: date) -> dict[str, object]:
        start_time, end_time = _local_day_bounds(metric_day, self.timezone)
        payload = self.analytics_client.fetch_content_posts(
            start_time=start_time,
            end_time=end_time,
            post_limit=self.post_limit,
            query_page_size=self.query_page_size,
        )
        raw_posts = payload.get("posts") if isinstance(payload, dict) else None
        posts = raw_posts if isinstance(raw_posts, list) else []
        sampled_at = _utcnow_iso()
        rollups: dict[str, _AuthorAccumulator] = defaultdict(_AuthorAccumulator)
        reply_rows: list[dict[str, object]] = []
        metrics_written = 0

        for post in posts:
            self._check_cancelled()
            if not isinstance(post, Mapping):
                continue
            reply_to_id = _normalize_optional_str(post.get("reply_to_id"))
            reply_tweet_id = _normalize_optional_str(post.get("id"))
            if not reply_to_id or not reply_tweet_id:
                continue

            target_tweet_id = reply_to_id
            target_author = _extract_first_reply_handle(post.get("text"))

            if not target_author:
                reply = self.twitter_client.fetch_tweet(reply_tweet_id)
                target_tweet_id = _normalize_optional_str(reply.target_tweet_id) or reply.reply_to_tweet_id or reply_to_id
                target_author = _normalize_optional_str(reply.target_screen_name or reply.reply_to_screen_name)
            if not target_tweet_id or not target_author:
                continue

            impressions = _metric_value(post, "impressions")
            likes = _metric_value(post, "likes")
            replies = _metric_value(post, "replies")
            reposts = _metric_value(post, "reposts")
            reply_rows.append(
                {
                    "reply_tweet_id": reply_tweet_id,
                    "target_tweet_id": target_tweet_id,
                    "target_author": target_author,
                    "impressions": impressions,
                    "likes": likes,
                    "replies": replies,
                    "reposts": reposts,
                    "sampled_at": sampled_at,
                }
            )
            accumulator = rollups[target_author]
            accumulator.reply_count += 1
            accumulator.impressions_total += impressions
            accumulator.likes_total += likes
            accumulator.replies_total += replies
            accumulator.reposts_total += reposts
            accumulator.max_impressions = max(accumulator.max_impressions, impressions)
            metrics_written += 1

        computed_at = _utcnow_iso()
        rollup_rows: list[dict[str, object]] = []
        for target_author, accumulator in rollups.items():
            rollup_rows.append(
                {
                    "target_author": target_author,
                    "reply_count": accumulator.reply_count,
                    "impressions_total": accumulator.impressions_total,
                    "likes_total": accumulator.likes_total,
                    "replies_total": accumulator.replies_total,
                    "reposts_total": accumulator.reposts_total,
                    "avg_impressions": accumulator.avg_impressions,
                    "max_impressions": accumulator.max_impressions,
                    "computed_at": computed_at,
                }
            )

        self.storage.replace_day_sync_snapshot(
            metric_date=metric_day.isoformat(),
            reply_metrics=reply_rows,
            author_rollups=rollup_rows,
        )

        return {
            "metric_date": metric_day.isoformat(),
            "reply_metrics_written": metrics_written,
            "author_rollups_written": len(rollups),
        }

    def _check_cancelled(self) -> None:
        if self.cancel_event is not None and self.cancel_event.is_set():
            raise AuthorAlphaSyncCancellationError("author-alpha sync stopped by operator")

    def _recompute_scores(self, *, reference_date: date, source: str) -> dict[str, object]:
        window_start = reference_date - timedelta(days=self.score_lookback_days - 1)
        window_rows = self.storage.list_reply_daily_metrics(window_start.isoformat(), reference_date.isoformat())
        day_counts: dict[str, int] = defaultdict(int)
        for row in window_rows:
            metric_date = _normalize_optional_str(row.get("metric_date"))
            if metric_date:
                day_counts[metric_date] += 1
        included_days = {
            metric_date
            for metric_date, count in day_counts.items()
            if count >= self.score_min_daily_replies
        }
        reply_rows = [
            row
            for row in window_rows
            if _normalize_optional_str(row.get("metric_date")) in included_days
        ]
        aggregates: dict[str, _AuthorAccumulator] = defaultdict(_AuthorAccumulator)
        author_impressions: dict[str, list[int]] = defaultdict(list)
        all_impressions: list[int] = []

        for row in reply_rows:
            target_author = _normalize_optional_str(row.get("target_author"))
            if not target_author:
                continue
            if target_author in self.excluded_authors:
                continue
            impressions = _int_value(row.get("impressions"))
            likes = _int_value(row.get("likes"))
            replies = _int_value(row.get("replies"))
            reposts = _int_value(row.get("reposts"))
            accumulator = aggregates[target_author]
            accumulator.reply_count += 1
            accumulator.impressions_total += impressions
            accumulator.likes_total += likes
            accumulator.replies_total += replies
            accumulator.reposts_total += reposts
            accumulator.max_impressions = max(accumulator.max_impressions, impressions)
            author_impressions[target_author].append(impressions)
            all_impressions.append(impressions)

        p95 = _percentile_95(all_impressions)
        global_winsorized_mean = _winsorized_mean(all_impressions, cap=p95)

        scored_at = _utcnow_iso()
        scored_authors: set[str] = set()
        for target_author, accumulator in aggregates.items():
            impressions = author_impressions[target_author]
            n = len(impressions)
            mu = _winsorized_mean(impressions, cap=p95)
            posterior_mean = _posterior_mean(
                n=n,
                mu=mu,
                mu0=global_winsorized_mean,
                k=self.score_prior_weight,
            )
            score = posterior_mean - (self.score_penalty_constant / math.sqrt(n + 1))
            scored_authors.add(target_author)
            self.storage.upsert_author(
                screen_name=target_author,
                author_name=None,
                rest_id=None,
                author_score=score,
                reply_count_7d=accumulator.reply_count,
                impressions_total_7d=accumulator.impressions_total,
                avg_impressions_7d=accumulator.avg_impressions,
                max_impressions_7d=accumulator.max_impressions,
                last_replied_at=None,
                last_post_seen_at=None,
                last_scored_at=scored_at,
                source=source,
            )

        zeroed = self.storage.zero_out_stale_authors(scored_authors, scored_at=scored_at)
        return {
            "window_start": window_start.isoformat(),
            "window_end": reference_date.isoformat(),
            "scored_author_count": len(scored_authors),
            "zeroed_author_count": zeroed,
            "excluded_authors": sorted(self.excluded_authors),
            "included_metric_dates": sorted(included_days),
            "score_min_daily_replies": self.score_min_daily_replies,
            "winsorization_cap_p95": p95,
            "global_winsorized_mean": round(global_winsorized_mean, 6),
            "score_prior_weight": self.score_prior_weight,
            "score_penalty_constant": self.score_penalty_constant,
        }

    def _default_reconcile_day(self) -> date:
        return datetime.now(UTC).astimezone(self.timezone).date() - timedelta(days=1)


def _local_day_bounds(metric_day: date, timezone: ZoneInfo) -> tuple[str, str]:
    start_at = datetime.combine(metric_day, time.min, tzinfo=timezone)
    end_at = datetime.combine(metric_day, time(hour=23, minute=59, second=59), tzinfo=timezone)
    return _to_rfc3339(start_at), _to_rfc3339(end_at)


def _to_rfc3339(value: datetime) -> str:
    normalized = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    return normalized.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utcnow_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso_date(value: str) -> date:
    return date.fromisoformat(value)


def _iter_days(start_day: date, end_day: date):
    current = start_day
    while current <= end_day:
        yield current
        current += timedelta(days=1)


def _metric_value(post: Mapping[str, object], key: str) -> int:
    public_metrics = post.get("public_metrics")
    if isinstance(public_metrics, Mapping):
        return _int_value(public_metrics.get(key))
    metrics_total = post.get("metrics_total")
    if isinstance(metrics_total, Mapping):
        return _int_value(metrics_total.get(key))
    return 0


def _int_value(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            try:
                return int(float(stripped))
            except ValueError:
                return 0
    return 0


def _normalize_optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text[1:] if text.startswith("@") else text


_HANDLE_RE = re.compile(r"@([A-Za-z0-9_]{1,15})")


def _extract_first_reply_handle(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    match = _HANDLE_RE.search(value)
    if match is None:
        return None
    handle = match.group(1).strip()
    return handle or None


def _percentile_95(values: list[int]) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    rank = max(1, math.ceil(0.95 * len(ordered)))
    return ordered[rank - 1]


def _winsorized_mean(values: list[int], *, cap: int) -> float:
    if not values:
        return 0.0
    if cap <= 0:
        return 0.0
    return sum(min(value, cap) for value in values) / len(values)


def _posterior_mean(*, n: int, mu: float, mu0: float, k: float) -> float:
    return ((n * mu) + (k * mu0)) / (n + k)


def _is_rate_limited(exc: Exception) -> bool:
    status_code = getattr(exc, "status_code", None)
    if status_code == 429:
        return True
    response = getattr(exc, "response", None)
    if getattr(response, "status_code", None) == 429:
        return True
    return "429" in str(exc)
