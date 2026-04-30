"""Standalone execution lane for author-alpha burst replies."""

from __future__ import annotations

import inspect
import random
import ssl
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from http.client import RemoteDisconnected
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from zoneinfo import ZoneInfo

from x_atuo.automation.config import AutomationConfig
from x_atuo.automation.state import (
    AutomationRequest,
    ExecutionResult,
    FeedCandidate,
    RunStatus,
    WorkflowKind,
    WorkflowStateModel,
    make_initial_state,
)
from x_atuo.core.ai_client import AIDraftResult, AIProviderError
from x_atuo.core.twitter_client import TwitterClientError
from x_atuo.core.twitter_models import TweetRecord, TwitterCommandResult
from x_atuo.core.x_web_notifications import XWebNotificationsError


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


class AuthorAlphaStorageProtocol(Protocol):
    def list_authors_ordered_by_score(self, *, limit: int | None = None) -> list[dict[str, Any]]: ...
    def get_target_last_success_at(self, target_tweet_id: str) -> str | None: ...


class DeviceFollowFeedProtocol(Protocol):
    def fetch_device_follow_feed(self, *, count: int) -> dict[str, object]: ...


class ReplyDraftProtocol(Protocol):
    def draft_reply(self, candidate: FeedCandidate, context: dict[str, Any] | None = None) -> AIDraftResult | str: ...


class ReplyClientProtocol(Protocol):
    def reply(self, tweet_id: str, text: str) -> TwitterCommandResult: ...


class SharedEngagementStorageProtocol(Protocol):
    def has_target_tweet_id(self, target_tweet_id: str, *, exclude_workflows: tuple[str, ...] | None = None) -> bool: ...
    def record_shared_engagement(
        self,
        *,
        workflow: str,
        run_id: str,
        target_tweet_id: str | None,
        target_author: str | None,
        target_tweet_url: str | None,
        reply_tweet_id: str | None,
        reply_url: str | None,
        followed: bool,
        created_at: str | None = None,
    ) -> None: ...


@dataclass(slots=True)
class AuthorAlphaExecutionGraph:
    config: AutomationConfig
    storage: AuthorAlphaStorageProtocol
    candidate_source: DeviceFollowFeedProtocol
    drafter: ReplyDraftProtocol
    reply_client: ReplyClientProtocol
    sleep: Any = None
    now: Any = None
    shared_storage: SharedEngagementStorageProtocol | None = None
    _timezone: ZoneInfo = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.sleep is None:
            async def _noop_sleep(_: int) -> None:
                return None

            self.sleep = _noop_sleep
        if self.now is None:
            self.now = lambda: datetime.now(UTC)
        self._timezone = ZoneInfo(self.config.author_alpha.timezone)

    async def invoke(self, request: AutomationRequest) -> WorkflowStateModel:
        state = make_initial_state(request)
        snapshot = state["snapshot"]
        if request.workflow is not WorkflowKind.AUTHOR_ALPHA_ENGAGE:
            snapshot.mark_failed("author-alpha graph requires author-alpha-engage workflow", node="prepare")
            return snapshot

        authors = self._ordered_authors()
        snapshot.log_event(
            "load_authors",
            "loaded ordered authors",
            author_count=len(authors),
            ordered_screen_names=[str(author.get("screen_name") or "") for author in authors],
        )

        post_queue = await self._build_post_queue(authors)
        max_targets = max(1, int(self.config.author_alpha.max_targets_per_run))
        post_queue = post_queue[:max_targets]
        snapshot.log_event(
            "build_queue",
            "built author-alpha post queue",
            queue_size=len(post_queue),
            tweet_ids=[candidate.tweet_id for candidate in post_queue],
        )

        consumed_target_tweet_ids: set[str] = set()
        sent_replies: list[dict[str, Any]] = []
        skipped_targets: list[dict[str, Any]] = []

        for index, candidate in enumerate(post_queue):
            snapshot.selected_candidate = candidate
            if candidate.tweet_id in consumed_target_tweet_ids:
                skipped_targets.append(
                    {
                        "tweet_id": candidate.tweet_id,
                        "screen_name": candidate.screen_name,
                        "reason": "target already consumed in this run",
                    }
                )
                snapshot.log_event(
                    "execute_burst",
                    "target skipped after prior burst",
                    tweet_id=candidate.tweet_id,
                    screen_name=candidate.screen_name,
                )
                continue

            try:
                burst_replies, burst_skipped = await self._execute_candidate_burst(snapshot, candidate)
            except Exception as exc:
                skipped_targets.append(
                    {
                        "tweet_id": candidate.tweet_id,
                        "screen_name": candidate.screen_name,
                        "reason": str(exc) or exc.__class__.__name__,
                    }
                )
                snapshot.log_event(
                    "execute_burst",
                    "candidate failed after retries",
                    tweet_id=candidate.tweet_id,
                    screen_name=candidate.screen_name,
                    error=str(exc) or exc.__class__.__name__,
                )
                continue
            sent_replies.extend(burst_replies)
            skipped_targets.extend(burst_skipped)
            consumed_target_tweet_ids.add(candidate.tweet_id)

            if self._daily_limit_reached():
                snapshot.log_event(
                    "execute_burst",
                    "stopping run after daily execution limit",
                    sent_reply_count=len(sent_replies),
                )
                break

            if index < len(post_queue) - 1:
                await _maybe_await(self.sleep(int(self.config.author_alpha.target_switch_delay_seconds)))

        detail = {
            "ordered_screen_names": [str(author.get("screen_name") or "") for author in authors],
            "consumed_target_tweet_ids": sorted(consumed_target_tweet_ids),
            "sent_reply_count": len(sent_replies),
            "sent_replies": sent_replies,
            "skipped_targets": skipped_targets,
        }
        result = ExecutionResult(
            action="author_alpha_engage",
            ok=bool(sent_replies),
            dry_run=request.dry_run,
            target_tweet_id=sent_replies[-1]["target_tweet_id"] if sent_replies else None,
            target_tweet_url=sent_replies[-1]["target_tweet_url"] if sent_replies else None,
            created_tweet_id=sent_replies[-1]["reply_tweet_id"] if sent_replies else None,
            reply_url=sent_replies[-1]["reply_url"] if sent_replies else None,
            detail=detail,
            error=None if sent_replies else "no author-alpha replies sent",
        )
        snapshot.result = result
        if sent_replies:
            snapshot.status = RunStatus.COMPLETED
        else:
            snapshot.status = RunStatus.SKIPPED
        snapshot.touch()
        snapshot.log_event("finalize", "author-alpha execution finished", status=snapshot.status.value, **detail)
        return snapshot

    def _ordered_authors(self) -> list[dict[str, Any]]:
        excluded = {
            str(author).strip()
            for author in getattr(self.config.author_alpha, "excluded_authors", [])
            if str(author).strip()
        }
        authors = [
            author
            for author in self.storage.list_authors_ordered_by_score()
            if str(author.get("screen_name") or "").strip() not in excluded
        ]
        authors.sort(
            key=lambda author: (
                -float(author.get("author_score") or 0),
                -int(author.get("reply_count_7d") or 0),
                -float(author.get("avg_impressions_7d") or 0),
                str(author.get("screen_name") or ""),
            )
        )
        return authors

    async def _build_post_queue(self, authors: list[dict[str, Any]]) -> list[FeedCandidate]:
        author_map = {
            str(author.get("screen_name") or "").strip(): author
            for author in authors
            if str(author.get("screen_name") or "").strip()
        }
        payload = await self._call_with_retries(
            lambda: self.candidate_source.fetch_device_follow_feed(
                count=max(1, int(self.config.author_alpha.device_follow_feed_count))
            ),
            operation="device-follow fetch",
        )
        raw_posts = payload.get("posts") if isinstance(payload, dict) else None
        excluded = {
            str(author).strip()
            for author in getattr(self.config.author_alpha, "excluded_authors", [])
            if str(author).strip()
        }
        queue: list[FeedCandidate] = []
        seen_tweet_ids: set[str] = set()
        for post in raw_posts if isinstance(raw_posts, list) else []:
            if not isinstance(post, dict):
                continue
            tweet_id = str(post.get("id") or "").strip()
            if not tweet_id or tweet_id in seen_tweet_ids:
                continue
            author = post.get("author") if isinstance(post.get("author"), dict) else {}
            screen_name = str(author.get("screen_name") or "").strip()
            if not screen_name or screen_name in excluded:
                continue
            candidate = FeedCandidate(
                tweet_id=tweet_id,
                screen_name=screen_name,
                text=str(post.get("text") or "") or None,
                created_at=_coerce_created_at(post.get("created_at")),
                author_verified=bool(author.get("verified")) if isinstance(author, dict) else None,
                can_reply=True,
                metadata=dict(post),
            )
            queue.append(candidate)
            seen_tweet_ids.add(tweet_id)
        queue.sort(
            key=lambda candidate: (
                -float(author_map.get(candidate.screen_name or "", {}).get("author_score") or 0),
                -int(author_map.get(candidate.screen_name or "", {}).get("reply_count_7d") or 0),
                -float(author_map.get(candidate.screen_name or "", {}).get("avg_impressions_7d") or 0),
                candidate.screen_name or "",
                candidate.tweet_id,
            )
        )
        return [candidate for candidate in queue if self._queue_reason(candidate) is None]

    async def _execute_candidate_burst(
        self,
        snapshot: WorkflowStateModel,
        candidate: FeedCandidate,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if candidate.can_reply is False:
            reason = candidate.reply_limit_reason or candidate.reply_limit_headline or "target cannot be replied to"
            snapshot.log_event(
                "execute_burst",
                "candidate skipped before burst",
                tweet_id=candidate.tweet_id,
                screen_name=candidate.screen_name,
                reason=reason,
            )
            return [], [{"tweet_id": candidate.tweet_id, "screen_name": candidate.screen_name, "reason": reason}]

        sent_replies: list[dict[str, Any]] = []
        skipped_targets: list[dict[str, Any]] = []
        max_burst = max(1, int(self.config.author_alpha.per_run_same_target_burst_limit))
        burst_id = f"{snapshot.run_id}:{candidate.tweet_id}"
        for burst_index in range(1, max_burst + 1):
            capacity_reason = self._capacity_reason(candidate)
            if capacity_reason is not None:
                skipped_targets.append(
                    {
                        "tweet_id": candidate.tweet_id,
                        "screen_name": candidate.screen_name,
                        "reason": capacity_reason,
                    }
                )
                snapshot.log_event(
                    "execute_burst",
                    "burst stopped by policy cap",
                    tweet_id=candidate.tweet_id,
                    screen_name=candidate.screen_name,
                    reason=capacity_reason,
                    burst_index=burst_index,
                )
                break

            text = await self._draft_text(
                candidate,
                snapshot=snapshot,
                burst_index=burst_index,
                previous_replies=sent_replies,
            )
            command_result = self._reply(
                candidate.tweet_id,
                text,
                snapshot=snapshot,
                dry_run=snapshot.request.dry_run,
            )
            if not command_result.ok:
                skipped_targets.append(
                    {
                        "tweet_id": candidate.tweet_id,
                        "screen_name": candidate.screen_name,
                        "reason": command_result.error_message or "reply failed",
                    }
                )
                snapshot.log_event(
                    "execute_burst",
                    "burst stopped on reply failure",
                    tweet_id=candidate.tweet_id,
                    screen_name=candidate.screen_name,
                    burst_index=burst_index,
                    error=command_result.error_message,
                )
                break

            sent_at = self._now().isoformat()
            reply_url = (
                f"https://x.com/i/status/{command_result.tweet_id}"
                if command_result.tweet_id
                else None
            )
            reply_record = {
                "target_tweet_id": candidate.tweet_id,
                "target_tweet_url": self._target_url(candidate),
                "reply_tweet_id": command_result.tweet_id,
                "reply_url": reply_url,
                "screen_name": candidate.screen_name,
                "text": text,
                "burst_index": burst_index,
                "created_at": sent_at,
            }
            sent_replies.append(reply_record)
            snapshot.log_event(
                "execute_burst",
                "reply sent",
                tweet_id=candidate.tweet_id,
                screen_name=candidate.screen_name,
                burst_index=burst_index,
                reply_tweet_id=command_result.tweet_id,
                dry_run=snapshot.request.dry_run,
            )
            if not snapshot.request.dry_run and candidate.screen_name and command_result.tweet_id:
                self._record_engagement(
                    run_id=snapshot.run_id,
                    target_author=candidate.screen_name,
                    target_tweet_id=candidate.tweet_id,
                    target_tweet_url=self._target_url(candidate),
                    reply_tweet_id=command_result.tweet_id,
                    reply_url=reply_url,
                    burst_id=burst_id,
                    burst_index=burst_index,
                    burst_size=max_burst,
                    created_at=sent_at,
                )

            if self._should_pause_before_next_reply(candidate, burst_index=burst_index):
                await _maybe_await(self.sleep(int(self.config.author_alpha.inter_reply_delay_seconds)))

        if sent_replies:
            self._update_burst_size(burst_id=burst_id, burst_size=len(sent_replies))

        return sent_replies, skipped_targets

    async def _draft_text(
        self,
        candidate: FeedCandidate,
        *,
        snapshot: WorkflowStateModel,
        burst_index: int,
        previous_replies: list[dict[str, Any]],
    ) -> str:
        context = {
            "workflow": "author_alpha_engage",
            "burst_index": burst_index,
            "previous_replies": [reply["text"] for reply in previous_replies],
        }
        drafted = await self._call_with_retries(
            lambda: self.drafter.draft_reply(candidate, context),
            operation="draft reply",
            snapshot=snapshot,
            candidate=candidate,
            burst_index=burst_index,
        )
        if isinstance(drafted, AIDraftResult):
            return drafted.text
        return str(drafted)

    def _reply(
        self,
        tweet_id: str,
        text: str,
        *,
        snapshot: WorkflowStateModel,
        dry_run: bool,
    ) -> TwitterCommandResult:
        if dry_run:
            return TwitterCommandResult(
                action="reply",
                ok=True,
                dry_run=True,
                target_tweet_id=tweet_id,
                tweet_id=f"dry-run-{tweet_id}",
                text=text,
            )
        return self.reply_client.reply(tweet_id, text)

    def _should_pause_before_next_reply(self, candidate: FeedCandidate, *, burst_index: int) -> bool:
        max_burst = max(1, int(self.config.author_alpha.per_run_same_target_burst_limit))
        if burst_index >= max_burst:
            return False
        if self._target_success_count(candidate.tweet_id) >= int(
            self.config.author_alpha.per_target_tweet_success_limit
        ):
            return False
        return self._capacity_reason(candidate) is None

    def _capacity_reason(self, candidate: FeedCandidate) -> str | None:
        if self._daily_limit_reached():
            return "daily execution limit reached"
        now = self._now()
        if self._recent_success_count_15m(now.isoformat()) >= int(
            self.config.author_alpha.global_send_limit_15m
        ):
            return "global 15m send limit reached"
        metric_date = self._metric_date(now)
        if candidate.screen_name and self._author_daily_success_count(
            candidate.screen_name,
            metric_date=metric_date,
        ) >= int(self.config.author_alpha.per_author_daily_success_limit):
            return "per-author daily success limit reached"
        if self._target_success_count(candidate.tweet_id) >= int(
            self.config.author_alpha.per_target_tweet_success_limit
        ):
            return "target lifetime success limit reached"
        return None

    def _queue_reason(self, candidate: FeedCandidate) -> str | None:
        if self._shared_target_seen_by_other_workflow(candidate.tweet_id):
            return "target already engaged by another workflow"
        if self._target_success_count(candidate.tweet_id) >= int(
            self.config.author_alpha.per_target_tweet_success_limit
        ):
            return "target lifetime success limit reached"
        last_success_at = self._target_last_success_at(candidate.tweet_id)
        if last_success_at is None:
            return None
        last_seen = _parse_timestamp(last_success_at)
        cooldown = max(0, int(self.config.author_alpha.target_revisit_cooldown_seconds))
        if self._now() < last_seen + timedelta(seconds=cooldown):
            return "target revisit cooldown active"
        return None

    def _daily_limit_reached(self) -> bool:
        limit = int(self.config.author_alpha.daily_execution_limit)
        return self._global_daily_success_count(self._metric_date(self._now())) >= limit

    def _global_daily_success_count(self, metric_date: str) -> int:
        reader = getattr(self.storage, "get_daily_success_count", None)
        if callable(reader):
            return int(reader(metric_date=metric_date))

        row = self._fetch_engagement_count(
            """
            SELECT COUNT(*) AS count
            FROM alpha_engagements
            WHERE metric_date = ?
            """,
            (metric_date,),
        )
        return int(row["count"]) if row is not None else 0

    def _target_success_count(self, target_tweet_id: str) -> int:
        reader = getattr(self.storage, "get_target_success_count", None)
        if callable(reader):
            return int(reader(target_tweet_id))
        row = self._fetch_engagement_count(
            """
            SELECT COUNT(*) AS count
            FROM alpha_engagements
            WHERE target_tweet_id = ?
            """,
            (target_tweet_id,),
        )
        return int(row["count"]) if row is not None else 0

    def _author_daily_success_count(self, target_author: str, *, metric_date: str) -> int:
        reader = getattr(self.storage, "get_author_daily_success_count", None)
        if callable(reader):
            return int(reader(target_author, metric_date=metric_date))
        row = self._fetch_engagement_count(
            """
            SELECT COUNT(*) AS count
            FROM alpha_engagements
            WHERE target_author = ? AND metric_date = ?
            """,
            (target_author, metric_date),
        )
        return int(row["count"]) if row is not None else 0

    def _recent_success_count_15m(self, as_of: str) -> int:
        reader = getattr(self.storage, "get_recent_success_count_15m", None)
        if callable(reader):
            return int(reader(as_of))
        anchor = _parse_timestamp(as_of)
        cutoff = anchor - timedelta(minutes=15)
        row = self._fetch_engagement_count(
            """
            SELECT COUNT(*) AS count
            FROM alpha_engagements
            WHERE unixepoch(created_at) >= unixepoch(?) AND unixepoch(created_at) <= unixepoch(?)
            """,
            (_normalize_timestamp(cutoff.isoformat()), _normalize_timestamp(anchor.isoformat())),
        )
        return int(row["count"]) if row is not None else 0

    def _target_last_success_at(self, target_tweet_id: str) -> str | None:
        reader = getattr(self.storage, "get_target_last_success_at", None)
        if callable(reader):
            return reader(target_tweet_id)
        row = self._fetch_engagement_count(
            """
            SELECT created_at
            FROM alpha_engagements
            WHERE target_tweet_id = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (target_tweet_id,),
        )
        if row is None:
            return None
        value = row["created_at"]
        return str(value) if value is not None else None

    async def _call_with_retries(
        self,
        fn: Any,
        *,
        operation: str,
        snapshot: WorkflowStateModel | None = None,
        candidate: FeedCandidate | None = None,
        burst_index: int | None = None,
    ) -> Any:
        max_attempts = 3
        attempt = 1
        while True:
            try:
                return fn()
            except Exception as exc:
                if attempt >= max_attempts or not _is_retryable_network_error(exc):
                    raise
                if snapshot is not None:
                    snapshot.log_event(
                        "retry",
                        "retrying transient network failure",
                        operation=operation,
                        attempt=attempt,
                        max_attempts=max_attempts,
                        tweet_id=candidate.tweet_id if candidate is not None else None,
                        screen_name=candidate.screen_name if candidate is not None else None,
                        burst_index=burst_index,
                        error=str(exc) or exc.__class__.__name__,
                    )
                delay_seconds = min(5.0, float(2 ** (attempt - 1)) + random.uniform(0.0, 0.25))
                if snapshot is not None:
                    snapshot.log_event(
                        "retry",
                        "waiting before retry",
                        operation=operation,
                        attempt=attempt,
                        delay_seconds=delay_seconds,
                        tweet_id=candidate.tweet_id if candidate is not None else None,
                        screen_name=candidate.screen_name if candidate is not None else None,
                        burst_index=burst_index,
                    )
                attempt += 1
                await _maybe_await(self.sleep(delay_seconds))

    def _record_engagement(
        self,
        *,
        run_id: str,
        target_author: str,
        target_tweet_id: str,
        target_tweet_url: str | None,
        reply_tweet_id: str,
        reply_url: str | None,
        burst_id: str,
        burst_index: int,
        burst_size: int,
        created_at: str,
    ) -> None:
        metric_date = self._metric_date(_parse_timestamp(created_at))
        writer = getattr(self.storage, "record_engagement", None)
        if callable(writer):
            writer(
                run_id=run_id,
                target_author=target_author,
                target_tweet_id=target_tweet_id,
                target_tweet_url=target_tweet_url,
                reply_tweet_id=reply_tweet_id,
                reply_url=reply_url,
                burst_id=burst_id,
                burst_index=burst_index,
                burst_size=burst_size,
                metric_date=metric_date,
                created_at=created_at,
            )
        else:
            connect = getattr(self.storage, "connect", None)
            if not callable(connect):
                raise AttributeError("storage must provide record_engagement() or connect()")
            with connect() as connection:
                connection.execute(
                    """
                    INSERT INTO alpha_engagements (
                        run_id,
                        target_author,
                        target_tweet_id,
                        target_tweet_url,
                        reply_tweet_id,
                        reply_url,
                        burst_id,
                        burst_index,
                        burst_size,
                        metric_date,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        target_author.strip(),
                        target_tweet_id.strip(),
                        target_tweet_url,
                        reply_tweet_id.strip(),
                        reply_url,
                        burst_id,
                        burst_index,
                        burst_size,
                        metric_date,
                        _normalize_timestamp(created_at),
                    ),
                )
        if self.shared_storage is not None:
            self.shared_storage.record_shared_engagement(
                workflow=WorkflowKind.AUTHOR_ALPHA_ENGAGE.value,
                run_id=run_id,
                target_tweet_id=target_tweet_id,
                target_author=target_author,
                target_tweet_url=target_tweet_url,
                reply_tweet_id=reply_tweet_id,
                reply_url=reply_url,
                followed=False,
                created_at=created_at,
            )

    def _update_burst_size(self, *, burst_id: str, burst_size: int) -> None:
        updater = getattr(self.storage, "update_burst_size", None)
        if callable(updater):
            updater(burst_id=burst_id, burst_size=burst_size)
            return
        connect = getattr(self.storage, "connect", None)
        if not callable(connect):
            return
        with connect() as connection:
            connection.execute(
                """
                UPDATE alpha_engagements
                SET burst_size = ?
                WHERE burst_id = ?
                """,
                (burst_size, burst_id),
            )

    def _metric_date(self, value: datetime) -> str:
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(self._timezone).date().isoformat()

    def _fetch_engagement_count(self, query: str, parameters: tuple[Any, ...]) -> Any:
        connect = getattr(self.storage, "connect", None)
        if not callable(connect):
            return None
        with connect() as connection:
            return connection.execute(query, parameters).fetchone()

    def _now(self) -> datetime:
        value = self.now()
        if not isinstance(value, datetime):
            raise TypeError("now() must return datetime")
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)

    @staticmethod
    def _target_url(candidate: FeedCandidate) -> str | None:
        if not candidate.screen_name or not candidate.tweet_id:
            return None
        return f"https://x.com/{candidate.screen_name}/status/{candidate.tweet_id}"

    def _shared_target_seen_by_other_workflow(self, target_tweet_id: str) -> bool:
        if self.shared_storage is None:
            return False
        return bool(
            self.shared_storage.has_target_tweet_id(
                target_tweet_id,
                exclude_workflows=(WorkflowKind.AUTHOR_ALPHA_ENGAGE.value,),
            )
        )


async def run_author_alpha_engage(
    request: AutomationRequest,
    *,
    config: AutomationConfig,
    storage: AuthorAlphaStorageProtocol,
    shared_storage: SharedEngagementStorageProtocol | None,
    candidate_source: DeviceFollowFeedProtocol,
    drafter: ReplyDraftProtocol,
    reply_client: ReplyClientProtocol,
    sleep: Any = None,
    now: Any = None,
) -> WorkflowStateModel:
    graph = AuthorAlphaExecutionGraph(
        config=config,
        storage=storage,
        shared_storage=shared_storage,
        candidate_source=candidate_source,
        drafter=drafter,
        reply_client=reply_client,
        sleep=sleep,
        now=now,
    )
    return await graph.invoke(request)


def _parse_timestamp(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _normalize_timestamp(value: str) -> str:
    return _parse_timestamp(value).isoformat()


def _coerce_created_at(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    except ValueError:
        return None


def _is_retryable_network_error(exc: Exception) -> bool:
    if isinstance(exc, HTTPError):
        return exc.code in {408, 409, 425, 429} or exc.code >= 500
    if isinstance(exc, (URLError, TimeoutError, ConnectionError, ssl.SSLError, RemoteDisconnected)):
        return True
    if isinstance(exc, AIProviderError):
        cause = exc.__cause__
        if isinstance(cause, Exception) and _is_retryable_network_error(cause):
            return True
    if isinstance(exc, (XWebNotificationsError, TwitterClientError)):
        message = str(exc).lower()
        retryable_markers = (
            "timed out",
            "timeout",
            "remote end closed connection",
            "unexpected eof",
            "eof occurred",
            "connection reset",
            "connection aborted",
            "temporarily unavailable",
            "ssl",
        )
        return any(marker in message for marker in retryable_markers)
    return False
