from __future__ import annotations

import asyncio
import ssl
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.error import URLError

from x_atuo.automation.author_alpha_graph import AuthorAlphaExecutionGraph
from x_atuo.automation.author_alpha_storage import AuthorAlphaStorage
from x_atuo.automation.config import AutomationConfig, AuthorAlphaSettings
from x_atuo.automation.state import AutomationRequest
from x_atuo.automation.storage import AutomationStorage
from x_atuo.core.ai_client import AIDraftResult
from x_atuo.core.twitter_models import TweetAuthor, TweetRecord, TwitterCommandResult


def test_author_alpha_orders_authors_by_author_score(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()
    _upsert_author(storage, "high_score", author_score=999, impressions_total_7d=10)
    _upsert_author(storage, "mid_score", author_score=100, impressions_total_7d=500)
    _upsert_author(storage, "low_score", author_score=1, impressions_total_7d=1000)

    feed = _FakeDeviceFollowFeedClient(
        [
            _feed_post("tweet-low", "low_score"),
            _feed_post("tweet-mid", "mid_score"),
            _feed_post("tweet-high", "high_score"),
        ]
    )
    drafter = _FakeDrafter()
    replier = _FakeReplyClient()

    graph = AuthorAlphaExecutionGraph(
        config=_config(per_run_same_target_burst_limit=1),
        storage=storage,
        candidate_source=feed,
        drafter=drafter,
        reply_client=replier,
        sleep=_SleepRecorder(),
    )

    snapshot = asyncio.run(graph.invoke(AutomationRequest.for_author_alpha_engage(job_name="job", dry_run=False)))

    assert snapshot.result is not None
    assert feed.calls == [50]


def test_author_alpha_excludes_configured_authors_from_queue(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()
    _upsert_author(storage, "blocked_author", author_score=1000, impressions_total_7d=1000)
    _upsert_author(storage, "open_author", author_score=100, impressions_total_7d=100)

    feed = _FakeDeviceFollowFeedClient(
        [
            _feed_post("tweet-blocked", "blocked_author"),
            _feed_post("tweet-open", "open_author"),
        ]
    )
    replier = _FakeReplyClient()
    graph = AuthorAlphaExecutionGraph(
        config=_config(per_run_same_target_burst_limit=1, excluded_authors=["blocked_author"]),
        storage=storage,
        candidate_source=feed,
        drafter=_FakeDrafter(),
        reply_client=replier,
        sleep=_SleepRecorder(),
    )

    snapshot = asyncio.run(graph.invoke(AutomationRequest.for_author_alpha_engage(job_name="job", dry_run=False)))

    assert snapshot.result is not None
    assert feed.calls == [50]
    assert [call["tweet_id"] for call in replier.calls] == ["tweet-open"]


def test_author_alpha_same_target_burst_replies_three_times_then_consumes_target(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()
    _upsert_author(storage, "alpha_one", impressions_total_7d=100)
    _upsert_author(storage, "alpha_two", impressions_total_7d=90)

    feed = _FakeDeviceFollowFeedClient(
        [
            _feed_post("tweet-1", "shared_target"),
            _feed_post("tweet-1", "shared_target"),
        ]
    )
    sleep = _SleepRecorder()
    replier = _FakeReplyClient()

    graph = AuthorAlphaExecutionGraph(
        config=_config(per_run_same_target_burst_limit=3),
        storage=storage,
        candidate_source=feed,
        drafter=_FakeDrafter(),
        reply_client=replier,
        sleep=sleep,
    )

    snapshot = asyncio.run(graph.invoke(AutomationRequest.for_author_alpha_engage(job_name="job", dry_run=False)))

    assert snapshot.result is not None
    assert len(replier.calls) == 3
    assert [call["tweet_id"] for call in replier.calls] == ["tweet-1", "tweet-1", "tweet-1"]
    assert _target_success_count(storage, "tweet-1") == 3
    assert sleep.calls == [5, 5]
    with storage.connect() as connection:
        rows = connection.execute(
            """
            SELECT burst_id, burst_index, burst_size
            FROM alpha_engagements
            WHERE target_tweet_id = 'tweet-1'
            ORDER BY burst_index ASC
            """
        ).fetchall()

    assert len(rows) == 3
    assert {row[0] for row in rows} == {rows[0][0]}
    assert [row[1] for row in rows] == [1, 2, 3]
    assert [row[2] for row in rows] == [3, 3, 3]


def test_author_alpha_limits_targets_per_run(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()
    for i in range(1, 8):
        _upsert_author(storage, f"author_{i}", author_score=100 - i, impressions_total_7d=100 - i)

    posts = [_feed_post(f"tweet-{i}", f"author_{i}") for i in range(1, 8)]
    feed = _FakeDeviceFollowFeedClient(posts)
    replier = _FakeReplyClient()
    graph = AuthorAlphaExecutionGraph(
        config=_config(max_targets_per_run=5, per_run_same_target_burst_limit=1),
        storage=storage,
        candidate_source=feed,
        drafter=_FakeDrafter(),
        reply_client=replier,
        sleep=_SleepRecorder(),
    )

    snapshot = asyncio.run(graph.invoke(AutomationRequest.for_author_alpha_engage(job_name="job", dry_run=False)))

    assert snapshot.result is not None
    assert len(replier.calls) == 5
    assert [call["tweet_id"] for call in replier.calls] == [f"tweet-{i}" for i in range(1, 6)]


def test_author_alpha_skips_recently_processed_target_until_revisit_cooldown_expires(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()
    _upsert_author(storage, "high_author", author_score=1000, impressions_total_7d=1000)
    _upsert_author(storage, "mid_author", author_score=500, impressions_total_7d=500)
    _record_engagement(
        storage,
        run_id="historic-1",
        target_author="high_author",
        target_tweet_id="tweet-hot",
        target_tweet_url="https://x.com/high_author/status/tweet-hot",
        reply_tweet_id="reply-old-1",
        reply_url="https://x.com/i/status/reply-old-1",
        created_at="2026-04-27T00:30:00+00:00",
    )

    feed = _FakeDeviceFollowFeedClient(
        [
            _feed_post("tweet-hot", "high_author"),
            _feed_post("tweet-mid", "mid_author"),
        ]
    )
    replier = _FakeReplyClient()
    graph = AuthorAlphaExecutionGraph(
        config=_config(
            max_targets_per_run=5,
            per_run_same_target_burst_limit=1,
            target_revisit_cooldown_seconds=3600,
        ),
        storage=storage,
        candidate_source=feed,
        drafter=_FakeDrafter(),
        reply_client=replier,
        sleep=_SleepRecorder(),
        now=lambda: datetime(2026, 4, 27, 1, 0, tzinfo=UTC),
    )

    snapshot = asyncio.run(graph.invoke(AutomationRequest.for_author_alpha_engage(job_name="job", dry_run=False)))

    assert snapshot.result is not None
    assert [call["tweet_id"] for call in replier.calls] == ["tweet-mid"]


def test_author_alpha_allows_target_again_after_revisit_cooldown(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()
    _upsert_author(storage, "high_author", author_score=1000, impressions_total_7d=1000)
    _upsert_author(storage, "mid_author", author_score=500, impressions_total_7d=500)
    _record_engagement(
        storage,
        run_id="historic-1",
        target_author="high_author",
        target_tweet_id="tweet-hot",
        target_tweet_url="https://x.com/high_author/status/tweet-hot",
        reply_tweet_id="reply-old-1",
        reply_url="https://x.com/i/status/reply-old-1",
        created_at="2026-04-27T00:30:00+00:00",
    )

    feed = _FakeDeviceFollowFeedClient(
        [
            _feed_post("tweet-hot", "high_author"),
            _feed_post("tweet-mid", "mid_author"),
        ]
    )
    replier = _FakeReplyClient()
    graph = AuthorAlphaExecutionGraph(
        config=_config(
            max_targets_per_run=5,
            per_run_same_target_burst_limit=1,
            target_revisit_cooldown_seconds=3600,
        ),
        storage=storage,
        candidate_source=feed,
        drafter=_FakeDrafter(),
        reply_client=replier,
        sleep=_SleepRecorder(),
        now=lambda: datetime(2026, 4, 27, 1, 31, tzinfo=UTC),
    )

    snapshot = asyncio.run(graph.invoke(AutomationRequest.for_author_alpha_engage(job_name="job", dry_run=False)))

    assert snapshot.result is not None
    assert [call["tweet_id"] for call in replier.calls] == ["tweet-hot", "tweet-mid"]


def test_author_alpha_waits_between_targets(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()
    for i in range(1, 4):
        _upsert_author(storage, f"author_{i}", author_score=100 - i, impressions_total_7d=100 - i)

    feed = _FakeDeviceFollowFeedClient([_feed_post(f"tweet-{i}", f"author_{i}") for i in range(1, 4)])
    sleep = _SleepRecorder()
    graph = AuthorAlphaExecutionGraph(
        config=_config(max_targets_per_run=3, per_run_same_target_burst_limit=1, target_switch_delay_seconds=10),
        storage=storage,
        candidate_source=feed,
        drafter=_FakeDrafter(),
        reply_client=_FakeReplyClient(),
        sleep=sleep,
    )

    snapshot = asyncio.run(graph.invoke(AutomationRequest.for_author_alpha_engage(job_name="job", dry_run=False)))

    assert snapshot.result is not None
    assert sleep.calls == [10, 10]


def test_author_alpha_retries_transient_draft_failure(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()
    _upsert_author(storage, "author_1", author_score=100, impressions_total_7d=100)

    class FlakyDrafter:
        def __init__(self) -> None:
            self.calls = 0

        def draft_reply(self, candidate, context=None):
            self.calls += 1
            if self.calls == 1:
                raise URLError("temporary network glitch")
            return AIDraftResult(text=f"reply-{candidate.tweet_id}", rationale="retry success")

    drafter = FlakyDrafter()
    sleep = _SleepRecorder()
    graph = AuthorAlphaExecutionGraph(
        config=_config(max_targets_per_run=1, per_run_same_target_burst_limit=1),
        storage=storage,
        candidate_source=_FakeDeviceFollowFeedClient([_feed_post("tweet-1", "author_1")]),
        drafter=drafter,
        reply_client=_FakeReplyClient(),
        sleep=sleep,
    )

    snapshot = asyncio.run(graph.invoke(AutomationRequest.for_author_alpha_engage(job_name="job", dry_run=False)))

    assert snapshot.result is not None
    assert snapshot.status.value == "completed"
    assert drafter.calls == 2
    assert any(event.node == "retry" and event.payload["operation"] == "draft reply" for event in snapshot.events)
    assert len(sleep.calls) == 1
    assert 1.0 <= sleep.calls[0] <= 1.25


def test_author_alpha_continues_after_retryable_candidate_failure(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()
    _upsert_author(storage, "author_1", author_score=100, impressions_total_7d=100)
    _upsert_author(storage, "author_2", author_score=90, impressions_total_7d=90)

    class DraftFailsForFirstCandidate:
        def draft_reply(self, candidate, context=None):
            if candidate.tweet_id == "tweet-1":
                raise ssl.SSLError("UNEXPECTED_EOF_WHILE_READING")
            return AIDraftResult(text=f"reply-{candidate.tweet_id}", rationale="second candidate ok")

    replier = _FakeReplyClient()
    graph = AuthorAlphaExecutionGraph(
        config=_config(max_targets_per_run=2, per_run_same_target_burst_limit=1),
        storage=storage,
        candidate_source=_FakeDeviceFollowFeedClient(
            [
                _feed_post("tweet-1", "author_1"),
                _feed_post("tweet-2", "author_2"),
            ]
        ),
        drafter=DraftFailsForFirstCandidate(),
        reply_client=replier,
        sleep=_SleepRecorder(),
    )

    snapshot = asyncio.run(graph.invoke(AutomationRequest.for_author_alpha_engage(job_name="job", dry_run=False)))

    assert snapshot.result is not None
    assert snapshot.status.value == "completed"
    assert [call["tweet_id"] for call in replier.calls] == ["tweet-2"]
    assert any(
        event.message == "candidate failed after retries" and event.payload["tweet_id"] == "tweet-1"
        for event in snapshot.events
    )


def test_author_alpha_does_not_retry_reply_writes(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()
    _upsert_author(storage, "author_1", author_score=100, impressions_total_7d=100)
    _upsert_author(storage, "author_2", author_score=90, impressions_total_7d=90)

    class FailingThenSuccessReplyClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, str]] = []

        def reply(self, tweet_id: str, text: str) -> TwitterCommandResult:
            self.calls.append({"tweet_id": tweet_id, "text": text})
            if tweet_id == "tweet-1":
                raise URLError("temporary write failure")
            return TwitterCommandResult(
                action="reply",
                ok=True,
                target_tweet_id=tweet_id,
                tweet_id=f"reply-{len(self.calls)}",
                text=text,
            )

    replier = FailingThenSuccessReplyClient()
    graph = AuthorAlphaExecutionGraph(
        config=_config(max_targets_per_run=2, per_run_same_target_burst_limit=1),
        storage=storage,
        candidate_source=_FakeDeviceFollowFeedClient(
            [
                _feed_post("tweet-1", "author_1"),
                _feed_post("tweet-2", "author_2"),
            ]
        ),
        drafter=_FakeDrafter(),
        reply_client=replier,
        sleep=_SleepRecorder(),
    )

    snapshot = asyncio.run(graph.invoke(AutomationRequest.for_author_alpha_engage(job_name="job", dry_run=False)))

    assert snapshot.result is not None
    assert snapshot.status.value == "completed"
    assert [call["tweet_id"] for call in replier.calls] == ["tweet-1", "tweet-2"]
    assert not any(event.node == "retry" and event.payload["operation"] == "send reply" for event in snapshot.events)


def test_author_alpha_allows_already_replied_target_until_lifetime_cap_of_four(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()
    _upsert_author(storage, "alpha_one", impressions_total_7d=100)
    for index in range(3):
        _record_engagement(
            storage,
            run_id=f"historic-{index}",
            target_author="alpha_one",
            target_tweet_id="tweet-1",
            target_tweet_url="https://x.com/alpha_one/status/tweet-1",
            reply_tweet_id=f"historic-reply-{index}",
            reply_url=f"https://x.com/i/status/historic-reply-{index}",
            created_at=f"2026-04-27T00:0{index}:00+00:00",
        )

    replier = _FakeReplyClient()
    graph = AuthorAlphaExecutionGraph(
        config=_config(per_run_same_target_burst_limit=3),
        storage=storage,
        candidate_source=_FakeDeviceFollowFeedClient([_feed_post("tweet-1", "alpha_one")]),
        drafter=_FakeDrafter(),
        reply_client=replier,
        sleep=_SleepRecorder(),
    )

    snapshot = asyncio.run(graph.invoke(AutomationRequest.for_author_alpha_engage(job_name="job", dry_run=False)))

    assert snapshot.result is not None
    assert len(replier.calls) == 1
    assert _target_success_count(storage, "tweet-1") == 4


def test_author_alpha_stops_burst_when_global_send_limit_15m_is_reached(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()
    _upsert_author(storage, "alpha_one", impressions_total_7d=100)
    base_time = datetime(2026, 4, 27, 0, 20, tzinfo=UTC)
    for index in range(48):
        created_at = (base_time - timedelta(minutes=14) + timedelta(seconds=index)).isoformat()
        _record_engagement(
            storage,
            run_id=f"historic-{index}",
            target_author=f"author-{index}",
            target_tweet_id=f"historic-tweet-{index}",
            target_tweet_url=None,
            reply_tweet_id=f"historic-reply-{index}",
            reply_url=None,
            created_at=created_at,
        )

    sleep = _SleepRecorder()
    replier = _FakeReplyClient()
    graph = AuthorAlphaExecutionGraph(
        config=_config(global_send_limit_15m=50, per_run_same_target_burst_limit=3),
        storage=storage,
        candidate_source=_FakeDeviceFollowFeedClient([_feed_post("tweet-1", "alpha_one")]),
        drafter=_FakeDrafter(),
        reply_client=replier,
        sleep=sleep,
        now=lambda: base_time,
    )

    snapshot = asyncio.run(graph.invoke(AutomationRequest.for_author_alpha_engage(job_name="job", dry_run=False)))

    assert snapshot.result is not None
    assert len(replier.calls) == 2
    assert _recent_success_count_15m(storage, base_time.isoformat()) == 50
    assert sleep.calls == [5]


def test_author_alpha_skips_author_when_per_author_daily_cap_is_reached(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()
    _upsert_author(storage, "capped_author", impressions_total_7d=100)
    _upsert_author(storage, "open_author", impressions_total_7d=90)
    now = datetime(2026, 4, 27, 2, 0, tzinfo=UTC)
    for index in range(100):
        _record_engagement(
            storage,
            run_id=f"historic-{index}",
            target_author="capped_author",
            target_tweet_id=f"capped-historic-{index}",
            target_tweet_url=None,
            reply_tweet_id=f"capped-reply-{index}",
            reply_url=None,
            created_at=f"2026-04-27T01:{index % 60:02d}:00+00:00",
        )

    replier = _FakeReplyClient()
    graph = AuthorAlphaExecutionGraph(
        config=_config(per_author_daily_success_limit=100, per_run_same_target_burst_limit=1),
        storage=storage,
        candidate_source=_FakeDeviceFollowFeedClient(
            [
                _feed_post("tweet-capped", "capped_author"),
                _feed_post("tweet-open", "open_author"),
            ]
        ),
        drafter=_FakeDrafter(),
        reply_client=replier,
        sleep=_SleepRecorder(),
        now=lambda: now,
    )

    snapshot = asyncio.run(graph.invoke(AutomationRequest.for_author_alpha_engage(job_name="job", dry_run=False)))

    assert snapshot.result is not None
    assert [call["tweet_id"] for call in replier.calls] == ["tweet-open"]


def test_author_alpha_stops_run_when_daily_execution_limit_is_reached(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()
    _upsert_author(storage, "alpha_one", impressions_total_7d=100)
    now = datetime(2026, 4, 27, 2, 0, tzinfo=UTC)

    sleep = _SleepRecorder()
    replier = _FakeReplyClient()
    graph = AuthorAlphaExecutionGraph(
        config=_config(daily_execution_limit=2, per_run_same_target_burst_limit=3),
        storage=storage,
        candidate_source=_FakeDeviceFollowFeedClient([_feed_post("tweet-1", "alpha_one")]),
        drafter=_FakeDrafter(),
        reply_client=replier,
        sleep=sleep,
        now=lambda: now,
    )

    snapshot = asyncio.run(graph.invoke(AutomationRequest.for_author_alpha_engage(job_name="job", dry_run=False)))

    assert snapshot.result is not None
    assert len(replier.calls) == 2
    assert _author_daily_success_count(storage, "alpha_one", metric_date="2026-04-27") == 2
    assert sleep.calls == [5]


def test_author_alpha_uses_configured_timezone_for_daily_author_cap(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()
    _upsert_author(storage, "alpha_one", impressions_total_7d=100)
    _record_engagement(
        storage,
        run_id="historic-1",
        target_author="alpha_one",
        target_tweet_id="historic-tweet-1",
        target_tweet_url=None,
        reply_tweet_id="historic-reply-1",
        reply_url=None,
        metric_date="2026-04-27",
        created_at="2026-04-26T16:10:00+00:00",
    )

    replier = _FakeReplyClient()
    graph = AuthorAlphaExecutionGraph(
        config=_config(
            timezone="Asia/Shanghai",
            per_author_daily_success_limit=1,
            per_run_same_target_burst_limit=1,
        ),
        storage=storage,
        candidate_source=_FakeDeviceFollowFeedClient([_feed_post("tweet-1", "alpha_one")]),
        drafter=_FakeDrafter(),
        reply_client=replier,
        sleep=_SleepRecorder(),
        now=lambda: datetime(2026, 4, 26, 16, 20, tzinfo=UTC),
    )

    snapshot = asyncio.run(graph.invoke(AutomationRequest.for_author_alpha_engage(job_name="job", dry_run=False)))

    assert snapshot.result is not None
    assert len(replier.calls) == 0


def test_author_alpha_records_success_into_shared_engagement_ledger(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    shared_storage = AutomationStorage(tmp_path / "shared.sqlite3")
    storage.initialize()
    shared_storage.initialize()
    _upsert_author(storage, "alpha_one", author_score=100, impressions_total_7d=100)

    graph = AuthorAlphaExecutionGraph(
        config=_config(max_targets_per_run=1, per_run_same_target_burst_limit=1),
        storage=storage,
        shared_storage=shared_storage,
        candidate_source=_FakeDeviceFollowFeedClient([_feed_post("tweet-1", "alpha_one")]),
        drafter=_FakeDrafter(),
        reply_client=_FakeReplyClient(),
        sleep=_SleepRecorder(),
    )

    snapshot = asyncio.run(graph.invoke(AutomationRequest.for_author_alpha_engage(job_name="job", dry_run=False)))

    assert snapshot.result is not None
    with shared_storage.connect() as connection:
        row = connection.execute(
            """
            SELECT workflow, target_tweet_id, target_author
            FROM shared_engagements
            WHERE target_tweet_id = 'tweet-1'
            """
        ).fetchone()
    assert row is not None
    assert tuple(row) == ("author-alpha-engage", "tweet-1", "alpha_one")


def test_author_alpha_skips_target_already_triggered_by_feed_engage(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    shared_storage = AutomationStorage(tmp_path / "shared.sqlite3")
    storage.initialize()
    shared_storage.initialize()
    _upsert_author(storage, "blocked_author", author_score=1000, impressions_total_7d=1000)
    _upsert_author(storage, "open_author", author_score=500, impressions_total_7d=500)

    shared_storage.record_shared_engagement(
        workflow="feed_engage",
        run_id="feed-run-1",
        target_tweet_id="tweet-blocked",
        target_author="blocked_author",
        target_tweet_url="https://x.com/blocked_author/status/tweet-blocked",
        reply_tweet_id="reply-feed-1",
        reply_url="https://x.com/i/status/reply-feed-1",
        followed=False,
        created_at="2026-04-27T00:00:00+00:00",
    )

    replier = _FakeReplyClient()
    graph = AuthorAlphaExecutionGraph(
        config=_config(max_targets_per_run=5, per_run_same_target_burst_limit=1),
        storage=storage,
        shared_storage=shared_storage,
        candidate_source=_FakeDeviceFollowFeedClient(
            [
                _feed_post("tweet-blocked", "blocked_author"),
                _feed_post("tweet-open", "open_author"),
            ]
        ),
        drafter=_FakeDrafter(),
        reply_client=replier,
        sleep=_SleepRecorder(),
    )

    snapshot = asyncio.run(graph.invoke(AutomationRequest.for_author_alpha_engage(job_name="job", dry_run=False)))

    assert snapshot.result is not None
    assert [call["tweet_id"] for call in replier.calls] == ["tweet-open"]


class _FakeDeviceFollowFeedClient:
    def __init__(self, posts: list[dict[str, object]]) -> None:
        self.posts = posts
        self.calls: list[int] = []

    def fetch_device_follow_feed(self, *, count: int) -> dict[str, object]:
        self.calls.append(count)
        return {
            "timeline_id": "tweet_notifications",
            "top_cursor": "top-x",
            "bottom_cursor": "bottom-x",
            "posts": self.posts[:count],
        }


class _FakeDrafter:
    def draft_reply(self, candidate, context=None) -> AIDraftResult:
        burst_index = 1
        if isinstance(context, dict):
            burst_index = int(context.get("burst_index") or 1)
        return AIDraftResult(
            text=f"reply-{candidate.tweet_id}-{burst_index}",
            rationale="test draft",
        )


class _FakeReplyClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    def reply(self, tweet_id: str, text: str) -> TwitterCommandResult:
        self.calls.append({"tweet_id": tweet_id, "text": text})
        return TwitterCommandResult(
            action="reply",
            ok=True,
            target_tweet_id=tweet_id,
            tweet_id=f"reply-{len(self.calls)}",
            text=text,
        )


class _SleepRecorder:
    def __init__(self) -> None:
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(float(seconds))


def _config(
    *,
    timezone: str = "Asia/Shanghai",
    excluded_authors: list[str] | None = None,
    daily_execution_limit: int = 700,
    global_send_limit_15m: int = 50,
    per_author_daily_success_limit: int = 100,
    per_target_tweet_success_limit: int = 4,
    device_follow_feed_count: int = 50,
    target_revisit_cooldown_seconds: int = 3600,
    max_targets_per_run: int = 5,
    per_run_same_target_burst_limit: int = 2,
    inter_reply_delay_seconds: int = 5,
    target_switch_delay_seconds: int = 10,
) -> AutomationConfig:
    return AutomationConfig(
        author_alpha=AuthorAlphaSettings(
            enabled=True,
            timezone=timezone,
            excluded_authors=list(excluded_authors or []),
            daily_execution_limit=daily_execution_limit,
            device_follow_feed_count=device_follow_feed_count,
            global_send_limit_15m=global_send_limit_15m,
            per_author_daily_success_limit=per_author_daily_success_limit,
            per_target_tweet_success_limit=per_target_tweet_success_limit,
            target_revisit_cooldown_seconds=target_revisit_cooldown_seconds,
            max_targets_per_run=max_targets_per_run,
            per_run_same_target_burst_limit=per_run_same_target_burst_limit,
            inter_reply_delay_seconds=inter_reply_delay_seconds,
            target_switch_delay_seconds=target_switch_delay_seconds,
        )
    )


def _tweet(tweet_id: str, screen_name: str) -> TweetRecord:
    return TweetRecord(
        tweet_id=tweet_id,
        text=f"post from {screen_name}",
        author=TweetAuthor(screen_name=screen_name, verified=True),
        created_at=datetime(2026, 4, 27, 0, 0, tzinfo=UTC),
    )


def _feed_post(tweet_id: str, screen_name: str) -> dict[str, object]:
    return {
        "id": tweet_id,
        "text": f"post from {screen_name}",
        "created_at": "2026-04-27T00:00:00Z",
        "url": f"https://x.com/{screen_name}/status/{tweet_id}",
        "author": {
            "screen_name": screen_name,
            "name": screen_name,
            "rest_id": f"rest-{screen_name}",
            "verified": True,
        },
        "public_metrics": {"likes": 0, "replies": 0, "reposts": 0, "quotes": 0},
        "reply_to_id": None,
    }


def _upsert_author(
    storage: AuthorAlphaStorage,
    screen_name: str,
    *,
    author_score: float = 0,
    impressions_total_7d: int,
) -> None:
    storage.upsert_author(
        screen_name=screen_name,
        author_name=screen_name.title(),
        rest_id=f"rest-{screen_name}",
        author_score=author_score,
        reply_count_7d=1,
        impressions_total_7d=impressions_total_7d,
        avg_impressions_7d=float(impressions_total_7d),
        max_impressions_7d=impressions_total_7d,
        last_replied_at=None,
        last_post_seen_at="2026-04-27T00:00:00+00:00",
        last_scored_at="2026-04-27T00:00:00+00:00",
        source="test",
    )


def _record_engagement(
    storage: AuthorAlphaStorage,
    *,
    run_id: str,
    target_author: str,
    target_tweet_id: str,
    target_tweet_url: str | None,
    reply_tweet_id: str,
    reply_url: str | None,
    metric_date: str | None = None,
    created_at: str,
) -> None:
    with storage.connect() as connection:
        connection.execute(
            """
            INSERT INTO alpha_engagements (
                run_id,
                target_author,
                target_tweet_id,
                target_tweet_url,
                reply_tweet_id,
                reply_url,
                metric_date,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                target_author,
                target_tweet_id,
                target_tweet_url,
                reply_tweet_id,
                reply_url,
                metric_date or created_at[:10],
                created_at,
            ),
        )


def _target_success_count(storage: AuthorAlphaStorage, target_tweet_id: str) -> int:
    with storage.connect() as connection:
        row = connection.execute(
            "SELECT COUNT(*) AS count FROM alpha_engagements WHERE target_tweet_id = ?",
            (target_tweet_id,),
        ).fetchone()
    return int(row["count"]) if row else 0


def _author_daily_success_count(
    storage: AuthorAlphaStorage,
    target_author: str,
    *,
    metric_date: str,
) -> int:
    with storage.connect() as connection:
        row = connection.execute(
            "SELECT COUNT(*) AS count FROM alpha_engagements WHERE target_author = ? AND metric_date = ?",
            (target_author, metric_date),
        ).fetchone()
    return int(row["count"]) if row else 0


def _recent_success_count_15m(storage: AuthorAlphaStorage, as_of: str) -> int:
    with storage.connect() as connection:
        row = connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM alpha_engagements
            WHERE unixepoch(created_at) >= unixepoch(datetime(?, '-15 minutes'))
              AND unixepoch(created_at) <= unixepoch(?)
            """,
            (as_of, as_of),
        ).fetchone()
    return int(row["count"]) if row else 0
