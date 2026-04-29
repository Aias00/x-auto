from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from x_atuo.automation.author_alpha_storage import AuthorAlphaStorage


def test_author_alpha_storage_initializes_schema(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")

    storage.initialize()

    assert storage.has_table("alpha_authors")
    assert storage.has_table("alpha_reply_daily_metrics")
    assert storage.has_table("alpha_author_daily_rollups")
    assert storage.has_table("alpha_sync_runs")
    assert storage.has_table("alpha_sync_checkpoints")
    assert storage.has_table("alpha_engagements")
    assert storage.has_table("alpha_runs")
    assert storage.has_table("alpha_run_audit_events")


def test_author_alpha_storage_upserts_and_orders_authors(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()

    storage.upsert_author(
        screen_name="alice",
        author_name="Alice",
        rest_id="rest-1",
        author_score=10.5,
        reply_count_7d=3,
        impressions_total_7d=300,
        avg_impressions_7d=100.0,
        max_impressions_7d=180,
        last_replied_at="2026-04-26T01:00:00+00:00",
        last_post_seen_at="2026-04-26T02:00:00+00:00",
        last_scored_at="2026-04-26T03:00:00+00:00",
        source="bootstrap",
    )
    storage.upsert_author(
        screen_name="bob",
        author_name="Bob",
        rest_id="rest-2",
        author_score=7.0,
        reply_count_7d=2,
        impressions_total_7d=250,
        avg_impressions_7d=125.0,
        max_impressions_7d=170,
        last_replied_at=None,
        last_post_seen_at="2026-04-26T04:00:00+00:00",
        last_scored_at="2026-04-26T05:00:00+00:00",
        source="reconcile",
    )
    storage.upsert_author(
        screen_name="alice",
        author_name="Alice Updated",
        rest_id="rest-1b",
        author_score=12.0,
        reply_count_7d=4,
        impressions_total_7d=420,
        avg_impressions_7d=105.0,
        max_impressions_7d=190,
        last_replied_at="2026-04-27T01:00:00+00:00",
        last_post_seen_at="2026-04-27T02:00:00+00:00",
        last_scored_at="2026-04-27T03:00:00+00:00",
        source="reconcile",
    )

    authors = storage.list_authors_ordered_by_score()

    assert [author["screen_name"] for author in authors] == ["alice", "bob"]
    assert authors[0]["author_name"] == "Alice Updated"
    assert authors[0]["author_score"] == pytest.approx(12.0)
    assert authors[0]["reply_count_7d"] == 4


def test_author_alpha_storage_upserts_daily_metrics_and_rollups(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()

    storage.upsert_reply_daily_metrics(
        metric_date="2026-04-26",
        reply_tweet_id="reply-1",
        target_tweet_id="target-1",
        target_author="alice",
        impressions=100,
        likes=10,
        replies=2,
        reposts=1,
        sampled_at="2026-04-26T08:00:00+00:00",
    )
    storage.upsert_reply_daily_metrics(
        metric_date="2026-04-26",
        reply_tweet_id="reply-1",
        target_tweet_id="target-1",
        target_author="alice",
        impressions=150,
        likes=12,
        replies=3,
        reposts=2,
        sampled_at="2026-04-26T09:00:00+00:00",
    )
    storage.upsert_author_daily_rollup(
        metric_date="2026-04-26",
        target_author="alice",
        reply_count=1,
        impressions_total=150,
        likes_total=12,
        replies_total=3,
        reposts_total=2,
        avg_impressions=150.0,
        max_impressions=150,
        computed_at="2026-04-26T10:00:00+00:00",
    )
    storage.upsert_author_daily_rollup(
        metric_date="2026-04-26",
        target_author="alice",
        reply_count=2,
        impressions_total=240,
        likes_total=20,
        replies_total=4,
        reposts_total=2,
        avg_impressions=120.0,
        max_impressions=150,
        computed_at="2026-04-26T11:00:00+00:00",
    )

    reply_row = storage.get_reply_daily_metric("2026-04-26", "reply-1")
    rollup_row = storage.get_author_daily_rollup("2026-04-26", "alice")

    assert reply_row is not None
    assert reply_row["impressions"] == 150
    assert reply_row["likes"] == 12
    assert rollup_row is not None
    assert rollup_row["reply_count"] == 2
    assert rollup_row["impressions_total"] == 240


def test_author_alpha_storage_tracks_sync_runs_and_checkpoints(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()

    storage.record_sync_run(
        run_id="run-1",
        run_type="bootstrap",
        status="running",
        from_date="2026-04-20",
        to_date="2026-04-26",
        current_date="2026-04-20",
        days_completed=0,
        days_total=7,
        resume_from_date="2026-04-20",
    )
    storage.update_sync_run(
        "run-1",
        status="completed",
        current_date="2026-04-26",
        days_completed=7,
        resume_from_date=None,
        finished_at="2026-04-27T00:00:00+00:00",
    )
    storage.write_checkpoint(
        sync_scope="bootstrap",
        last_completed_date="2026-04-26",
        next_pending_date="2026-04-27",
        last_run_id="run-1",
        updated_at="2026-04-27T00:00:00+00:00",
    )

    run = storage.get_sync_run("run-1")
    checkpoint = storage.read_checkpoint("bootstrap")

    assert run is not None
    assert run["status"] == "completed"
    assert run["days_completed"] == 7
    assert run["resume_from_date"] is None
    assert checkpoint == {
        "sync_scope": "bootstrap",
        "last_completed_date": "2026-04-26",
        "next_pending_date": "2026-04-27",
        "last_run_id": "run-1",
        "updated_at": "2026-04-27T00:00:00+00:00",
    }
    assert storage.get_active_sync_run() is None
    assert [row["run_id"] for row in storage.list_sync_runs(limit=5)] == ["run-1"]


def test_author_alpha_storage_counts_target_and_author_successes(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()

    storage.record_engagement(
        run_id="run-1",
        target_author="alice",
        target_tweet_id="tweet-1",
        target_tweet_url="https://x.com/alice/status/tweet-1",
        reply_tweet_id="reply-1",
        reply_url="https://x.com/i/status/reply-1",
        burst_id="burst-1",
        burst_index=1,
        burst_size=3,
        created_at="2026-04-27T00:01:00+00:00",
    )
    storage.record_engagement(
        run_id="run-1",
        target_author="alice",
        target_tweet_id="tweet-1",
        target_tweet_url="https://x.com/alice/status/tweet-1",
        reply_tweet_id="reply-2",
        reply_url="https://x.com/i/status/reply-2",
        burst_id="burst-1",
        burst_index=2,
        burst_size=3,
        created_at="2026-04-27T00:10:00+00:00",
    )
    storage.record_engagement(
        run_id="run-2",
        target_author="alice",
        target_tweet_id="tweet-2",
        target_tweet_url="https://x.com/alice/status/tweet-2",
        reply_tweet_id="reply-3",
        reply_url="https://x.com/i/status/reply-3",
        burst_id="burst-2",
        burst_index=1,
        burst_size=1,
        created_at="2026-04-27T00:20:00+00:00",
    )
    storage.record_engagement(
        run_id="run-3",
        target_author="bob",
        target_tweet_id="tweet-3",
        target_tweet_url="https://x.com/bob/status/tweet-3",
        reply_tweet_id="reply-4",
        reply_url="https://x.com/i/status/reply-4",
        burst_id="burst-3",
        burst_index=1,
        burst_size=1,
        created_at="2026-04-26T23:50:00+00:00",
    )

    assert storage.get_target_success_count("tweet-1") == 2
    assert storage.get_author_daily_success_count(
        "alice", metric_date="2026-04-27"
    ) == 3
    assert storage.get_recent_success_count_15m("2026-04-27T00:20:00+00:00") == 2

    with storage.connect() as connection:
        row = connection.execute(
            """
            SELECT burst_id, burst_index, burst_size
            FROM alpha_engagements
            WHERE reply_tweet_id = 'reply-2'
            """
        ).fetchone()

    assert tuple(row) == ("burst-1", 2, 3)


def test_author_alpha_storage_counts_recent_successes_inclusively_across_offsets(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()

    storage.record_engagement(
        run_id="run-1",
        target_author="alice",
        target_tweet_id="tweet-1",
        target_tweet_url=None,
        reply_tweet_id="reply-1",
        reply_url=None,
        created_at="2026-04-27T08:05:00+08:00",
    )
    storage.record_engagement(
        run_id="run-1",
        target_author="alice",
        target_tweet_id="tweet-2",
        target_tweet_url=None,
        reply_tweet_id="reply-2",
        reply_url=None,
        created_at="2026-04-27T00:20:00+00:00",
    )

    assert storage.get_recent_success_count_15m("2026-04-27T00:20:00+00:00") == 2


def test_author_alpha_storage_rejects_partial_engagement_identity(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()

    with pytest.raises(ValueError):
        storage.record_engagement(
            run_id="run-1",
            target_author="",
            target_tweet_id="tweet-1",
            target_tweet_url=None,
            reply_tweet_id="reply-1",
            reply_url=None,
            created_at="2026-04-27T00:20:00+00:00",
        )


def test_author_alpha_storage_counts_author_day_by_explicit_metric_date(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()

    storage.record_engagement(
        run_id="run-1",
        target_author="alice",
        target_tweet_id="tweet-1",
        target_tweet_url=None,
        reply_tweet_id="reply-1",
        reply_url=None,
        metric_date="2026-04-27",
        created_at="2026-04-26T16:10:00+00:00",
    )

    assert storage.get_author_daily_success_count("alice", metric_date="2026-04-27") == 1
    assert storage.get_daily_success_count(metric_date="2026-04-27") == 1
    assert storage.get_author_daily_success_count("alice", metric_date="2026-04-26") == 0


def test_author_alpha_storage_zeroes_stale_authors(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()

    storage.upsert_author(
        screen_name="alice",
        author_name="Alice",
        rest_id="rest-1",
        author_score=9.0,
        reply_count_7d=3,
        impressions_total_7d=300,
        avg_impressions_7d=100.0,
        max_impressions_7d=150,
        last_replied_at="2026-04-25T01:00:00+00:00",
        last_post_seen_at="2026-04-25T02:00:00+00:00",
        last_scored_at="2026-04-25T03:00:00+00:00",
        source="bootstrap",
    )
    storage.upsert_author(
        screen_name="bob",
        author_name="Bob",
        rest_id="rest-2",
        author_score=8.0,
        reply_count_7d=2,
        impressions_total_7d=200,
        avg_impressions_7d=100.0,
        max_impressions_7d=120,
        last_replied_at="2026-04-25T04:00:00+00:00",
        last_post_seen_at="2026-04-25T05:00:00+00:00",
        last_scored_at="2026-04-25T06:00:00+00:00",
        source="bootstrap",
    )

    updated = storage.zero_out_stale_authors({"alice"}, scored_at="2026-04-27T00:00:00+00:00")

    authors = {author["screen_name"]: author for author in storage.list_authors_ordered_by_score()}

    assert updated == 1
    assert authors["alice"]["author_score"] == pytest.approx(9.0)
    assert authors["bob"]["author_score"] == pytest.approx(0.0)
    assert authors["bob"]["reply_count_7d"] == 0
    assert authors["bob"]["impressions_total_7d"] == 0
    assert authors["bob"]["avg_impressions_7d"] == pytest.approx(0.0)
    assert authors["bob"]["max_impressions_7d"] == 0
    assert authors["bob"]["last_scored_at"] == "2026-04-27T00:00:00+00:00"


def test_author_alpha_storage_lists_rollups_in_date_order(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()

    storage.upsert_author_daily_rollup(
        metric_date="2026-04-27",
        target_author="bob",
        reply_count=1,
        impressions_total=80,
        likes_total=3,
        replies_total=1,
        reposts_total=0,
        avg_impressions=80.0,
        max_impressions=80,
        computed_at="2026-04-27T10:00:00+00:00",
    )
    storage.upsert_author_daily_rollup(
        metric_date="2026-04-26",
        target_author="alice",
        reply_count=2,
        impressions_total=120,
        likes_total=5,
        replies_total=2,
        reposts_total=1,
        avg_impressions=60.0,
        max_impressions=75,
        computed_at="2026-04-26T10:00:00+00:00",
    )

    rows = storage.list_author_daily_rollups("2026-04-26", "2026-04-27")

    assert [(row["metric_date"], row["target_author"]) for row in rows] == [
        ("2026-04-26", "alice"),
        ("2026-04-27", "bob"),
    ]


def test_author_alpha_storage_writes_sqlite_file(tmp_path: Path) -> None:
    db_path = tmp_path / "author-alpha.sqlite3"
    storage = AuthorAlphaStorage(db_path)

    storage.initialize()

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'alpha_authors'"
        ).fetchone()
    finally:
        conn.close()

    assert row == ("alpha_authors",)


def test_author_alpha_storage_persists_execution_run_lookup(tmp_path: Path) -> None:
    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()

    storage.create_execution_run(
        run_id="run-1",
        job_id="scheduled-author-alpha-engage",
        job_type="author_alpha_engage",
        endpoint="scheduler:author-alpha-engage",
        request_payload={"dry_run": False},
        status="queued",
    )
    storage.add_execution_audit_event(
        run_id="run-1",
        event_type="trigger_received",
        node="service",
        payload={"endpoint": "scheduler:author-alpha-engage"},
    )
    storage.update_execution_run(
        "run-1",
        status="completed",
        response_payload={"status": "completed"},
        started_at="2026-04-27T00:00:00+00:00",
        finished_at="2026-04-27T00:01:00+00:00",
    )

    payload = storage.get_execution_run("run-1")

    assert payload is not None
    assert payload["run"]["id"] == "run-1"
    assert payload["run"]["job_type"] == "author_alpha_engage"
    assert payload["run"]["response_payload"] == {"status": "completed"}
    assert payload["audit_events"][0]["event_type"] == "trigger_received"
