from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

from x_atuo.core.twitter_client import TwitterClient, TwitterCredentials
from x_atuo.core.twitter_models import TweetAuthor, TweetRecord


def test_fetch_tweet_enriches_reply_ancestry_fields_from_detail_payload() -> None:
    class _StubTwitterClient(TwitterClient):
        def _build_native_client(self):
            class NativeAuthor:
                id = "user-1"
                name = "Replier"
                screen_name = "replier"
                profile_image_url = ""
                verified = True

            class NativeMetrics:
                likes = 0
                retweets = 0
                replies = 0
                quotes = 0
                views = 0
                bookmarks = 0

            class NativeTweet:
                id = "reply-1"
                text = "reply body"
                author = NativeAuthor()
                metrics = NativeMetrics()
                created_at = "2026-04-29T00:00:00+00:00"
                media = []
                urls = []
                is_retweet = False
                lang = "en"
                retweeted_by = None
                quoted_tweet = None
                score = None
                article_title = None
                article_text = None
                is_subscriber_only = False
                is_promoted = False

            return type("NativeClient", (), {"fetch_tweet_detail": lambda self_, tweet_id, count=1: [NativeTweet()]})()

        def _fetch_tweet_detail_payload(self, tweet_id: str):
            return {
                "data": {
                    "threaded_conversation_with_injections_v2": {
                        "instructions": [
                            {
                                "entries": [
                                    {
                                        "content": {
                                            "itemContent": {
                                                "tweet_results": {
                                                    "result": {
                                                        "__typename": "Tweet",
                                                        "rest_id": "reply-1",
                                                        "legacy": {
                                                            "id_str": "reply-1",
                                                            "conversation_id_str": "conversation-1",
                                                            "in_reply_to_status_id_str": "target-1",
                                                            "in_reply_to_screen_name": "target_author",
                                                        },
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    {
                                        "content": {
                                            "itemContent": {
                                                "tweet_results": {
                                                    "result": {
                                                        "__typename": "Tweet",
                                                        "rest_id": "target-1",
                                                        "core": {
                                                            "user_results": {
                                                                "result": {
                                                                    "legacy": {
                                                                        "screen_name": "target_author"
                                                                    }
                                                                }
                                                            }
                                                        },
                                                    }
                                                }
                                            }
                                        }
                                    },
                                ]
                            }
                        ]
                    }
                }
            }

    client = _StubTwitterClient(credentials=TwitterCredentials(auth_token="token", ct0="csrf"))

    tweet = client.fetch_tweet("reply-1")

    assert tweet.reply_to_tweet_id == "target-1"
    assert tweet.reply_to_screen_name == "target_author"
    assert tweet.conversation_id == "conversation-1"
    assert tweet.target_tweet_id == "target-1"
    assert tweet.target_screen_name == "target_author"


def test_fetch_tweet_enriches_original_target_fields_for_nested_reply_payload() -> None:
    class _StubTwitterClient(TwitterClient):
        def _build_native_client(self):
            class NativeAuthor:
                id = "user-1"
                name = "Replier"
                screen_name = "replier"
                profile_image_url = ""
                verified = True

            class NativeMetrics:
                likes = 0
                retweets = 0
                replies = 0
                quotes = 0
                views = 0
                bookmarks = 0

            class NativeTweet:
                id = "reply-2"
                text = "nested reply body"
                author = NativeAuthor()
                metrics = NativeMetrics()
                created_at = "2026-04-29T00:00:00+00:00"
                media = []
                urls = []
                is_retweet = False
                lang = "en"
                retweeted_by = None
                quoted_tweet = None
                score = None
                article_title = None
                article_text = None
                is_subscriber_only = False
                is_promoted = False

            return type("NativeClient", (), {"fetch_tweet_detail": lambda self_, tweet_id, count=1: [NativeTweet()]})()

        def _fetch_tweet_detail_payload(self, tweet_id: str):
            return {
                "data": {
                    "threaded_conversation_with_injections_v2": {
                        "instructions": [
                            {
                                "entries": [
                                    {
                                        "content": {
                                            "itemContent": {
                                                "tweet_results": {
                                                    "result": {
                                                        "__typename": "Tweet",
                                                        "rest_id": "reply-2",
                                                        "legacy": {
                                                            "id_str": "reply-2",
                                                            "conversation_id_str": "target-1",
                                                            "in_reply_to_status_id_str": "our-reply-1",
                                                            "in_reply_to_screen_name": "our_account",
                                                        },
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    {
                                        "content": {
                                            "itemContent": {
                                                "tweet_results": {
                                                    "result": {
                                                        "__typename": "Tweet",
                                                        "rest_id": "our-reply-1",
                                                        "core": {
                                                            "user_results": {
                                                                "result": {
                                                                    "legacy": {
                                                                        "screen_name": "our_account"
                                                                    }
                                                                }
                                                            }
                                                        },
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    {
                                        "content": {
                                            "itemContent": {
                                                "tweet_results": {
                                                    "result": {
                                                        "__typename": "Tweet",
                                                        "rest_id": "target-1",
                                                        "core": {
                                                            "user_results": {
                                                                "result": {
                                                                    "legacy": {"screen_name": "alice"}
                                                                }
                                                            }
                                                        },
                                                    }
                                                }
                                            }
                                        }
                                    },
                                ]
                            }
                        ]
                    }
                }
            }

    client = _StubTwitterClient(credentials=TwitterCredentials(auth_token="token", ct0="csrf"))

    tweet = client.fetch_tweet("reply-2")

    assert tweet.reply_to_tweet_id == "our-reply-1"
    assert tweet.reply_to_screen_name == "our_account"
    assert tweet.target_tweet_id == "target-1"
    assert tweet.target_screen_name == "alice"


def test_bootstrap_backfills_days_writes_rollups_and_scores() -> None:
    storage = _FakeAuthorAlphaStorage()
    analytics = _FakeAnalyticsClient(
        timezone="Asia/Shanghai",
        posts_by_day={
            "2026-04-20": [
                _reply_post("reply-1", "target-1", impressions=120, likes=8, replies=2, reposts=1),
            ],
            "2026-04-21": [
                _reply_post("reply-2", "target-2", impressions=75, likes=4, replies=1, reposts=0),
            ],
            "2026-04-22": [
                _reply_post("reply-3", "target-3", impressions=40, likes=2, replies=0, reposts=0),
            ],
        },
    )
    twitter = _FakeTwitterClient(
        {
            "reply-1": _reply_record("reply-1", reply_to_tweet_id="target-1", reply_to_screen_name="alice"),
            "reply-2": _reply_record("reply-2", reply_to_tweet_id="target-2", reply_to_screen_name="bob"),
            "reply-3": _reply_record("reply-3", reply_to_tweet_id="target-3", reply_to_screen_name="alice"),
        }
    )

    from x_atuo.automation.author_alpha_sync import AuthorAlphaSync

    sync = AuthorAlphaSync(
        storage=storage,
        analytics_client=analytics,
        twitter_client=twitter,
        timezone="Asia/Shanghai",
        score_min_daily_replies=0,
    )

    result = sync.bootstrap(from_date="2026-04-20", to_date="2026-04-22")

    assert result["days_completed"] == 3
    assert storage.count_reply_daily_metrics() == 3
    assert storage.count_author_daily_rollups() == 3
    assert analytics.calls[0] == (
        "2026-04-19T16:00:00Z",
        "2026-04-20T15:59:59Z",
    )
    assert analytics.calls[-1] == (
        "2026-04-21T16:00:00Z",
        "2026-04-22T15:59:59Z",
    )
    assert storage.reply_daily_metrics[("2026-04-20", "reply-1")]["target_author"] == "alice"
    assert storage.authors["alice"]["author_score"] > storage.authors["bob"]["author_score"]
    assert storage.authors["alice"]["reply_count_7d"] == 2
    assert storage.authors["alice"]["avg_impressions_7d"] == 80.0
    assert storage.authors["alice"]["max_impressions_7d"] == 120
    assert storage.checkpoints["bootstrap"]["last_completed_date"] == "2026-04-22"
    assert storage.checkpoints["bootstrap"]["next_pending_date"] is None


def test_bootstrap_uses_fast_parent_reply_path_without_fetching_tweet_detail() -> None:
    storage = _FakeAuthorAlphaStorage()
    analytics = _FakeAnalyticsClient(
        timezone="UTC",
        posts_by_day={
            "2026-04-20": [
                _reply_post(
                    "reply-fast-1",
                    "target-fast-1",
                    impressions=55,
                    likes=4,
                    replies=1,
                    reposts=0,
                    text="@TMGC_net @candytoy_c Cool",
                ),
            ],
        },
    )
    twitter = _FakeTwitterClient({})

    from x_atuo.automation.author_alpha_sync import AuthorAlphaSync

    sync = AuthorAlphaSync(
        storage=storage,
        analytics_client=analytics,
        twitter_client=twitter,
        timezone="UTC",
        score_min_daily_replies=0,
    )

    sync.bootstrap(from_date="2026-04-20", to_date="2026-04-20")

    assert storage.reply_daily_metrics[("2026-04-20", "reply-fast-1")]["target_tweet_id"] == "target-fast-1"
    assert storage.reply_daily_metrics[("2026-04-20", "reply-fast-1")]["target_author"] == "TMGC_net"
    assert twitter.calls == []


def test_bootstrap_scores_nested_replies_against_original_target_author() -> None:
    storage = _FakeAuthorAlphaStorage()
    analytics = _FakeAnalyticsClient(
        timezone="UTC",
        posts_by_day={
            "2026-04-20": [
                _reply_post("reply-1", "our-reply-1", impressions=120, likes=8, replies=2, reposts=1),
            ],
        },
    )
    twitter = _FakeTwitterClient(
        {
            "reply-1": _reply_record(
                "reply-1",
                reply_to_tweet_id="our-reply-1",
                reply_to_screen_name="our_account",
                target_tweet_id="target-1",
                target_screen_name="alice",
            ),
        }
    )

    from x_atuo.automation.author_alpha_sync import AuthorAlphaSync

    sync = AuthorAlphaSync(
        storage=storage,
        analytics_client=analytics,
        twitter_client=twitter,
        timezone="UTC",
        score_min_daily_replies=0,
    )

    sync.bootstrap(from_date="2026-04-20", to_date="2026-04-20")

    assert storage.reply_daily_metrics[("2026-04-20", "reply-1")]["target_tweet_id"] == "target-1"
    assert storage.reply_daily_metrics[("2026-04-20", "reply-1")]["target_author"] == "alice"
    assert storage.authors["alice"]["author_score"] < storage.authors["alice"]["impressions_total_7d"]
    assert "our_account" not in storage.authors


def test_bootstrap_resume_starts_from_checkpoint_next_pending_date() -> None:
    storage = _FakeAuthorAlphaStorage()
    storage.write_checkpoint(
        sync_scope="bootstrap",
        last_completed_date="2026-04-20",
        next_pending_date="2026-04-21",
        last_run_id="prior-run",
        updated_at="2026-04-21T00:00:00Z",
    )
    analytics = _FakeAnalyticsClient(
        timezone="UTC",
        posts_by_day={
            "2026-04-21": [_reply_post("reply-2", "target-2", impressions=50, likes=3, replies=1, reposts=0)],
            "2026-04-22": [_reply_post("reply-3", "target-3", impressions=25, likes=1, replies=0, reposts=0)],
        },
    )
    twitter = _FakeTwitterClient(
        {
            "reply-2": _reply_record("reply-2", reply_to_tweet_id="target-2", reply_to_screen_name="bob"),
            "reply-3": _reply_record("reply-3", reply_to_tweet_id="target-3", reply_to_screen_name="carol"),
        }
    )

    from x_atuo.automation.author_alpha_sync import AuthorAlphaSync

    sync = AuthorAlphaSync(
        storage=storage,
        analytics_client=analytics,
        twitter_client=twitter,
        timezone="UTC",
        score_min_daily_replies=0,
    )
    result = sync.bootstrap(from_date="2026-04-20", to_date="2026-04-22", resume=True)

    assert result["days_completed"] == 2
    assert [day for day, _ in storage.reply_daily_metrics] == ["2026-04-21", "2026-04-22"]
    assert analytics.requested_days == ["2026-04-21", "2026-04-22"]


def test_reconcile_replaces_same_day_rollups_when_target_author_changes() -> None:
    storage = _FakeAuthorAlphaStorage()
    analytics = _FakeAnalyticsClient(
        timezone="UTC",
        posts_by_day={
            "2026-04-20": [_reply_post("reply-1", "our-reply-1", impressions=120, likes=8, replies=2, reposts=1)],
        },
    )
    twitter = _FakeTwitterClient(
        {
            "reply-1": _reply_record("reply-1", reply_to_tweet_id="our-reply-1", reply_to_screen_name="our_account"),
        }
    )

    from x_atuo.automation.author_alpha_sync import AuthorAlphaSync

    sync = AuthorAlphaSync(
        storage=storage,
        analytics_client=analytics,
        twitter_client=twitter,
        timezone="UTC",
        score_min_daily_replies=0,
    )
    sync.reconcile(target_date="2026-04-20")
    assert ("2026-04-20", "our_account") in storage.author_daily_rollups

    twitter.tweets["reply-1"] = _reply_record(
        "reply-1",
        reply_to_tweet_id="our-reply-1",
        reply_to_screen_name="our_account",
        target_tweet_id="target-1",
        target_screen_name="alice",
    )

    sync.reconcile(target_date="2026-04-20")

    assert ("2026-04-20", "our_account") not in storage.author_daily_rollups
    assert ("2026-04-20", "alice") in storage.author_daily_rollups


def test_reconcile_zeroes_authors_with_no_recent_rows() -> None:
    storage = _FakeAuthorAlphaStorage()
    storage.upsert_author(
        screen_name="alice",
        author_name="Alice",
        rest_id=None,
        author_score=999,
        reply_count_7d=9,
        impressions_total_7d=999,
        avg_impressions_7d=111.0,
        max_impressions_7d=500,
        last_replied_at=None,
        last_post_seen_at=None,
        last_scored_at="2026-04-19T00:00:00Z",
        source="bootstrap",
    )
    storage.upsert_author(
        screen_name="bob",
        author_name="Bob",
        rest_id=None,
        author_score=500,
        reply_count_7d=5,
        impressions_total_7d=500,
        avg_impressions_7d=100.0,
        max_impressions_7d=250,
        last_replied_at=None,
        last_post_seen_at=None,
        last_scored_at="2026-04-19T00:00:00Z",
        source="bootstrap",
    )
    analytics = _FakeAnalyticsClient(
        timezone="UTC",
        posts_by_day={
            "2026-04-27": [_reply_post("reply-9", "target-9", impressions=88, likes=7, replies=2, reposts=1)],
        },
    )
    twitter = _FakeTwitterClient(
        {
            "reply-9": _reply_record("reply-9", reply_to_tweet_id="target-9", reply_to_screen_name="bob"),
        }
    )

    from x_atuo.automation.author_alpha_sync import AuthorAlphaSync

    sync = AuthorAlphaSync(
        storage=storage,
        analytics_client=analytics,
        twitter_client=twitter,
        timezone="UTC",
        score_min_daily_replies=0,
    )
    result = sync.reconcile(target_date="2026-04-27")

    assert result["metric_date"] == "2026-04-27"
    assert storage.authors["bob"]["author_score"] < storage.authors["bob"]["impressions_total_7d"]
    assert storage.authors["alice"]["author_score"] == 0
    assert storage.authors["alice"]["reply_count_7d"] == 0
    assert storage.zeroed_authors == ["alice"]


def test_recompute_scores_uses_shrinkage_and_small_sample_penalty() -> None:
    storage = _FakeAuthorAlphaStorage()
    analytics = _FakeAnalyticsClient(timezone="UTC", posts_by_day={})
    twitter = _FakeTwitterClient({})

    from x_atuo.automation.author_alpha_sync import AuthorAlphaSync

    sync = AuthorAlphaSync(
        storage=storage,
        analytics_client=analytics,
        twitter_client=twitter,
        timezone="UTC",
        score_min_daily_replies=0,
    )

    storage.replace_day_sync_snapshot(
        metric_date="2026-04-20",
        reply_metrics=[
            {
                "reply_tweet_id": "a-1",
                "target_tweet_id": "ta-1",
                "target_author": "author_a",
                "impressions": 100,
                "likes": 0,
                "replies": 0,
                "reposts": 0,
                "sampled_at": "2026-04-20T00:00:00Z",
            },
            {
                "reply_tweet_id": "a-2",
                "target_tweet_id": "ta-2",
                "target_author": "author_a",
                "impressions": 100,
                "likes": 0,
                "replies": 0,
                "reposts": 0,
                "sampled_at": "2026-04-20T00:00:00Z",
            },
            {
                "reply_tweet_id": "b-1",
                "target_tweet_id": "tb-1",
                "target_author": "author_b",
                "impressions": 100,
                "likes": 0,
                "replies": 0,
                "reposts": 0,
                "sampled_at": "2026-04-20T00:00:00Z",
            },
        ],
        author_rollups=[],
    )

    summary = sync._recompute_scores(reference_date=date.fromisoformat("2026-04-20"), source="test")

    assert summary["winsorization_cap_p95"] == 100
    assert round(summary["global_winsorized_mean"], 6) == 100.0
    assert summary["score_prior_weight"] == 7.0
    assert summary["score_penalty_constant"] == 200.0
    assert storage.authors["author_a"]["author_score"] > storage.authors["author_b"]["author_score"]


def test_recompute_scores_winsorizes_single_outlier() -> None:
    storage = _FakeAuthorAlphaStorage()
    analytics = _FakeAnalyticsClient(timezone="UTC", posts_by_day={})
    twitter = _FakeTwitterClient({})

    from x_atuo.automation.author_alpha_sync import AuthorAlphaSync

    sync = AuthorAlphaSync(
        storage=storage,
        analytics_client=analytics,
        twitter_client=twitter,
        timezone="UTC",
        score_min_daily_replies=0,
    )

    reply_metrics = [
        {
            "reply_tweet_id": "outlier-1",
            "target_tweet_id": "to-1",
            "target_author": "outlier_author",
            "impressions": 100000,
            "likes": 0,
            "replies": 0,
            "reposts": 0,
            "sampled_at": "2026-04-20T00:00:00Z",
        }
    ]
    for index in range(1, 20):
        reply_metrics.append(
            {
                "reply_tweet_id": f"stable-{index}",
                "target_tweet_id": f"ts-{index}",
                "target_author": "stable_author",
                "impressions": 1500,
                "likes": 0,
                "replies": 0,
                "reposts": 0,
                "sampled_at": "2026-04-20T00:00:00Z",
            }
        )
    storage.replace_day_sync_snapshot(metric_date="2026-04-20", reply_metrics=reply_metrics, author_rollups=[])

    summary = sync._recompute_scores(reference_date=date.fromisoformat("2026-04-20"), source="test")

    assert summary["winsorization_cap_p95"] == 1500
    assert storage.authors["stable_author"]["author_score"] > storage.authors["outlier_author"]["author_score"]


def test_recompute_scores_respects_configured_prior_weight_and_penalty() -> None:
    storage = _FakeAuthorAlphaStorage()
    analytics = _FakeAnalyticsClient(timezone="UTC", posts_by_day={})
    twitter = _FakeTwitterClient({})

    from x_atuo.automation.author_alpha_sync import AuthorAlphaSync

    sync = AuthorAlphaSync(
        storage=storage,
        analytics_client=analytics,
        twitter_client=twitter,
        timezone="UTC",
        score_min_daily_replies=0,
        score_prior_weight=20.0,
        score_penalty_constant=500.0,
    )

    storage.replace_day_sync_snapshot(
        metric_date="2026-04-20",
        reply_metrics=[
            {
                "reply_tweet_id": "author-a-1",
                "target_tweet_id": "ta-1",
                "target_author": "author_a",
                "impressions": 1000,
                "likes": 0,
                "replies": 0,
                "reposts": 0,
                "sampled_at": "2026-04-20T00:00:00Z",
            },
            {
                "reply_tweet_id": "author-b-1",
                "target_tweet_id": "tb-1",
                "target_author": "author_b",
                "impressions": 1000,
                "likes": 0,
                "replies": 0,
                "reposts": 0,
                "sampled_at": "2026-04-20T00:00:00Z",
            },
            {
                "reply_tweet_id": "author-b-2",
                "target_tweet_id": "tb-2",
                "target_author": "author_b",
                "impressions": 1000,
                "likes": 0,
                "replies": 0,
                "reposts": 0,
                "sampled_at": "2026-04-20T00:00:00Z",
            },
        ],
        author_rollups=[],
    )

    summary = sync._recompute_scores(reference_date=date.fromisoformat("2026-04-20"), source="test")

    assert summary["score_prior_weight"] == 20.0
    assert summary["score_penalty_constant"] == 500.0
    assert storage.authors["author_b"]["author_score"] > storage.authors["author_a"]["author_score"]


def test_recompute_scores_filters_out_low_volume_days() -> None:
    storage = _FakeAuthorAlphaStorage()
    analytics = _FakeAnalyticsClient(timezone="UTC", posts_by_day={})
    twitter = _FakeTwitterClient({})

    from x_atuo.automation.author_alpha_sync import AuthorAlphaSync

    sync = AuthorAlphaSync(
        storage=storage,
        analytics_client=analytics,
        twitter_client=twitter,
        timezone="UTC",
        score_min_daily_replies=3,
        score_prior_weight=0.0,
        score_penalty_constant=0.0,
    )

    storage.replace_day_sync_snapshot(
        metric_date="2026-04-20",
        reply_metrics=[
            {
                "reply_tweet_id": "low-1",
                "target_tweet_id": "tl-1",
                "target_author": "low_day_author",
                "impressions": 5000,
                "likes": 0,
                "replies": 0,
                "reposts": 0,
                "sampled_at": "2026-04-20T00:00:00Z",
            },
            {
                "reply_tweet_id": "low-2",
                "target_tweet_id": "tl-2",
                "target_author": "low_day_author",
                "impressions": 5000,
                "likes": 0,
                "replies": 0,
                "reposts": 0,
                "sampled_at": "2026-04-20T00:00:00Z",
            },
        ],
        author_rollups=[],
    )
    storage.replace_day_sync_snapshot(
        metric_date="2026-04-21",
        reply_metrics=[
            {
                "reply_tweet_id": "high-1",
                "target_tweet_id": "th-1",
                "target_author": "high_day_author",
                "impressions": 100,
                "likes": 0,
                "replies": 0,
                "reposts": 0,
                "sampled_at": "2026-04-21T00:00:00Z",
            },
            {
                "reply_tweet_id": "high-2",
                "target_tweet_id": "th-2",
                "target_author": "high_day_author",
                "impressions": 100,
                "likes": 0,
                "replies": 0,
                "reposts": 0,
                "sampled_at": "2026-04-21T00:00:00Z",
            },
            {
                "reply_tweet_id": "high-3",
                "target_tweet_id": "th-3",
                "target_author": "high_day_author",
                "impressions": 100,
                "likes": 0,
                "replies": 0,
                "reposts": 0,
                "sampled_at": "2026-04-21T00:00:00Z",
            },
        ],
        author_rollups=[],
    )

    summary = sync._recompute_scores(reference_date=date.fromisoformat("2026-04-21"), source="test")

    assert summary["included_metric_dates"] == ["2026-04-21"]
    assert summary["score_min_daily_replies"] == 3
    assert "high_day_author" in storage.authors
    assert "low_day_author" not in storage.authors


def test_reconcile_persists_error_message_on_failure() -> None:
    storage = _FakeAuthorAlphaStorage()

    class _FailingAnalyticsClient(_FakeAnalyticsClient):
        def fetch_content_posts(
            self,
            *,
            start_time: str,
            end_time: str,
            post_limit: int,
            query_page_size: int | None = None,
        ) -> dict[str, object]:
            raise RuntimeError("analytics boom")

    analytics = _FailingAnalyticsClient(timezone="UTC", posts_by_day={})
    twitter = _FakeTwitterClient({})

    from x_atuo.automation.author_alpha_sync import AuthorAlphaSync

    sync = AuthorAlphaSync(storage=storage, analytics_client=analytics, twitter_client=twitter, timezone="UTC")

    try:
        sync.reconcile(target_date="2026-04-27")
    except RuntimeError as exc:
        assert str(exc) == "analytics boom"
    else:
        raise AssertionError("expected reconcile to fail")

    run = next(iter(storage.sync_runs.values()))
    assert run["status"] == "failed"
    assert run["error"] == "analytics boom"


def _reply_post(
    reply_tweet_id: str,
    reply_to_id: str,
    *,
    impressions: int,
    likes: int,
    replies: int,
    reposts: int,
    text: str | None = None,
) -> dict[str, object]:
    return {
        "id": reply_tweet_id,
        "text": text or "",
        "reply_to_id": reply_to_id,
        "public_metrics": {
            "impressions": impressions,
            "likes": likes,
            "replies": replies,
            "reposts": reposts,
        },
        "metrics_total": {
            "impressions": impressions,
            "likes": likes,
            "replies": replies,
            "reposts": reposts,
        },
    }


def _reply_record(
    tweet_id: str,
    *,
    reply_to_tweet_id: str,
    reply_to_screen_name: str,
    target_tweet_id: str | None = None,
    target_screen_name: str | None = None,
) -> TweetRecord:
    return TweetRecord(
        tweet_id=tweet_id,
        text="reply",
        author=TweetAuthor(screen_name="replier", verified=True),
        created_at=datetime(2026, 4, 27, tzinfo=UTC),
        conversation_id="conversation-1",
        reply_to_tweet_id=reply_to_tweet_id,
        reply_to_screen_name=reply_to_screen_name,
        target_tweet_id=target_tweet_id,
        target_screen_name=target_screen_name,
    )


class _FakeTwitterClient:
    def __init__(self, tweets: dict[str, TweetRecord]) -> None:
        self.tweets = tweets
        self.calls: list[str] = []

    def fetch_tweet(self, tweet_id: str) -> TweetRecord:
        self.calls.append(tweet_id)
        return self.tweets[tweet_id]


class _FakeAnalyticsClient:
    def __init__(self, *, timezone: str, posts_by_day: dict[str, list[dict[str, object]]]) -> None:
        self.timezone = ZoneInfo(timezone)
        self.posts_by_day = posts_by_day
        self.calls: list[tuple[str, str]] = []
        self.requested_days: list[str] = []

    def fetch_content_posts(
        self,
        *,
        start_time: str,
        end_time: str,
        post_limit: int,
        query_page_size: int | None = None,
    ) -> dict[str, object]:
        self.calls.append((start_time, end_time))
        start_at = datetime.fromisoformat(start_time.replace("Z", "+00:00")).astimezone(self.timezone)
        end_at = datetime.fromisoformat(end_time.replace("Z", "+00:00")).astimezone(self.timezone)
        assert start_at.time().isoformat() == "00:00:00"
        assert end_at.time().isoformat() == "23:59:59"
        assert start_at.date() == end_at.date()
        day = start_at.date().isoformat()
        self.requested_days.append(day)
        return {"posts": list(self.posts_by_day.get(day, []))}


@dataclass
class _FakeAuthorAlphaStorage:
    reply_daily_metrics: dict[tuple[str, str], dict[str, object]] | None = None
    author_daily_rollups: dict[tuple[str, str], dict[str, object]] | None = None
    authors: dict[str, dict[str, object]] | None = None
    checkpoints: dict[str, dict[str, object]] | None = None
    sync_runs: dict[str, dict[str, object]] | None = None
    zeroed_authors: list[str] | None = None

    def __post_init__(self) -> None:
        self.reply_daily_metrics = {}
        self.author_daily_rollups = {}
        self.authors = {}
        self.checkpoints = {}
        self.sync_runs = {}
        self.zeroed_authors = []

    def record_sync_run(self, **values) -> None:
        self.sync_runs[values["run_id"]] = dict(values)

    def update_sync_run(self, run_id: str, **updates) -> None:
        self.sync_runs[run_id].update(updates)

    def read_checkpoint(self, sync_scope: str) -> dict[str, object] | None:
        checkpoint = self.checkpoints.get(sync_scope)
        return dict(checkpoint) if checkpoint else None

    def write_checkpoint(self, **values) -> None:
        self.checkpoints[values["sync_scope"]] = dict(values)

    def upsert_reply_daily_metrics(self, **values) -> None:
        key = (str(values["metric_date"]), str(values["reply_tweet_id"]))
        self.reply_daily_metrics[key] = dict(values)

    def upsert_author_daily_rollup(self, **values) -> None:
        key = (str(values["metric_date"]), str(values["target_author"]))
        self.author_daily_rollups[key] = dict(values)

    def replace_day_sync_snapshot(self, *, metric_date: str, reply_metrics, author_rollups) -> None:
        self.reply_daily_metrics = {
            key: value
            for key, value in self.reply_daily_metrics.items()
            if key[0] != metric_date
        }
        self.author_daily_rollups = {
            key: value
            for key, value in self.author_daily_rollups.items()
            if key[0] != metric_date
        }
        for values in reply_metrics:
            key = (metric_date, str(values["reply_tweet_id"]))
            row = {"metric_date": metric_date, **dict(values)}
            self.reply_daily_metrics[key] = row
        for values in author_rollups:
            key = (metric_date, str(values["target_author"]))
            row = {"metric_date": metric_date, **dict(values)}
            self.author_daily_rollups[key] = row

    def list_author_daily_rollups(self, start_date: str, end_date: str) -> list[dict[str, object]]:
        return [
            dict(row)
            for (metric_date, _), row in sorted(self.author_daily_rollups.items())
            if start_date <= metric_date <= end_date
        ]

    def list_reply_daily_metrics(self, start_date: str, end_date: str) -> list[dict[str, object]]:
        return [
            dict(row)
            for (metric_date, _), row in sorted(self.reply_daily_metrics.items())
            if start_date <= metric_date <= end_date
        ]

    def upsert_author(self, **values) -> None:
        current = dict(self.authors.get(str(values["screen_name"]), {}))
        current.update(values)
        self.authors[str(values["screen_name"])] = current

    def zero_out_stale_authors(self, scored_screen_names: set[str], *, scored_at: str) -> int:
        zeroed = 0
        for screen_name, row in self.authors.items():
            if screen_name in scored_screen_names:
                continue
            row.update(
                {
                    "author_score": 0,
                    "reply_count_7d": 0,
                    "impressions_total_7d": 0,
                    "avg_impressions_7d": 0.0,
                    "max_impressions_7d": 0,
                    "last_scored_at": scored_at,
                }
            )
            self.zeroed_authors.append(screen_name)
            zeroed += 1
        return zeroed

    def count_reply_daily_metrics(self) -> int:
        return len(self.reply_daily_metrics)

    def count_author_daily_rollups(self) -> int:
        return len(self.author_daily_rollups)
