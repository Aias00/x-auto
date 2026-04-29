from __future__ import annotations

from fastapi.testclient import TestClient

import x_atuo.automation.api as automation_api
from x_atuo.automation.api import app
from x_atuo.core.x_web_analytics import build_account_content_snapshot
from x_atuo.core.x_web_notifications import build_device_follow_feed_snapshot
from x_atuo.core.x_web_notifications import build_notifications_snapshot
from x_atuo.core.x_web_notifications import XWebNotificationsConfigError
from x_atuo.core.x_web_analytics import XWebClientConfigError
from x_atuo.core.x_web_analytics import build_account_analytics_snapshot


class _FakeAnalyticsClient:
    def fetch_account_overview(
        self,
        *,
        start_time: str,
        end_time: str,
        granularity: str,
    ) -> dict[str, object]:
        assert start_time.endswith("Z")
        assert end_time.endswith("Z")
        assert granularity == "Daily"
        return {
            "account": {
                "id": "user-1",
                "username": "demo",
                "name": "Demo User",
                "verified": True,
                "is_blue_verified": True,
                "profile_image_url_https": "https://example.com/avatar.jpg",
                "followers_count": 120,
                "verified_follower_count": 55,
            },
            "metrics_time_series": [
                {
                    "timestamp": "2026-04-24T00:00:00Z",
                    "metrics": {
                        "impressions": 10,
                        "engagements": 4,
                        "profile_visits": 2,
                        "follows": 1,
                        "likes": 3,
                    },
                },
                {
                    "timestamp": "2026-04-25T00:00:00Z",
                    "metrics": {
                        "impressions": 5,
                        "engagements": 1,
                        "retweets": 1,
                    },
                },
            ],
        }

    def fetch_content_posts(
        self,
        *,
        start_time: str,
        end_time: str,
        post_limit: int,
        query_page_size: int | None = None,
    ) -> dict[str, object]:
        assert start_time.endswith("Z")
        assert end_time.endswith("Z")
        assert post_limit >= 2
        assert query_page_size is None or query_page_size >= 10
        return {
            "account": {
                "id": "user-1",
                "username": "demo",
                "name": "Demo User",
                "verified": True,
                "is_blue_verified": True,
                "profile_image_url_https": "https://example.com/avatar.jpg",
                "followers_count": 120,
                "verified_follower_count": 55,
            },
            "posts": [
                {
                    "id": "tweet-1",
                    "text": "first",
                    "created_at": "2026-04-24T00:00:00Z",
                    "public_metrics": {"like_count": 3, "reply_count": 1, "retweet_count": 0},
                    "metrics_total": {"impressions": 15, "engagements": 5, "likes": 4},
                    "reply_to_id": "root-1",
                    "community_id": None,
                },
                {
                    "id": "tweet-2",
                    "text": "second",
                    "created_at": "2026-04-23T00:00:00Z",
                    "public_metrics": {"like_count": 2, "reply_count": 0, "retweet_count": 1},
                    "metrics_total": {"impressions": 8, "engagements": 2, "likes": 2, "retweets": 1},
                    "reply_to_id": None,
                    "community_id": None,
                },
                {
                    "id": "tweet-3",
                    "text": "third",
                    "created_at": "2026-04-25T06:00:00Z",
                    "public_metrics": {"like_count": 9, "reply_count": 4, "retweet_count": 2},
                    "metrics_total": {"impressions": 99, "engagements": 12, "likes": 9, "retweets": 2},
                    "reply_to_id": "root-2",
                    "community_id": None,
                },
            ],
        }


class _FakeNotificationsClient:
    def fetch_notifications(
        self,
        *,
        timeline_type: str,
        count: int,
        cursor: str | None = None,
    ) -> dict[str, object]:
        assert timeline_type in {"All", "Mentions"}
        assert count == 3
        assert cursor is None
        return {
            "timeline_type": timeline_type,
            "entries": [
                {
                    "entry_id": "entry-1",
                    "sort_index": "3",
                    "notification_id": "notif-1",
                    "icon": "bell_icon",
                    "text": "Turned on post notifications for Alice",
                    "url": "/2/notifications/device_follow.json",
                    "template_type": "TimelineNotificationAggregateUserActions",
                    "actors": [{"screen_name": "alice", "name": "Alice"}],
                },
                {
                    "entry_id": "entry-2",
                    "sort_index": "2",
                    "notification_id": "notif-2",
                    "icon": "heart_icon",
                    "text": "Bob liked your post",
                    "url": "https://twitter.com/bob",
                    "template_type": "TimelineNotificationRecap",
                    "actors": [{"screen_name": "bob", "name": "Bob"}],
                },
                {
                    "entry_id": "entry-3",
                    "sort_index": "1",
                    "notification_id": "notif-3",
                    "icon": "mention_icon",
                    "text": "Carol mentioned you",
                    "url": "https://twitter.com/carol",
                    "template_type": "TimelineNotificationAggregateUserActions",
                    "actors": [{"screen_name": "carol", "name": "Carol"}],
                },
            ],
            "top_cursor": "top-1",
            "bottom_cursor": "bottom-1",
        }

    def fetch_device_follow_feed(
        self,
        *,
        count: int,
    ) -> dict[str, object]:
        assert count == 2
        return {
            "timeline_id": "tweet_notifications",
            "top_cursor": "top-x",
            "bottom_cursor": "bottom-x",
            "posts": [
                {
                    "id": "tweet-1",
                    "text": "first notification post",
                    "created_at": "2026-04-28T14:00:00Z",
                    "url": "https://x.com/alice/status/tweet-1",
                    "author": {
                        "screen_name": "alice",
                        "name": "Alice",
                        "rest_id": "100",
                        "verified": True,
                    },
                    "public_metrics": {
                        "likes": 3,
                        "replies": 1,
                        "reposts": 0,
                    },
                    "reply_to_id": None,
                },
                {
                    "id": "tweet-2",
                    "text": "second notification post",
                    "created_at": "2026-04-28T13:55:00Z",
                    "url": "https://x.com/bob/status/tweet-2",
                    "author": {
                        "screen_name": "bob",
                        "name": "Bob",
                        "rest_id": "101",
                        "verified": False,
                    },
                    "public_metrics": {
                        "likes": 2,
                        "replies": 0,
                        "reposts": 1,
                    },
                    "reply_to_id": None,
                },
            ],
        }


def test_build_account_analytics_snapshot_aggregates_post_metrics() -> None:
    snapshot = build_account_analytics_snapshot(
        _FakeAnalyticsClient(),
        days=7,
        post_limit=2,
        granularity="daily",
    )

    assert snapshot["account"]["username"] == "demo"
    assert snapshot["summary"] == {
        "post_count": 2,
        "impressions": 15,
        "engagements": 5,
        "profile_visits": 2,
        "follows": 1,
        "likes": 3,
        "retweets": 1,
    }
    assert snapshot["posts"][0]["analytics"]["metrics_total"] == {
        "impressions": 15,
        "engagements": 5,
        "likes": 4,
    }
    assert snapshot["posts"][0]["analytics"]["timestamped_metrics"] == []
    assert snapshot["posts"][1]["analytics"]["metrics_total"] == {
        "impressions": 8,
        "engagements": 2,
        "likes": 2,
        "retweets": 1,
    }


def test_build_account_content_snapshot_filters_replies_and_sorts_by_impressions_desc() -> None:
    snapshot = build_account_content_snapshot(
        _FakeAnalyticsClient(),
        from_date="2026-04-25",
        to_date="2026-04-25",
        content_type="replies",
        sort_field="impressions",
        sort_direction="desc",
        limit=10,
    )

    assert snapshot["filters"] == {
        "type": "replies",
        "sort": "impressions",
        "dir": "desc",
        "from": "2026-04-25",
        "to": "2026-04-25",
        "limit": 10,
    }
    assert snapshot["total_matches"] == 2
    assert snapshot["returned_count"] == 2
    assert [post["id"] for post in snapshot["posts"]] == ["tweet-3", "tweet-1"]
    assert snapshot["posts"][0]["public_metrics"]["impressions"] == 99
    assert snapshot["posts"][1]["reply_to_id"] == "root-1"


def test_account_analytics_route_returns_snapshot(monkeypatch) -> None:
    calls: dict[str, object] = {}

    def fake_get_account_analytics_snapshot(*, request, days: int, post_limit: int, granularity: str):
        calls["days"] = days
        calls["post_limit"] = post_limit
        calls["granularity"] = granularity
        return {
            "account": {"id": "user-1", "username": "demo", "public_metrics": {"followers_count": 120}},
            "window": {
                "days": days,
                "post_limit": post_limit,
                "granularity": granularity,
                "start_time": "2026-04-18T00:00:00Z",
                "end_time": "2026-04-25T00:00:00Z",
            },
            "summary": {"post_count": 1, "impressions": 10},
            "posts": [],
        }

    monkeypatch.setattr(automation_api, "_get_account_analytics_snapshot", fake_get_account_analytics_snapshot)

    with TestClient(app) as client:
        response = client.get("/analytics/account", params={"days": 14, "post_limit": 25, "granularity": "daily"})

    assert response.status_code == 200
    assert calls == {"days": 14, "post_limit": 25, "granularity": "daily"}
    assert response.json()["summary"] == {"post_count": 1, "impressions": 10}


def test_account_analytics_route_maps_config_error_to_503(monkeypatch) -> None:
    def fake_get_account_analytics_snapshot(*, request, days: int, post_limit: int, granularity: str):
        raise XWebClientConfigError("twitter auth_token/ct0 are not configured")

    monkeypatch.setattr(automation_api, "_get_account_analytics_snapshot", fake_get_account_analytics_snapshot)

    with TestClient(app) as client:
        response = client.get("/analytics/account")

    assert response.status_code == 503
    assert response.json() == {"detail": "twitter auth_token/ct0 are not configured"}


def test_account_content_route_returns_filtered_snapshot(monkeypatch) -> None:
    calls: dict[str, object] = {}

    def fake_get_account_content_snapshot(*, request, from_date: str | None, to_date: str | None, content_type: str, sort_field: str, sort_direction: str, limit: int):
        calls["from_date"] = from_date
        calls["to_date"] = to_date
        calls["content_type"] = content_type
        calls["sort_field"] = sort_field
        calls["sort_direction"] = sort_direction
        calls["limit"] = limit
        return {
            "account": {"id": "user-1", "username": "demo"},
            "filters": {
                "type": content_type,
                "sort": sort_field,
                "dir": sort_direction,
                "from": from_date,
                "to": to_date,
                "limit": limit,
            },
            "total_matches": 1,
            "returned_count": 1,
            "posts": [{"id": "tweet-9", "public_metrics": {"impressions": 42}}],
        }

    monkeypatch.setattr(automation_api, "_get_account_content_snapshot", fake_get_account_content_snapshot)

    with TestClient(app) as client:
        response = client.get(
            "/analytics/account/content",
            params={"type": "replies", "sort": "impressions", "dir": "desc", "from": "2026-04-25", "to": "2026-04-25", "limit": 10},
        )

    assert response.status_code == 200
    assert calls == {
        "from_date": "2026-04-25",
        "to_date": "2026-04-25",
        "content_type": "replies",
        "sort_field": "impressions",
        "sort_direction": "desc",
        "limit": 10,
    }
    assert response.json()["posts"][0]["id"] == "tweet-9"


def test_build_notifications_snapshot_returns_entries_and_cursors() -> None:
    snapshot = build_notifications_snapshot(
        _FakeNotificationsClient(),
        timeline_type="All",
        count=3,
    )

    assert snapshot["timeline_type"] == "All"
    assert snapshot["count"] == 3
    assert snapshot["top_cursor"] == "top-1"
    assert snapshot["bottom_cursor"] == "bottom-1"
    assert [entry["notification_id"] for entry in snapshot["entries"]] == ["notif-1", "notif-2", "notif-3"]
    assert snapshot["entries"][0]["actors"][0]["screen_name"] == "alice"
    assert [entry["kind"] for entry in snapshot["entries"]] == [
        "new_post_notification",
        "like",
        "mention",
    ]


def test_build_device_follow_feed_snapshot_returns_posts_and_cursors() -> None:
    snapshot = build_device_follow_feed_snapshot(_FakeNotificationsClient(), count=2)

    assert snapshot["count"] == 2
    assert snapshot["timeline_id"] == "tweet_notifications"
    assert snapshot["top_cursor"] == "top-x"
    assert snapshot["bottom_cursor"] == "bottom-x"
    assert [post["id"] for post in snapshot["posts"]] == ["tweet-1", "tweet-2"]
    assert snapshot["posts"][0]["author"]["screen_name"] == "alice"


def test_notifications_route_returns_snapshot(monkeypatch) -> None:
    calls: dict[str, object] = {}

    def fake_get_notifications_snapshot(*, request, timeline_type: str, count: int, cursor: str | None):
        calls["timeline_type"] = timeline_type
        calls["count"] = count
        calls["cursor"] = cursor
        return {
            "timeline_type": timeline_type,
            "count": count,
            "top_cursor": "top-x",
            "bottom_cursor": "bottom-x",
            "entries": [{"notification_id": "notif-x", "text": "Hello", "kind": "other"}],
        }

    monkeypatch.setattr(automation_api, "_get_notifications_snapshot", fake_get_notifications_snapshot)

    with TestClient(app) as client:
        response = client.get("/notifications", params={"timeline_type": "Mentions", "count": 20})

    assert response.status_code == 200
    assert calls == {"timeline_type": "Mentions", "count": 20, "cursor": None}
    assert response.json()["entries"][0]["notification_id"] == "notif-x"
    assert response.json()["entries"][0]["kind"] == "other"


def test_notifications_route_maps_config_error_to_503(monkeypatch) -> None:
    def fake_get_notifications_snapshot(*, request, timeline_type: str, count: int, cursor: str | None):
        raise XWebNotificationsConfigError("twitter auth_token/ct0 are not configured")

    monkeypatch.setattr(automation_api, "_get_notifications_snapshot", fake_get_notifications_snapshot)

    with TestClient(app) as client:
        response = client.get("/notifications")

    assert response.status_code == 503
    assert response.json() == {"detail": "twitter auth_token/ct0 are not configured"}


def test_device_follow_feed_route_returns_snapshot(monkeypatch) -> None:
    calls: dict[str, object] = {}

    def fake_get_device_follow_feed_snapshot(*, request, count: int):
        calls["count"] = count
        return {
            "count": count,
            "timeline_id": "tweet_notifications",
            "top_cursor": "top-x",
            "bottom_cursor": "bottom-x",
            "posts": [{"id": "tweet-x", "text": "Hello", "author": {"screen_name": "alice"}}],
        }

    monkeypatch.setattr(automation_api, "_get_device_follow_feed_snapshot", fake_get_device_follow_feed_snapshot)

    with TestClient(app) as client:
        response = client.get("/notifications/device-follow-feed", params={"count": 20})

    assert response.status_code == 200
    assert calls == {"count": 20}
    assert response.json()["posts"][0]["id"] == "tweet-x"


def test_device_follow_feed_route_maps_config_error_to_503(monkeypatch) -> None:
    def fake_get_device_follow_feed_snapshot(*, request, count: int):
        raise XWebNotificationsConfigError("twitter auth_token/ct0 are not configured")

    monkeypatch.setattr(automation_api, "_get_device_follow_feed_snapshot", fake_get_device_follow_feed_snapshot)

    with TestClient(app) as client:
        response = client.get("/notifications/device-follow-feed")

    assert response.status_code == 503
    assert response.json() == {"detail": "twitter auth_token/ct0 are not configured"}
