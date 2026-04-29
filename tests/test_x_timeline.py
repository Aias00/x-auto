from __future__ import annotations


def test_extract_timeline_cursor_reads_graphql_cursor_entry() -> None:
    from x_atuo.core.x_timeline import extract_timeline_cursor

    timeline = {
        "instructions": [
            {
                "entries": [
                    {"content": {"entryType": "TimelineTimelineCursor", "cursorType": "Top", "value": "top-1"}},
                    {"content": {"entryType": "TimelineTimelineCursor", "cursorType": "Bottom", "value": "bottom-1"}},
                ]
            }
        ]
    }

    assert extract_timeline_cursor(timeline, "Top") == "top-1"
    assert extract_timeline_cursor(timeline, "Bottom") == "bottom-1"


def test_extract_legacy_cursor_reads_add_entries_cursor() -> None:
    from x_atuo.core.x_timeline import extract_legacy_cursor

    timeline = {
        "instructions": [
            {
                "addEntries": {
                    "entries": [
                        {"content": {"operation": {"cursor": {"cursorType": "Bottom", "value": "legacy-bottom"}}}}
                    ]
                }
            }
        ]
    }

    assert extract_legacy_cursor(timeline, "Bottom") == "legacy-bottom"


def test_normalize_notification_entries_builds_notification_rows() -> None:
    from x_atuo.core.x_timeline import normalize_notification_entries

    timeline = {
        "instructions": [
            {
                "entries": [
                    {
                        "entryId": "entry-1",
                        "sortIndex": "1",
                        "content": {
                            "entryType": "TimelineTimelineItem",
                            "itemContent": {
                                "itemType": "TimelineNotification",
                                "id": "notif-1",
                                "notification_icon": "bell_icon",
                                "rich_message": {"text": "Turned on post notifications"},
                                "notification_url": {"url": "/2/notifications/device_follow.json"},
                                "template": {
                                    "__typename": "TimelineNotificationAggregateUserActions",
                                    "from_users": [
                                        {
                                            "user_results": {
                                                "result": {
                                                    "rest_id": "rest-1",
                                                    "core": {"screen_name": "alice", "name": "Alice"},
                                                }
                                            }
                                        }
                                    ],
                                },
                            },
                        },
                    }
                ]
            }
        ]
    }

    rows = normalize_notification_entries(timeline)

    assert rows == [
        {
            "entry_id": "entry-1",
            "sort_index": "1",
            "notification_id": "notif-1",
            "icon": "bell_icon",
            "text": "Turned on post notifications",
            "url": "/2/notifications/device_follow.json",
            "template_type": "TimelineNotificationAggregateUserActions",
            "actors": [{"screen_name": "alice", "name": "Alice", "rest_id": "rest-1"}],
            "kind": "new_post_notification",
        }
    ]


def test_normalize_device_follow_posts_builds_shared_post_shape() -> None:
    from x_atuo.core.x_timeline import normalize_device_follow_posts

    timeline = {
        "instructions": [
            {
                "addEntries": {
                    "entries": [
                        {
                            "content": {
                                "item": {
                                    "content": {
                                        "tweet": {
                                            "id": "tweet-1",
                                        }
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        ]
    }
    users = {
        "user-1": {
            "id_str": "user-1",
            "screen_name": "alice",
            "name": "Alice",
            "verified": True,
        }
    }
    tweets = {
        "tweet-1": {
            "id": "tweet-1",
            "user_id_str": "user-1",
            "full_text": "hello",
            "created_at": "2026-04-29T00:00:00Z",
            "favorite_count": 2,
            "reply_count": 3,
            "retweet_count": 4,
            "quote_count": 5,
            "in_reply_to_status_id_str": "parent-1",
        }
    }

    posts = normalize_device_follow_posts(timeline, users=users, tweets=tweets)

    assert posts == [
        {
            "id": "tweet-1",
            "text": "hello",
            "created_at": "2026-04-29T00:00:00Z",
            "url": "https://x.com/alice/status/tweet-1",
            "author": {
                "screen_name": "alice",
                "name": "Alice",
                "rest_id": "user-1",
                "verified": True,
            },
            "public_metrics": {
                "impressions": 0,
                "likes": 2,
                "replies": 3,
                "reposts": 4,
                "quotes": 5,
            },
            "reply_to_id": "parent-1",
            "community_id": None,
        }
    ]
