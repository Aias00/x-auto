"""Shared timeline parsing helpers for X Web readers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from x_atuo.core.x_web_normalization import dig, normalize_post_payload


def extract_timeline_cursor(timeline: Mapping[str, Any], cursor_type: str) -> str | None:
    for instruction in timeline.get("instructions") or []:
        if not isinstance(instruction, dict):
            continue
        for entry in instruction.get("entries", []) or []:
            if not isinstance(entry, dict):
                continue
            content = entry.get("content")
            if not isinstance(content, dict):
                continue
            if content.get("entryType") == "TimelineTimelineCursor" and content.get("cursorType") == cursor_type:
                value = content.get("value")
                return str(value) if value else None
    return None


def extract_legacy_cursor(timeline: Mapping[str, Any], cursor_type: str) -> str | None:
    for instruction in timeline.get("instructions") or []:
        if not isinstance(instruction, dict):
            continue
        entries = instruction.get("entries")
        if not isinstance(entries, list):
            add_entries = instruction.get("addEntries")
            entries = add_entries.get("entries") if isinstance(add_entries, dict) else None
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            operation = dig(entry, "content", "operation")
            cursor = operation.get("cursor") if isinstance(operation, dict) else None
            if not isinstance(cursor, dict):
                continue
            if cursor.get("cursorType") == cursor_type:
                value = cursor.get("value")
                return str(value) if value else None
    return None


def normalize_notification_entries(timeline: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for instruction in timeline.get("instructions") or []:
        if not isinstance(instruction, dict):
            continue
        for entry in instruction.get("entries", []) or []:
            if not isinstance(entry, dict):
                continue
            content = entry.get("content")
            if not isinstance(content, dict) or content.get("entryType") != "TimelineTimelineItem":
                continue
            item = content.get("itemContent")
            if not isinstance(item, dict) or item.get("itemType") != "TimelineNotification":
                continue
            rows.append(
                {
                    "entry_id": entry.get("entryId"),
                    "sort_index": entry.get("sortIndex"),
                    "notification_id": item.get("id"),
                    "icon": item.get("notification_icon"),
                    "text": dig(item, "rich_message", "text"),
                    "url": dig(item, "notification_url", "url"),
                    "template_type": dig(item, "template", "__typename"),
                    "actors": _extract_actors(dig(item, "template", "from_users")),
                    "kind": classify_notification(item),
                }
            )
    return rows


def normalize_device_follow_posts(
    timeline: Mapping[str, Any],
    *,
    users: Mapping[str, Any],
    tweets: Mapping[str, Any],
) -> list[dict[str, Any]]:
    posts: list[dict[str, Any]] = []
    seen: set[str] = set()
    for instruction in timeline.get("instructions") or []:
        if not isinstance(instruction, dict):
            continue
        entries = instruction.get("entries")
        if not isinstance(entries, list):
            add_entries = instruction.get("addEntries")
            entries = add_entries.get("entries") if isinstance(add_entries, dict) else None
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            tweet_id = dig(entry, "content", "item", "content", "tweet", "id")
            if not isinstance(tweet_id, str) or not tweet_id or tweet_id in seen:
                continue
            tweet = tweets.get(tweet_id)
            if not isinstance(tweet, dict):
                continue
            user_id = str(tweet.get("user_id_str") or tweet.get("user_id") or "")
            user = users.get(user_id) if user_id else None
            posts.append(
                normalize_post_payload(
                    post_id=tweet_id,
                    text=str(tweet.get("full_text") or tweet.get("text") or ""),
                    created_at=str(tweet.get("created_at") or "") or None,
                    screen_name=user.get("screen_name") if isinstance(user, dict) else None,
                    author_name=user.get("name") if isinstance(user, dict) else None,
                    rest_id=user.get("id_str") if isinstance(user, dict) else user_id or None,
                    verified=bool(user.get("verified")) if isinstance(user, dict) else False,
                    likes=int(tweet.get("favorite_count") or 0),
                    replies=int(tweet.get("reply_count") or 0),
                    reposts=int(tweet.get("retweet_count") or 0),
                    quotes=int(tweet.get("quote_count") or 0),
                    reply_to_id=str(tweet.get("in_reply_to_status_id_str") or "") or None,
                )
            )
            seen.add(tweet_id)
    return posts


def classify_notification(item: Mapping[str, Any]) -> str:
    icon = str(item.get("notification_icon") or "")
    url = str(dig(item, "notification_url", "url") or "")
    if icon == "bell_icon" and "device_follow" in url:
        return "new_post_notification"
    if icon == "heart_icon":
        return "like"
    if icon == "person_icon":
        return "follow"
    if icon == "mention_icon":
        return "mention"
    return "other"


def ensure_notification_kind(entry: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(entry)
    if normalized.get("kind"):
        return normalized
    item_like = {
        "notification_icon": normalized.get("icon"),
        "notification_url": {"url": normalized.get("url")},
    }
    normalized["kind"] = classify_notification(item_like)
    return normalized


def _extract_actors(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    actors: list[dict[str, Any]] = []
    for item in value:
        user = dig(item, "user_results", "result")
        if not isinstance(user, dict):
            continue
        core = user.get("core") if isinstance(user.get("core"), dict) else {}
        actors.append(
            {
                "screen_name": core.get("screen_name"),
                "name": core.get("name"),
                "rest_id": user.get("rest_id"),
            }
        )
    return actors
