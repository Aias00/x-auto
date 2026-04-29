"""Shared normalization helpers for X Web reader surfaces."""

from __future__ import annotations

from typing import Any


def dig(value: Any, *path: str) -> Any:
    current = value
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def coerce_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError:
            return None
    return None


def normalize_author_payload(
    *,
    screen_name: str | None,
    author_name: str | None,
    rest_id: str | None,
    verified: bool,
) -> dict[str, Any]:
    return {
        "screen_name": screen_name,
        "name": author_name,
        "rest_id": rest_id,
        "verified": verified,
    }


def normalize_public_metrics(
    *,
    impressions: int = 0,
    likes: int = 0,
    replies: int = 0,
    reposts: int = 0,
    quotes: int = 0,
) -> dict[str, int]:
    return {
        "impressions": impressions,
        "likes": likes,
        "replies": replies,
        "reposts": reposts,
        "quotes": quotes,
    }


def normalize_post_payload(
    *,
    post_id: str,
    text: str,
    created_at: str | None,
    screen_name: str | None,
    author_name: str | None,
    rest_id: str | None,
    verified: bool,
    impressions: int = 0,
    likes: int = 0,
    replies: int = 0,
    reposts: int = 0,
    quotes: int = 0,
    reply_to_id: str | None = None,
    community_id: str | None = None,
) -> dict[str, Any]:
    return {
        "id": post_id,
        "text": text,
        "created_at": created_at,
        "url": f"https://x.com/{screen_name}/status/{post_id}" if screen_name else None,
        "author": normalize_author_payload(
            screen_name=screen_name,
            author_name=author_name,
            rest_id=rest_id,
            verified=verified,
        ),
        "public_metrics": normalize_public_metrics(
            impressions=impressions,
            likes=likes,
            replies=replies,
            reposts=reposts,
            quotes=quotes,
        ),
        "reply_to_id": reply_to_id,
        "community_id": community_id,
    }
