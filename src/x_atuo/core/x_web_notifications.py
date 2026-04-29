"""Cookie-authenticated X Web notifications helpers."""

from __future__ import annotations

import urllib.parse
from collections.abc import Mapping, Sequence
from typing import Any, Literal

from x_atuo.automation.config import AutomationConfig
from x_atuo.core.twitter_client import TwitterCredentials
from x_atuo.core.x_graphql import NOTIFICATIONS_TIMELINE_SPEC, build_graphql_get_url
from x_atuo.core.twitter_runtime import load_twitter_runtime
from x_atuo.core.x_timeline import (
    ensure_notification_kind,
    extract_legacy_cursor,
    extract_timeline_cursor,
    normalize_device_follow_posts,
    normalize_notification_entries,
)
from x_atuo.core.x_web_transport import XWebTransport

NotificationsTimelineType = Literal["All", "Mentions", "Priority", "Verified", "SuperFollowers"]
_DEVICE_FOLLOW_EXT = (
    "mediaStats,highlightedLabel,parodyCommentaryFanLabel,voiceInfo,"
    "birdwatchPivot,superFollowMetadata,unmentionInfo,editControl,article"
)


class XWebNotificationsError(RuntimeError):
    """Raised when notifications requests fail or return malformed payloads."""


class XWebNotificationsConfigError(XWebNotificationsError):
    """Raised when login cookies are unavailable."""


class XWebNotificationsClient:
    """Read X notifications through cookie-authenticated web GraphQL."""

    def __init__(
        self,
        *,
        credentials: TwitterCredentials,
        proxy: str | None = None,
        timeout_seconds: int = 30,
    ) -> None:
        if not credentials.ok:
            raise XWebNotificationsConfigError("twitter auth_token/ct0 are not configured")
        self.credentials = credentials
        self.proxy = proxy
        self.timeout_seconds = timeout_seconds
        self.transport = XWebTransport(
            credentials=credentials,
            proxy=proxy,
            timeout_seconds=timeout_seconds,
        )

    @classmethod
    def from_settings(
        cls,
        settings: AutomationConfig,
        *,
        base_env: Mapping[str, str] | None = None,
    ) -> "XWebNotificationsClient":
        runtime = load_twitter_runtime(
            settings.agent_reach_config_path,
            proxy=settings.twitter.proxy_url,
            base_env=base_env,
        )
        return cls(
            credentials=TwitterCredentials(auth_token=runtime.auth_token, ct0=runtime.ct0),
            proxy=runtime.proxy,
            timeout_seconds=30,
        )

    def fetch_notifications(
        self,
        *,
        timeline_type: NotificationsTimelineType,
        count: int,
        cursor: str | None = None,
    ) -> dict[str, object]:
        variables: dict[str, Any] = {"timeline_type": timeline_type, "count": count}
        if cursor:
            variables["cursor"] = cursor
        payload = self._graphql_get(variables=variables)
        timeline = dig(payload, "data", "viewer_v2", "user_results", "result", "notification_timeline", "timeline")
        if not isinstance(timeline, dict):
            raise XWebNotificationsError("x.com notifications payload is malformed")
        return {
            "timeline_type": timeline_type,
            "entries": normalize_notification_entries(timeline),
            "top_cursor": extract_timeline_cursor(timeline, "Top"),
            "bottom_cursor": extract_timeline_cursor(timeline, "Bottom"),
        }

    def fetch_device_follow_feed(
        self,
        *,
        count: int,
    ) -> dict[str, object]:
        payload = self._device_follow_get(count=count)
        timeline = payload.get("timeline")
        global_objects = payload.get("globalObjects")
        if not isinstance(timeline, dict) or not isinstance(global_objects, dict):
            raise XWebNotificationsError("x.com device-follow payload is malformed")
        users = global_objects.get("users")
        tweets = global_objects.get("tweets")
        if not isinstance(users, dict) or not isinstance(tweets, dict):
            raise XWebNotificationsError("x.com device-follow payload is malformed")
        return {
            "timeline_id": timeline.get("id"),
            "posts": normalize_device_follow_posts(timeline, users=users, tweets=tweets),
            "top_cursor": extract_legacy_cursor(timeline, "Top"),
            "bottom_cursor": extract_legacy_cursor(timeline, "Bottom"),
        }

    def _graphql_get(self, *, variables: Mapping[str, Any]) -> dict[str, Any]:
        url = build_graphql_get_url(NOTIFICATIONS_TIMELINE_SPEC, variables=variables)
        payload = self.transport.get_json(url, referer=NOTIFICATIONS_TIMELINE_SPEC.referer)
        if not isinstance(payload, dict):
            raise XWebNotificationsError(f"unexpected X Web payload type: {type(payload)!r}")
        return payload

    def _device_follow_get(self, *, count: int) -> dict[str, Any]:
        params = {
            "include_profile_interstitial_type": "1",
            "include_blocking": "1",
            "include_blocked_by": "1",
            "include_followed_by": "1",
            "include_want_retweets": "1",
            "include_mute_edge": "1",
            "include_can_dm": "1",
            "include_can_media_tag": "1",
            "include_ext_is_blue_verified": "1",
            "include_ext_verified_type": "1",
            "include_ext_profile_image_shape": "1",
            "skip_status": "1",
            "cards_platform": "Web-12",
            "include_cards": "1",
            "include_ext_alt_text": "true",
            "include_ext_limited_action_results": "true",
            "include_quote_count": "true",
            "include_reply_count": "1",
            "tweet_mode": "extended",
            "include_ext_views": "true",
            "include_entities": "true",
            "include_user_entities": "true",
            "include_ext_media_color": "true",
            "include_ext_media_availability": "true",
            "include_ext_sensitive_media_warning": "true",
            "include_ext_trusted_friends_metadata": "true",
            "send_error_codes": "true",
            "simple_quoted_tweet": "true",
            "count": str(count),
            "ext": _DEVICE_FOLLOW_EXT,
        }
        url = f"https://x.com/i/api/2/notifications/device_follow.json?{urllib.parse.urlencode(params)}"
        payload = self.transport.get_json(url, referer="https://x.com/i/timeline")
        if not isinstance(payload, dict):
            raise XWebNotificationsError(f"unexpected X Web payload type: {type(payload)!r}")
        return payload


def build_notifications_snapshot(
    client: XWebNotificationsClient,
    *,
    timeline_type: NotificationsTimelineType,
    count: int,
    cursor: str | None = None,
) -> dict[str, Any]:
    data = client.fetch_notifications(timeline_type=timeline_type, count=count, cursor=cursor)
    entries = [
        ensure_notification_kind(entry)
        for entry in list(data.get("entries") or [])
        if isinstance(entry, dict)
    ]
    return {
        "timeline_type": timeline_type,
        "count": count,
        "cursor": cursor,
        "top_cursor": data.get("top_cursor"),
        "bottom_cursor": data.get("bottom_cursor"),
        "entries": entries,
    }


def build_device_follow_feed_snapshot(
    client: XWebNotificationsClient,
    *,
    count: int,
) -> dict[str, Any]:
    data = client.fetch_device_follow_feed(count=count)
    return {
        "count": count,
        "timeline_id": data.get("timeline_id"),
        "top_cursor": data.get("top_cursor"),
        "bottom_cursor": data.get("bottom_cursor"),
        "posts": [
            dict(post)
            for post in list(data.get("posts") or [])
            if isinstance(post, dict)
        ],
    }
