"""Cookie-authenticated X Web analytics helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, Literal

from x_atuo.automation.config import AutomationConfig
from x_atuo.core.twitter_client import TwitterCredentials
from x_atuo.core.twitter_runtime import load_twitter_runtime
from x_atuo.core.x_graphql import ACCOUNT_OVERVIEW_SPEC, CONTENT_POST_LIST_SPEC, build_graphql_get_url
from x_atuo.core.x_web_normalization import coerce_int, dig, normalize_post_payload
from x_atuo.core.x_web_transport import XWebTransport

AnalyticsGranularity = Literal["hourly", "daily", "weekly", "total"]
ContentType = Literal["all", "posts", "replies", "community"]
ContentSortField = Literal["date", "impressions", "likes", "replies", "reposts"]
SortDirection = Literal["asc", "desc"]

_USER_OVERVIEW_REQUESTED_METRICS = (
    "Impressions",
    "Engagements",
    "ProfileVisits",
    "Follows",
    "Replies",
    "Likes",
    "Retweets",
    "MediaViews",
    "LinkClicks",
)
_POST_LIST_REQUESTED_METRICS = (
    "Impressions",
    "Engagements",
    "ProfileVisits",
    "Replies",
    "Likes",
    "Retweets",
    "MediaViews",
    "LinkClicks",
    "Bookmark",
    "Share",
)
_METRIC_NAME_MAP = {
    "Impressions": "impressions",
    "Engagements": "engagements",
    "ProfileVisits": "profile_visits",
    "Follows": "follows",
    "Replies": "replies",
    "Likes": "likes",
    "Retweets": "retweets",
    "MediaViews": "media_views",
    "LinkClicks": "link_clicks",
    "Bookmark": "bookmarks",
    "Share": "shares",
}
_GRANULARITY_MAP = {
    "hourly": "Hourly",
    "daily": "Daily",
    "weekly": "Weekly",
    "total": "Total",
}


class XWebClientError(RuntimeError):
    """Raised when X Web analytics requests fail or return malformed payloads."""


class XWebClientConfigError(XWebClientError):
    """Raised when login cookies are unavailable."""


class XWebAnalyticsClient:
    """Read account analytics through X Web GraphQL endpoints using login cookies."""

    def __init__(
        self,
        *,
        credentials: TwitterCredentials,
        proxy: str | None = None,
        timeout_seconds: int = 30,
    ) -> None:
        if not credentials.ok:
            raise XWebClientConfigError("twitter auth_token/ct0 are not configured")
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
    ) -> "XWebAnalyticsClient":
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

    def fetch_account_overview(
        self,
        *,
        start_time: str,
        end_time: str,
        granularity: str,
    ) -> dict[str, object]:
        payload = self._graphql_get(
            spec=ACCOUNT_OVERVIEW_SPEC,
            variables={
                "from_time": start_time,
                "to_time": end_time,
                "granularity": granularity,
                "requested_metrics": list(_USER_OVERVIEW_REQUESTED_METRICS),
                "show_verified_followers": True,
            },
        )
        result = dig(payload, "data", "viewer_v2", "user_results", "result")
        if not isinstance(result, dict) or result.get("__typename") != "User":
            raise XWebClientError("x.com account overview payload is malformed")
        return {
            "account": {
                "global_id": str(result.get("id") or ""),
                "followers_count": coerce_int(dig(result, "relationship_counts", "followers")),
                "verified_follower_count": coerce_int(result.get("verified_follower_count")),
            },
            "metrics_time_series": _normalize_time_series(result.get("organic_metrics_time_series")),
        }

    def fetch_content_posts(
        self,
        *,
        start_time: str,
        end_time: str,
        post_limit: int,
        query_page_size: int | None = None,
    ) -> dict[str, object]:
        payload = self._graphql_get(
            spec=CONTENT_POST_LIST_SPEC,
            variables={
                "from_time": start_time,
                "to_time": end_time,
                "max_results": post_limit,
                "query_page_size": query_page_size if query_page_size is not None else max(post_limit, 10),
                "requested_metrics": list(_POST_LIST_REQUESTED_METRICS),
            },
        )
        result = dig(payload, "data", "viewer_v2", "user_results", "result")
        if not isinstance(result, dict) or result.get("__typename") != "User":
            raise XWebClientError("x.com content posts payload is malformed")

        legacy = result.get("legacy") if isinstance(result.get("legacy"), dict) else {}
        account = {
            "id": str(result.get("rest_id") or result.get("id") or ""),
            "rest_id": str(result.get("rest_id") or result.get("id") or ""),
            "username": str(legacy.get("screen_name") or "") or None,
            "name": str(legacy.get("name") or "") or None,
            "verified": bool(legacy.get("verified_type")),
            "is_blue_verified": bool(result.get("is_blue_verified")),
            "profile_image_url_https": str(legacy.get("profile_image_url_https") or "") or None,
        }

        posts: list[dict[str, object]] = []
        seen_ids: set[str] = set()
        for item in result.get("tweets_results") or []:
            if not isinstance(item, dict):
                continue
            tweet = item.get("result")
            if not isinstance(tweet, dict) or tweet.get("__typename") != "Tweet":
                continue
            post_id = str(tweet.get("rest_id") or tweet.get("id") or "")
            if not post_id or post_id in seen_ids:
                continue
            seen_ids.add(post_id)

            posts.append(_normalize_content_post(tweet))
            if len(posts) >= post_limit:
                break
        return {"account": account, "posts": posts}

    def _graphql_get(
        self,
        *,
        spec,
        variables: Mapping[str, Any],
    ) -> dict[str, Any]:
        url = build_graphql_get_url(spec, variables=variables)
        payload = self.transport.get_json(url, referer=spec.referer)
        if not isinstance(payload, dict):
            raise XWebClientError(f"unexpected X Web payload type: {type(payload)!r}")
        errors = payload.get("errors")
        if isinstance(errors, list) and errors:
            raise XWebClientError(_format_errors(errors))
        return payload


def build_account_analytics_snapshot(
    client: XWebAnalyticsClient,
    *,
    days: int,
    post_limit: int,
    granularity: AnalyticsGranularity,
    now: datetime | None = None,
) -> dict[str, Any]:
    end_at = _coerce_utc(now or datetime.now(UTC)).replace(microsecond=0)
    start_at = (end_at - timedelta(days=days)).replace(microsecond=0)
    start_time = _to_rfc3339(start_at)
    end_time = _to_rfc3339(end_at)
    x_granularity = _to_x_granularity(granularity)

    overview = client.fetch_account_overview(
        start_time=start_time,
        end_time=end_time,
        granularity=x_granularity,
    )
    content = client.fetch_content_posts(
        start_time=start_time,
        end_time=end_time,
        post_limit=post_limit,
    )

    account = {}
    if isinstance(content.get("account"), dict):
        account.update(content["account"])
    if isinstance(overview.get("account"), dict):
        for key, value in overview["account"].items():
            if value is None:
                continue
            if key == "global_id":
                account[key] = value
                continue
            if key in {"id", "rest_id"} and account.get(key):
                continue
            account[key] = value

    points = overview.get("metrics_time_series")
    metrics_time_series = points if isinstance(points, list) else []
    raw_posts = content.get("posts") or []
    content_posts = raw_posts[:post_limit] if isinstance(raw_posts, list) else []
    summary = {"post_count": len(content_posts)}
    _merge_metric_totals(summary, _sum_time_series(metrics_time_series))

    posts: list[dict[str, Any]] = []
    for item in content_posts:
        if not isinstance(item, dict):
            continue
        posts.append(
            {
                "id": str(item.get("id") or ""),
                "text": str(item.get("text") or ""),
                "created_at": item.get("created_at"),
                "public_metrics": dict(item.get("public_metrics") or {}),
                "reply_to_id": item.get("reply_to_id"),
                "community_id": item.get("community_id"),
                "analytics": {
                    "granularity": granularity,
                    "metrics_total": dict(item.get("metrics_total") or {}),
                    "timestamped_metrics": [],
                },
            }
        )

    return {
        "account": account,
        "window": {
            "days": days,
            "post_limit": post_limit,
            "granularity": granularity,
            "start_time": start_time,
            "end_time": end_time,
        },
        "summary": summary,
        "posts": posts,
    }


def build_account_content_snapshot(
    client: XWebAnalyticsClient,
    *,
    from_date: str | None,
    to_date: str | None,
    content_type: ContentType,
    sort_field: ContentSortField,
    sort_direction: SortDirection,
    limit: int,
) -> dict[str, Any]:
    start_day, end_day = _resolve_date_window(from_date, to_date)
    start_time, end_time = _date_window_to_rfc3339(start_day, end_day)
    content = client.fetch_content_posts(
        start_time=start_time,
        end_time=end_time,
        post_limit=1000,
        query_page_size=100,
    )
    raw_posts = content.get("posts") if isinstance(content.get("posts"), list) else []
    filtered_posts = [post for post in raw_posts if isinstance(post, dict) and _matches_content_type(post, content_type)]
    ordered_posts = sorted(
        filtered_posts,
        key=lambda post: _content_sort_key(post, sort_field),
        reverse=(sort_direction == "desc"),
    )
    sliced_posts = [_normalize_content_page_post(post) for post in ordered_posts[:limit]]

    return {
        "account": dict(content.get("account") or {}),
        "filters": {
            "type": content_type,
            "sort": sort_field,
            "dir": sort_direction,
            "from": start_day.isoformat(),
            "to": end_day.isoformat(),
            "limit": limit,
        },
        "total_matches": len(filtered_posts),
        "returned_count": len(sliced_posts),
        "posts": sliced_posts,
    }


def _to_x_granularity(value: AnalyticsGranularity) -> str:
    return _GRANULARITY_MAP[value]


def _resolve_date_window(from_date: str | None, to_date: str | None) -> tuple[date, date]:
    if from_date is None and to_date is None:
        today = datetime.now().astimezone().date()
        return today - timedelta(days=27), today
    if from_date is None or to_date is None:
        raise XWebClientError("both from and to must be provided together")
    start_day = date.fromisoformat(from_date)
    end_day = date.fromisoformat(to_date)
    if end_day < start_day:
        raise XWebClientError("to must be on or after from")
    return start_day, end_day


def _date_window_to_rfc3339(start_day: date, end_day: date) -> tuple[str, str]:
    local_tz = datetime.now().astimezone().tzinfo or UTC
    start_at = datetime.combine(start_day, time.min, tzinfo=local_tz)
    end_at = datetime.combine(end_day, time.max.replace(microsecond=0), tzinfo=local_tz)
    return _to_rfc3339(start_at), _to_rfc3339(end_at)


def _coerce_utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _to_rfc3339(value: datetime) -> str:
    return _coerce_utc(value).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_time_series(value: Any) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    points: list[dict[str, object]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        timestamp = dig(item, "timestamp", "iso8601_time")
        metrics = _normalize_metric_totals(item.get("metric_values"))
        points.append({"timestamp": str(timestamp or ""), "metrics": metrics})
    return points


def _normalize_metric_totals(value: Any) -> dict[str, int]:
    if not isinstance(value, list):
        return {}
    metrics: dict[str, int] = {}
    for item in value:
        if not isinstance(item, dict):
            continue
        raw_name = str(item.get("metric_type") or "")
        name = _METRIC_NAME_MAP.get(raw_name)
        if not name:
            continue
        metric_value = coerce_int(item.get("metric_value"))
        if metric_value is None:
            continue
        metrics[name] = metric_value
    return metrics


def _normalize_content_post(tweet: Mapping[str, Any]) -> dict[str, object]:
    legacy_tweet = tweet.get("legacy") if isinstance(tweet.get("legacy"), dict) else {}
    metrics_total = _normalize_metric_totals(tweet.get("organic_metrics_total"))
    reply_to_id = _extract_nested_rest_id(tweet.get("reply_to_results"))
    community_id = _extract_nested_rest_id(tweet.get("community_results"))
    normalized = normalize_post_payload(
        post_id=str(tweet.get("rest_id") or tweet.get("id") or ""),
        text=str(legacy_tweet.get("full_text") or ""),
        created_at=str(legacy_tweet.get("created_at") or "") or None,
        screen_name=None,
        author_name=None,
        rest_id=None,
        verified=False,
        impressions=metrics_total.get("impressions", 0),
        likes=coerce_int(legacy_tweet.get("favorite_count")) or 0,
        replies=coerce_int(legacy_tweet.get("reply_count")) or 0,
        reposts=coerce_int(legacy_tweet.get("retweet_count")) or 0,
        reply_to_id=reply_to_id,
        community_id=community_id,
    )
    normalized["metrics_total"] = metrics_total
    return normalized


def _extract_nested_rest_id(value: Any) -> str | None:
    if not isinstance(value, dict):
        return None
    direct_id = str(value.get("rest_id") or value.get("id") or "")
    if direct_id:
        return direct_id
    result = value.get("result")
    if not isinstance(result, dict):
        return None
    return str(result.get("rest_id") or result.get("id") or "") or None


def _matches_content_type(post: Mapping[str, Any], content_type: ContentType) -> bool:
    reply_to_id = str(post.get("reply_to_id") or "")
    community_id = str(post.get("community_id") or "")
    if content_type == "posts":
        return not reply_to_id and not community_id
    if content_type == "replies":
        return bool(reply_to_id)
    if content_type == "community":
        return bool(community_id)
    return True


def _content_sort_key(post: Mapping[str, Any], sort_field: ContentSortField) -> Any:
    if sort_field == "impressions":
        return _content_metric_value(post, "impressions")
    if sort_field == "likes":
        return _content_metric_value(post, "likes")
    if sort_field == "replies":
        return _content_metric_value(post, "replies")
    if sort_field == "reposts":
        return _content_metric_value(post, "reposts")
    created_at = post.get("created_at")
    if isinstance(created_at, str) and created_at:
        try:
            return datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y").timestamp()
        except ValueError:
            return created_at
    return 0


def _content_metric_value(post: Mapping[str, Any], key: str) -> int:
    public_metrics = post.get("public_metrics") if isinstance(post.get("public_metrics"), dict) else {}
    if isinstance(public_metrics.get(key), int):
        return int(public_metrics[key])
    metrics_total = post.get("metrics_total") if isinstance(post.get("metrics_total"), dict) else {}
    if isinstance(metrics_total.get(key), int):
        return int(metrics_total[key])
    return 0


def _normalize_content_page_post(post: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(post)
    public_metrics = dict(post.get("public_metrics") or {})
    public_metrics.setdefault("impressions", _content_metric_value(post, "impressions"))
    public_metrics.setdefault("likes", _content_metric_value(post, "likes"))
    public_metrics.setdefault("replies", _content_metric_value(post, "replies"))
    public_metrics.setdefault("reposts", _content_metric_value(post, "reposts"))
    normalized["public_metrics"] = public_metrics
    return normalized


def _sum_time_series(points: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for point in points:
        metrics = point.get("metrics")
        if not isinstance(metrics, dict):
            continue
        _merge_metric_totals(totals, {str(key): int(value) for key, value in metrics.items() if isinstance(value, int)})
    return totals


def _merge_metric_totals(target: dict[str, int], update: Mapping[str, int]) -> None:
    for key, value in update.items():
        target[key] = target.get(key, 0) + int(value)


def _format_errors(errors: Sequence[Any]) -> str:
    messages: list[str] = []
    for item in errors:
        if not isinstance(item, dict):
            continue
        message = str(item.get("message") or item.get("detail") or "").strip()
        code = str(item.get("code") or dig(item, "extensions", "code") or "").strip()
        if code and message:
            messages.append(f"{code}: {message}")
        elif message:
            messages.append(message)
        elif code:
            messages.append(code)
    return "; ".join(messages) or "x.com analytics request failed"
