"""Shared X GraphQL operation specs and GET URL builder."""

from __future__ import annotations

import json
import logging
import re
import urllib.parse
from dataclasses import dataclass
from typing import Any, Mapping

logger = logging.getLogger(__name__)

TWITTER_OPENAPI_URL = (
    "https://raw.githubusercontent.com/fa0311/"
    "twitter-openapi/refs/heads/main/src/config/placeholder.json"
)

FALLBACK_QUERY_IDS = {
    "HomeTimeline": "L8Lb9oomccM012S7fQ-QKA",
    "HomeLatestTimeline": "tzmrSIWxyV4IRRh9nij6TQ",
    "UserByScreenName": "IGgvgiOx4QZndDHuD3x9TQ",
    "UserTweets": "O0epvwaQPUx-bT9YlqlL6w",
    "TweetDetail": "xIYgDwjboktoFeXe_fgacw",
    "Likes": "RozQdCp4CilQzrcuU0NY5w",
    "SearchTimeline": "rkp6b4vtR9u7v3naGoOzUQ",
    "Bookmarks": "uzboyXSHSJrR-mGJqep0TQ",
    "ListLatestTweetsTimeline": "fb_6wmHD2dk9D-xYXOQlgw",
    "Followers": "Enf9DNUZYiT037aersI5gg",
    "Following": "ntIPnH1WMBKW--4Tn1q71A",
    "CreateTweet": "zkcFc6F-RKRgWN8HUkJfZg",
    "DeleteTweet": "nxpZCY2K-I6QoFHAHeojFQ",
    "FavoriteTweet": "lI07N6Otwv1PhnEgXILM7A",
    "UnfavoriteTweet": "ZYKSe-w7KEslx3JhSIk5LA",
    "CreateRetweet": "mbRO74GrOvSfRcJnlMapnQ",
    "DeleteRetweet": "ZyZigVsNiFO6v1dEks1eWg",
    "CreateBookmark": "aoDbu3RHznuiSkQ9aNM67Q",
    "DeleteBookmark": "Wlmlj2-xzyS1GN3a6cj-mQ",
    "TweetResultByRestId": "zy39CwTyYhU-_0LP7dljjg",
    "BookmarkFoldersSlice": "i78YDd0Tza-dV4SYs58kRg",
    "BookmarkFolderTimeline": "hNY7X2xE2N7HVF6Qb_mu6w",
}

_DEFAULT_FEATURES = {
    "responsive_web_graphql_exclude_directive_enabled": True,
    "verified_phone_label_enabled": False,
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "responsive_web_graphql_timeline_navigation_enabled": True,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "c9s_tweet_anatomy_moderator_badge_enabled": True,
    "tweetypie_unmention_optimization_enabled": True,
    "responsive_web_edit_tweet_api_enabled": True,
    "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
    "view_counts_everywhere_api_enabled": True,
    "longform_notetweets_consumption_enabled": True,
    "responsive_web_twitter_article_tweet_consumption_enabled": True,
    "tweet_awards_web_tipping_enabled": False,
    "longform_notetweets_rich_text_read_enabled": True,
    "longform_notetweets_inline_media_enabled": True,
    "rweb_video_timestamps_enabled": True,
    "responsive_web_media_download_video_enabled": True,
    "freedom_of_speech_not_reach_fetch_enabled": True,
    "standardized_nudges_misinfo": True,
    "responsive_web_enhance_cards_enabled": False,
}

FEATURES = dict(_DEFAULT_FEATURES)

_cached_query_ids: dict[str, str] = {}
_bundles_scanned = False


@dataclass(frozen=True, slots=True)
class XGraphQLOperationSpec:
    query_id: str
    operation_name: str
    referer: str
    features: Mapping[str, Any] | None = None
    field_toggles: Mapping[str, Any] | None = None


ACCOUNT_OVERVIEW_SPEC = XGraphQLOperationSpec(
    query_id="E0yfVvmFrSUeGpg0OIEWTA",
    operation_name="accountOverviewQuery",
    referer="https://x.com/i/account_analytics/overview",
)

CONTENT_POST_LIST_SPEC = XGraphQLOperationSpec(
    query_id="cUrPGzqG9MVSC7RKHWkN9w",
    operation_name="contentPostListQuery",
    referer="https://x.com/i/account_analytics/content",
)

NOTIFICATIONS_TIMELINE_SPEC = XGraphQLOperationSpec(
    query_id="Bn1PAViAcG1Mv4dQWkTG3A",
    operation_name="NotificationsTimeline",
    referer="https://x.com/notifications",
    features={
        "rweb_video_screen_enabled": True,
        "rweb_cashtags_enabled": True,
        "profile_label_improvements_pcf_label_in_post_enabled": True,
        "responsive_web_profile_redirect_enabled": True,
        "rweb_tipjar_consumption_enabled": True,
        "verified_phone_label_enabled": False,
        "creator_subscriptions_tweet_preview_api_enabled": True,
        "responsive_web_graphql_timeline_navigation_enabled": True,
        "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
        "premium_content_api_read_enabled": True,
        "communities_web_enable_tweet_community_results_fetch": True,
        "c9s_tweet_anatomy_moderator_badge_enabled": True,
        "responsive_web_grok_analyze_button_fetch_trends_enabled": True,
        "responsive_web_grok_analyze_post_followups_enabled": True,
        "responsive_web_jetfuel_frame": True,
        "responsive_web_grok_share_attachment_enabled": True,
        "responsive_web_grok_annotations_enabled": True,
        "articles_preview_enabled": True,
        "responsive_web_edit_tweet_api_enabled": True,
        "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
        "view_counts_everywhere_api_enabled": True,
        "longform_notetweets_consumption_enabled": True,
        "responsive_web_twitter_article_tweet_consumption_enabled": True,
        "content_disclosure_indicator_enabled": True,
        "content_disclosure_ai_generated_indicator_enabled": True,
        "responsive_web_grok_show_grok_translated_post": True,
        "responsive_web_grok_analysis_button_from_backend": True,
        "post_ctas_fetch_enabled": True,
        "freedom_of_speech_not_reach_fetch_enabled": True,
        "standardized_nudges_misinfo": True,
        "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
        "longform_notetweets_rich_text_read_enabled": True,
        "longform_notetweets_inline_media_enabled": True,
        "responsive_web_grok_image_annotation_enabled": True,
        "responsive_web_grok_imagine_annotation_enabled": True,
        "responsive_web_grok_community_note_auto_translation_is_enabled": True,
        "responsive_web_enhance_cards_enabled": False,
    },
    field_toggles={
        "withPayments": False,
        "withAuxiliaryUserLabels": False,
        "withArticleRichContentState": False,
        "withArticlePlainText": False,
        "withArticleSummaryText": False,
        "withArticleVoiceOver": False,
        "withGrokAnalyze": False,
        "withDisallowedReplyControls": False,
    },
)

TWEET_DETAIL_SPEC = XGraphQLOperationSpec(
    query_id=FALLBACK_QUERY_IDS["TweetDetail"],
    operation_name="TweetDetail",
    referer="https://x.com/",
    features={
        "responsive_web_graphql_exclude_directive_enabled": True,
        "verified_phone_label_enabled": False,
        "creator_subscriptions_tweet_preview_api_enabled": True,
        "responsive_web_graphql_timeline_navigation_enabled": True,
        "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
        "c9s_tweet_anatomy_moderator_badge_enabled": True,
        "tweetypie_unmention_optimization_enabled": True,
        "responsive_web_edit_tweet_api_enabled": True,
        "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
        "view_counts_everywhere_api_enabled": True,
        "longform_notetweets_consumption_enabled": True,
        "responsive_web_twitter_article_tweet_consumption_enabled": True,
        "tweet_awards_web_tipping_enabled": False,
        "longform_notetweets_rich_text_read_enabled": True,
        "longform_notetweets_inline_media_enabled": True,
        "rweb_video_timestamps_enabled": True,
        "responsive_web_media_download_video_enabled": True,
        "freedom_of_speech_not_reach_fetch_enabled": True,
        "standardized_nudges_misinfo": True,
        "responsive_web_enhance_cards_enabled": False,
    },
    field_toggles={
        "withArticleRichContentState": True,
        "withArticlePlainText": False,
        "withGrokAnalyze": False,
        "withDisallowedReplyControls": False,
    },
)


def build_graphql_get_url(spec: XGraphQLOperationSpec, *, variables: Mapping[str, Any]) -> str:
    return _build_graphql_url(
        spec.query_id,
        spec.operation_name,
        dict(variables),
        dict(spec.features or {}),
        dict(spec.field_toggles or {}) if spec.field_toggles else None,
    )


def _build_graphql_url(
    query_id: str,
    operation_name: str,
    variables: Mapping[str, Any],
    features: Mapping[str, Any],
    field_toggles: Mapping[str, Any] | None = None,
) -> str:
    compact_features = {k: v for k, v in features.items() if v is not False}
    url = "https://x.com/i/api/graphql/%s/%s?variables=%s" % (
        query_id,
        operation_name,
        urllib.parse.quote(json.dumps(dict(variables), separators=(",", ":"))),
    )
    if compact_features:
        url += "&features=%s" % urllib.parse.quote(
            json.dumps(compact_features, separators=(",", ":"))
        )
    if field_toggles:
        url += "&fieldToggles=%s" % urllib.parse.quote(
            json.dumps(dict(field_toggles), separators=(",", ":"))
        )
    return url


def _update_features_from_html(html: str) -> None:
    try:
        feature_pattern = re.compile(
            r'"([a-z][a-z0-9_]+)":\s*\{\s*"value"\s*:\s*(true|false)',
            re.IGNORECASE,
        )
        for match in feature_pattern.finditer(html):
            key = match.group(1)
            value = match.group(2).lower() == "true"
            if key in FEATURES:
                FEATURES[key] = value
    except Exception as exc:
        logger.debug("Feature extraction from HTML failed: %s", exc)


def _fetch_from_github(url_fetch_fn, operation_name: str) -> str | None:
    try:
        payload = url_fetch_fn(TWITTER_OPENAPI_URL)
        parsed = json.loads(payload)
        operation = parsed.get(operation_name, {})
        query_id = operation.get("queryId")
        if isinstance(query_id, str) and query_id:
            return query_id
    except Exception as exc:
        logger.debug("GitHub queryId lookup failed: %s", exc)
    return None


def _scan_bundles(url_fetch_fn) -> None:
    global _bundles_scanned
    if _bundles_scanned:
        return
    _bundles_scanned = True
    try:
        html = url_fetch_fn("https://x.com")
        script_pattern = re.compile(
            r'(?:src|href)=["\']'
            r'(https://abs\.twimg\.com/responsive-web/client-web[^"\']+'
            r'\.js)'
            r'["\']'
        )
        script_urls = script_pattern.findall(html)
    except Exception as exc:
        logger.warning("Failed to scan JS bundles: %s", exc)
        return
    for script_url in script_urls:
        try:
            bundle = url_fetch_fn(script_url)
            op_pattern = re.compile(
                r'queryId:\s*"([A-Za-z0-9_-]+)"[^}]{0,200}'
                r'operationName:\s*"([^"]+)"'
            )
            for match in op_pattern.finditer(bundle):
                query_id, operation_name = match.group(1), match.group(2)
                _cached_query_ids.setdefault(operation_name, query_id)
        except Exception:
            continue


def _invalidate_query_id(operation_name: str) -> None:
    _cached_query_ids.pop(operation_name, None)


def _resolve_query_id(operation_name: str, prefer_fallback: bool = True, url_fetch_fn=None) -> str:
    cached = _cached_query_ids.get(operation_name)
    if cached:
        return cached
    fallback = FALLBACK_QUERY_IDS.get(operation_name)
    if prefer_fallback and fallback:
        _cached_query_ids[operation_name] = fallback
        return fallback
    if url_fetch_fn:
        github_query_id = _fetch_from_github(url_fetch_fn, operation_name)
        if github_query_id:
            _cached_query_ids[operation_name] = github_query_id
            return github_query_id
        _scan_bundles(url_fetch_fn)
        cached = _cached_query_ids.get(operation_name)
        if cached:
            return cached
    if fallback:
        _cached_query_ids[operation_name] = fallback
        return fallback
    raise RuntimeError(f'Cannot resolve queryId for "{operation_name}"')
