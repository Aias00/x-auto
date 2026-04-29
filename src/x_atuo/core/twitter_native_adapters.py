"""Shared adapters from vendored native Twitter models into x-atuo models."""

from __future__ import annotations

from typing import Any

from x_atuo.core.twitter_models import TweetRecord


def tweet_record_from_native(tweet: Any) -> TweetRecord:
    return TweetRecord.from_payload(native_tweet_to_payload(tweet))


def user_profile_payload_from_native(profile: Any) -> dict[str, Any]:
    return {
        "id": getattr(profile, "id", ""),
        "screen_name": getattr(profile, "screen_name", ""),
        "name": getattr(profile, "name", ""),
        "bio": getattr(profile, "bio", ""),
        "location": getattr(profile, "location", ""),
        "url": getattr(profile, "url", ""),
        "followers_count": getattr(profile, "followers_count", 0),
        "following_count": getattr(profile, "following_count", 0),
        "tweets_count": getattr(profile, "tweets_count", 0),
        "likes_count": getattr(profile, "likes_count", 0),
        "verified": getattr(profile, "verified", False),
        "profile_image_url": getattr(profile, "profile_image_url", ""),
        "created_at": getattr(profile, "created_at", ""),
    }


def native_tweet_to_payload(tweet: Any) -> dict[str, Any]:
    author = getattr(tweet, "author", None)
    metrics = getattr(tweet, "metrics", None)
    return {
        "id": getattr(tweet, "id", ""),
        "text": getattr(tweet, "text", ""),
        "created_at": getattr(tweet, "created_at", None),
        "author": {
            "id": getattr(author, "id", None),
            "name": getattr(author, "name", None),
            "screenName": getattr(author, "screen_name", None),
            "verified": bool(getattr(author, "verified", False)),
            "profileImageUrl": getattr(author, "profile_image_url", None),
        },
        "metrics": {
            "likes": getattr(metrics, "likes", 0),
            "retweets": getattr(metrics, "retweets", 0),
            "replies": getattr(metrics, "replies", 0),
            "quotes": getattr(metrics, "quotes", 0),
            "views": getattr(metrics, "views", 0),
            "bookmarks": getattr(metrics, "bookmarks", 0),
        },
        "lang": getattr(tweet, "lang", None),
        "urls": list(getattr(tweet, "urls", []) or []),
        "isRetweet": bool(getattr(tweet, "is_retweet", False)),
        "retweetedBy": getattr(tweet, "retweeted_by", None),
        "isSubscriberOnly": bool(getattr(tweet, "is_subscriber_only", False)),
        "isPromoted": bool(getattr(tweet, "is_promoted", False)),
        "articleTitle": getattr(tweet, "article_title", None),
        "articleText": getattr(tweet, "article_text", None),
    }
