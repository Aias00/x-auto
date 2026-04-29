from __future__ import annotations


def test_tweet_record_from_native_maps_article_and_author_fields() -> None:
    from x_atuo.core.twitter_native_adapters import tweet_record_from_native

    class NativeAuthor:
        id = "user-1"
        name = "Author One"
        screen_name = "author_one"
        profile_image_url = "https://example.com/avatar.png"
        verified = True

    class NativeMetrics:
        likes = 1
        retweets = 2
        replies = 3
        quotes = 4
        views = 5
        bookmarks = 6

    class NativeTweet:
        id = "tweet-1"
        text = "hello"
        author = NativeAuthor()
        metrics = NativeMetrics()
        created_at = "2026-04-29T00:00:00+00:00"
        media = []
        urls = ["https://example.com"]
        is_retweet = False
        lang = "en"
        retweeted_by = None
        quoted_tweet = None
        score = 1.5
        article_title = "Longform Title"
        article_text = "Longform body"
        is_subscriber_only = False
        is_promoted = False

    tweet = tweet_record_from_native(NativeTweet())

    assert tweet.tweet_id == "tweet-1"
    assert tweet.text == "hello"
    assert tweet.screen_name == "author_one"
    assert tweet.verified is True
    assert tweet.article_title == "Longform Title"
    assert tweet.article_text == "Longform body"
    assert tweet.raw["metrics"]["likes"] == 1


def test_user_profile_payload_from_native_maps_counts_and_identity() -> None:
    from x_atuo.core.twitter_native_adapters import user_profile_payload_from_native

    class NativeProfile:
        id = "user-2"
        name = "Follower One"
        screen_name = "follower_one"
        bio = "bio"
        location = "earth"
        url = "https://example.com"
        followers_count = 10
        following_count = 20
        tweets_count = 30
        likes_count = 40
        verified = True
        profile_image_url = "https://example.com/p.png"
        created_at = "2026-04-29T00:00:00+00:00"

    payload = user_profile_payload_from_native(NativeProfile())

    assert payload == {
        "id": "user-2",
        "screen_name": "follower_one",
        "name": "Follower One",
        "bio": "bio",
        "location": "earth",
        "url": "https://example.com",
        "followers_count": 10,
        "following_count": 20,
        "tweets_count": 30,
        "likes_count": 40,
        "verified": True,
        "profile_image_url": "https://example.com/p.png",
        "created_at": "2026-04-29T00:00:00+00:00",
    }
