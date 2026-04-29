from __future__ import annotations

from pathlib import Path

from x_atuo.core.twitter_client import TwitterClient, TwitterCredentials


def test_load_twitter_runtime_prefers_agent_reach_config(tmp_path: Path) -> None:
    from x_atuo.core.twitter_runtime import load_twitter_runtime

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "twitter_auth_token: cfg-token",
                "twitter_ct0: cfg-ct0",
            ]
        ),
        encoding="utf-8",
    )

    runtime = load_twitter_runtime(config_path, proxy="http://127.0.0.1:7890", base_env={})

    assert runtime.auth_token == "cfg-token"
    assert runtime.ct0 == "cfg-ct0"
    assert runtime.proxy == "http://127.0.0.1:7890"


def test_twitter_client_reply_uses_native_client(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeNativeClient:
        def create_tweet(self, text, reply_to_id=None, media_ids=None):
            captured["text"] = text
            captured["reply_to_id"] = reply_to_id
            captured["media_ids"] = media_ids
            return "tweet-native-1"

    monkeypatch.setattr(
        TwitterClient,
        "_build_native_client",
        lambda self: FakeNativeClient(),
    )

    client = TwitterClient(
        twitter_bin="twitter",
        credentials=TwitterCredentials(auth_token="token", ct0="csrf"),
        proxy="http://127.0.0.1:7890",
    )

    result = client.reply("123", "hello")

    assert result.ok is True
    assert result.tweet_id == "tweet-native-1"
    assert captured == {
        "text": "hello",
        "reply_to_id": "123",
        "media_ids": None,
    }


def test_twitter_client_fetch_feed_uses_native_client(monkeypatch) -> None:
    class NativeAuthor:
        id = "user-1"
        name = "Author One"
        screen_name = "author_one"
        profile_image_url = ""
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

    class FakeNativeClient:
        def fetch_home_timeline(self, count=20, include_promoted=False, cursor=None, return_cursor=False):
            assert count == 3
            return [NativeTweet()]

    monkeypatch.setattr(
        TwitterClient,
        "_build_native_client",
        lambda self: FakeNativeClient(),
    )

    client = TwitterClient(
        twitter_bin="twitter",
        credentials=TwitterCredentials(auth_token="token", ct0="csrf"),
    )

    tweets = client.fetch_feed(max_items=3)

    assert [tweet.tweet_id for tweet in tweets] == ["tweet-1"]
    assert tweets[0].screen_name == "author_one"


def test_twitter_client_fetch_user_posts_uses_native_client(monkeypatch) -> None:
    class NativeAuthor:
        id = "user-1"
        name = "Author One"
        screen_name = "author_one"
        profile_image_url = ""
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

    class FakeNativeClient:
        def resolve_user_id(self, identifier):
            assert identifier == "author_one"
            return "user-1"

        def fetch_user_tweets(self, user_id, count=20):
            assert user_id == "user-1"
            assert count == 2
            return [NativeTweet()]

    monkeypatch.setattr(
        TwitterClient,
        "_build_native_client",
        lambda self: FakeNativeClient(),
    )

    client = TwitterClient(
        twitter_bin="twitter",
        credentials=TwitterCredentials(auth_token="token", ct0="csrf"),
    )

    tweets = client.fetch_user_posts("author_one", max_items=2)

    assert [tweet.tweet_id for tweet in tweets] == ["tweet-1"]


def test_twitter_client_fetch_search_uses_native_client(monkeypatch) -> None:
    class NativeAuthor:
        id = "user-1"
        name = "Author One"
        screen_name = "author_one"
        profile_image_url = ""
        verified = True

    class NativeMetrics:
        likes = 1
        retweets = 2
        replies = 3
        quotes = 4
        views = 5
        bookmarks = 6

    class NativeTweet:
        id = "tweet-search-1"
        text = "search result"
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

    class FakeNativeClient:
        def fetch_search(self, query, count=20, product="Top"):
            assert query == "openai"
            assert count == 4
            assert product == "Latest"
            return [NativeTweet()]

    monkeypatch.setattr(TwitterClient, "_build_native_client", lambda self: FakeNativeClient())

    client = TwitterClient(twitter_bin="twitter", credentials=TwitterCredentials(auth_token="token", ct0="csrf"))

    tweets = client.fetch_search("openai", max_items=4, product="Latest")

    assert [tweet.tweet_id for tweet in tweets] == ["tweet-search-1"]


def test_twitter_client_fetch_bookmarks_uses_native_client(monkeypatch) -> None:
    class NativeAuthor:
        id = "user-1"
        name = "Author One"
        screen_name = "author_one"
        profile_image_url = ""
        verified = True

    class NativeMetrics:
        likes = 1
        retweets = 2
        replies = 3
        quotes = 4
        views = 5
        bookmarks = 6

    class NativeTweet:
        id = "tweet-bookmark-1"
        text = "bookmark result"
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

    class FakeNativeClient:
        def fetch_bookmarks(self, count=50):
            assert count == 7
            return [NativeTweet()]

    monkeypatch.setattr(TwitterClient, "_build_native_client", lambda self: FakeNativeClient())

    client = TwitterClient(twitter_bin="twitter", credentials=TwitterCredentials(auth_token="token", ct0="csrf"))

    tweets = client.fetch_bookmarks(max_items=7)

    assert [tweet.tweet_id for tweet in tweets] == ["tweet-bookmark-1"]


def test_twitter_client_fetch_user_likes_uses_native_client(monkeypatch) -> None:
    class NativeAuthor:
        id = "user-1"
        name = "Author One"
        screen_name = "author_one"
        profile_image_url = ""
        verified = True

    class NativeMetrics:
        likes = 1
        retweets = 2
        replies = 3
        quotes = 4
        views = 5
        bookmarks = 6

    class NativeTweet:
        id = "tweet-like-1"
        text = "liked tweet"
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

    class FakeNativeClient:
        def resolve_user_id(self, identifier):
            assert identifier == "author_one"
            return "user-1"

        def fetch_user_likes(self, user_id, count=20):
            assert user_id == "user-1"
            assert count == 6
            return [NativeTweet()]

    monkeypatch.setattr(TwitterClient, "_build_native_client", lambda self: FakeNativeClient())

    client = TwitterClient(twitter_bin="twitter", credentials=TwitterCredentials(auth_token="token", ct0="csrf"))

    tweets = client.fetch_user_likes("author_one", max_items=6)

    assert [tweet.tweet_id for tweet in tweets] == ["tweet-like-1"]


def test_twitter_client_fetch_followers_uses_native_client(monkeypatch) -> None:
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

    class FakeNativeClient:
        def resolve_user_id(self, identifier):
            assert identifier == "author_one"
            return "user-1"

        def fetch_followers(self, user_id, count=20):
            assert user_id == "user-1"
            assert count == 3
            return [NativeProfile()]

    monkeypatch.setattr(TwitterClient, "_build_native_client", lambda self: FakeNativeClient())

    client = TwitterClient(twitter_bin="twitter", credentials=TwitterCredentials(auth_token="token", ct0="csrf"))

    users = client.fetch_followers("author_one", max_items=3)

    assert users[0]["screen_name"] == "follower_one"
    assert users[0]["verified"] is True


def test_twitter_client_fetch_following_uses_native_client(monkeypatch) -> None:
    class NativeProfile:
        id = "user-3"
        name = "Following One"
        screen_name = "following_one"
        bio = "bio"
        location = "earth"
        url = "https://example.com"
        followers_count = 10
        following_count = 20
        tweets_count = 30
        likes_count = 40
        verified = False
        profile_image_url = "https://example.com/p.png"
        created_at = "2026-04-29T00:00:00+00:00"

    class FakeNativeClient:
        def resolve_user_id(self, identifier):
            assert identifier == "author_one"
            return "user-1"

        def fetch_following(self, user_id, count=20):
            assert user_id == "user-1"
            assert count == 3
            return [NativeProfile()]

    monkeypatch.setattr(TwitterClient, "_build_native_client", lambda self: FakeNativeClient())

    client = TwitterClient(twitter_bin="twitter", credentials=TwitterCredentials(auth_token="token", ct0="csrf"))

    users = client.fetch_following("author_one", max_items=3)

    assert users[0]["screen_name"] == "following_one"
    assert users[0]["verified"] is False


def test_twitter_client_retweet_uses_native_client(monkeypatch) -> None:
    class FakeNativeClient:
        def retweet(self, tweet_id):
            assert tweet_id == "tweet-1"
            return True

    monkeypatch.setattr(TwitterClient, "_build_native_client", lambda self: FakeNativeClient())

    client = TwitterClient(twitter_bin="twitter", credentials=TwitterCredentials(auth_token="token", ct0="csrf"))

    result = client.retweet("tweet-1")

    assert result.ok is True
    assert result.target_tweet_id == "tweet-1"


def test_twitter_client_unretweet_uses_native_client(monkeypatch) -> None:
    class FakeNativeClient:
        def unretweet(self, tweet_id):
            assert tweet_id == "tweet-1"
            return True

    monkeypatch.setattr(TwitterClient, "_build_native_client", lambda self: FakeNativeClient())

    client = TwitterClient(twitter_bin="twitter", credentials=TwitterCredentials(auth_token="token", ct0="csrf"))

    result = client.unretweet("tweet-1")

    assert result.ok is True
    assert result.target_tweet_id == "tweet-1"


def test_twitter_client_like_uses_native_client(monkeypatch) -> None:
    class FakeNativeClient:
        def like_tweet(self, tweet_id):
            assert tweet_id == "tweet-1"
            return True

    monkeypatch.setattr(TwitterClient, "_build_native_client", lambda self: FakeNativeClient())

    client = TwitterClient(twitter_bin="twitter", credentials=TwitterCredentials(auth_token="token", ct0="csrf"))

    result = client.like("tweet-1")

    assert result.ok is True
    assert result.target_tweet_id == "tweet-1"


def test_twitter_client_unlike_uses_native_client(monkeypatch) -> None:
    class FakeNativeClient:
        def unlike_tweet(self, tweet_id):
            assert tweet_id == "tweet-1"
            return True

    monkeypatch.setattr(TwitterClient, "_build_native_client", lambda self: FakeNativeClient())

    client = TwitterClient(twitter_bin="twitter", credentials=TwitterCredentials(auth_token="token", ct0="csrf"))

    result = client.unlike("tweet-1")

    assert result.ok is True
    assert result.target_tweet_id == "tweet-1"


def test_twitter_client_bookmark_uses_native_client(monkeypatch) -> None:
    class FakeNativeClient:
        def bookmark_tweet(self, tweet_id):
            assert tweet_id == "tweet-1"
            return True

    monkeypatch.setattr(TwitterClient, "_build_native_client", lambda self: FakeNativeClient())

    client = TwitterClient(twitter_bin="twitter", credentials=TwitterCredentials(auth_token="token", ct0="csrf"))

    result = client.bookmark("tweet-1")

    assert result.ok is True
    assert result.target_tweet_id == "tweet-1"


def test_twitter_client_unbookmark_uses_native_client(monkeypatch) -> None:
    class FakeNativeClient:
        def unbookmark_tweet(self, tweet_id):
            assert tweet_id == "tweet-1"
            return True

    monkeypatch.setattr(TwitterClient, "_build_native_client", lambda self: FakeNativeClient())

    client = TwitterClient(twitter_bin="twitter", credentials=TwitterCredentials(auth_token="token", ct0="csrf"))

    result = client.unbookmark("tweet-1")

    assert result.ok is True
    assert result.target_tweet_id == "tweet-1"


def test_twitter_client_delete_tweet_uses_native_client(monkeypatch) -> None:
    class FakeNativeClient:
        def delete_tweet(self, tweet_id):
            assert tweet_id == "tweet-1"
            return True

    monkeypatch.setattr(TwitterClient, "_build_native_client", lambda self: FakeNativeClient())

    client = TwitterClient(twitter_bin="twitter", credentials=TwitterCredentials(auth_token="token", ct0="csrf"))

    result = client.delete_tweet("tweet-1")

    assert result.ok is True
    assert result.target_tweet_id == "tweet-1"


def test_twitter_client_fetch_article_uses_native_client(monkeypatch) -> None:
    class NativeAuthor:
        id = "user-1"
        name = "Author One"
        screen_name = "author_one"
        profile_image_url = ""
        verified = True

    class NativeMetrics:
        likes = 1
        retweets = 2
        replies = 3
        quotes = 4
        views = 5
        bookmarks = 6

    class NativeTweet:
        id = "tweet-article-1"
        text = "article preview"
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
        article_title = "Longform Title"
        article_text = "Longform body"
        is_subscriber_only = False
        is_promoted = False

    class FakeNativeClient:
        def fetch_article(self, tweet_id):
            assert tweet_id == "tweet-article-1"
            return NativeTweet()

    monkeypatch.setattr(TwitterClient, "_build_native_client", lambda self: FakeNativeClient())

    client = TwitterClient(twitter_bin="twitter", credentials=TwitterCredentials(auth_token="token", ct0="csrf"))

    tweet = client.fetch_article("tweet-article-1")

    assert tweet.tweet_id == "tweet-article-1"
    assert tweet.article_title == "Longform Title"
    assert tweet.article_text == "Longform body"
