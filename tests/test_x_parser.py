from __future__ import annotations


def test_parse_user_result_from_shared_parser() -> None:
    from x_atuo.core.x_parser import parse_user_result

    result = parse_user_result(
        {
            "rest_id": "user-1",
            "is_blue_verified": True,
            "legacy": {
                "name": "Alice",
                "screen_name": "alice",
                "description": "bio",
                "location": "earth",
                "followers_count": "10",
                "friends_count": "20",
                "statuses_count": "30",
                "favourites_count": "40",
                "profile_image_url_https": "https://example.com/avatar.png",
                "created_at": "Wed Apr 29 00:00:00 +0000 2026",
            },
        }
    )

    assert result is not None
    assert result.id == "user-1"
    assert result.screen_name == "alice"
    assert result.verified is True


def test_parse_timeline_response_from_shared_parser() -> None:
    from x_atuo.core.x_parser import parse_timeline_response

    def get_instructions(data):
        return data["instructions"]

    tweets, cursor = parse_timeline_response(
        {
            "instructions": [
                {
                    "entries": [
                        {
                            "content": {
                                "cursorType": "Bottom",
                                "value": "cursor-1",
                                "itemContent": {
                                    "tweet_results": {
                                        "result": {
                                            "__typename": "Tweet",
                                            "rest_id": "tweet-1",
                                            "legacy": {
                                                "full_text": "hello",
                                                "favorite_count": 1,
                                                "retweet_count": 2,
                                                "reply_count": 3,
                                                "quote_count": 4,
                                                "created_at": "Wed Apr 29 00:00:00 +0000 2026",
                                                "lang": "en",
                                            },
                                            "core": {
                                                "user_results": {
                                                    "result": {
                                                        "rest_id": "user-1",
                                                        "legacy": {
                                                            "name": "Alice",
                                                            "screen_name": "alice",
                                                            "profile_image_url_https": "https://example.com/avatar.png",
                                                        },
                                                    }
                                                }
                                            },
                                            "views": {"count": "5"},
                                        }
                                    }
                                },
                            }
                        }
                    ]
                }
            ]
        },
        get_instructions,
    )

    assert cursor == "cursor-1"
    assert len(tweets) == 1
    assert tweets[0].id == "tweet-1"
    assert tweets[0].author.screen_name == "alice"


def test_core_parser_exports_tweet_parser_symbol() -> None:
    from x_atuo.core.x_parser import parse_tweet_result

    assert parse_tweet_result is not None
