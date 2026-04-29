from __future__ import annotations


def test_build_graphql_get_url_uses_shared_operation_spec() -> None:
    from x_atuo.core.x_graphql import ACCOUNT_OVERVIEW_SPEC, build_graphql_get_url

    url = build_graphql_get_url(
        ACCOUNT_OVERVIEW_SPEC,
        variables={"from_time": "2026-04-01T00:00:00Z", "to_time": "2026-04-02T00:00:00Z"},
    )

    assert f"/{ACCOUNT_OVERVIEW_SPEC.query_id}/{ACCOUNT_OVERVIEW_SPEC.operation_name}" in url
    assert "variables=" in url
    assert "features=" not in url
    assert "fieldToggles=" not in url


def test_build_graphql_get_url_omits_false_features_and_includes_field_toggles() -> None:
    from x_atuo.core.x_graphql import NOTIFICATIONS_TIMELINE_SPEC, build_graphql_get_url

    url = build_graphql_get_url(
        NOTIFICATIONS_TIMELINE_SPEC,
        variables={"timeline_type": "All", "count": 20},
    )

    assert f"/{NOTIFICATIONS_TIMELINE_SPEC.query_id}/{NOTIFICATIONS_TIMELINE_SPEC.operation_name}" in url
    assert "features=" in url
    assert "fieldToggles=" in url
    assert "responsive_web_enhance_cards_enabled" not in url
    assert "withPayments" in url


def test_normalize_post_payload_builds_consistent_shape() -> None:
    from x_atuo.core.x_web_normalization import normalize_post_payload

    post = normalize_post_payload(
        post_id="tweet-1",
        text="hello",
        created_at="2026-04-29T00:00:00Z",
        screen_name="author_one",
        author_name="Author One",
        rest_id="rest-1",
        verified=True,
        impressions=10,
        likes=2,
        replies=3,
        reposts=4,
        quotes=5,
        reply_to_id="parent-1",
        community_id="community-1",
    )

    assert post == {
        "id": "tweet-1",
        "text": "hello",
        "created_at": "2026-04-29T00:00:00Z",
        "url": "https://x.com/author_one/status/tweet-1",
        "author": {
            "screen_name": "author_one",
            "name": "Author One",
            "rest_id": "rest-1",
            "verified": True,
        },
        "public_metrics": {
            "impressions": 10,
            "likes": 2,
            "replies": 3,
            "reposts": 4,
            "quotes": 5,
        },
        "reply_to_id": "parent-1",
        "community_id": "community-1",
    }


def test_resolve_query_id_uses_shared_fallback_and_cache() -> None:
    from x_atuo.core.x_graphql import FALLBACK_QUERY_IDS, _invalidate_query_id, _resolve_query_id

    _invalidate_query_id("SearchTimeline")
    query_id = _resolve_query_id("SearchTimeline", prefer_fallback=True, url_fetch_fn=None)

    assert query_id == FALLBACK_QUERY_IDS["SearchTimeline"]


def test_update_features_from_html_only_mutates_known_keys() -> None:
    from x_atuo.core.x_graphql import FEATURES, _update_features_from_html

    original = dict(FEATURES)
    known_key = next(iter(FEATURES.keys()))
    try:
        html = f'"{known_key}":{{"value":false}} "brand_new_unknown_flag":{{"value":true}}'
        _update_features_from_html(html)
        assert FEATURES[known_key] is False
        assert "brand_new_unknown_flag" not in FEATURES
    finally:
        FEATURES.clear()
        FEATURES.update(original)
