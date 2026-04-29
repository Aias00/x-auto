from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path

from fastapi.testclient import TestClient

from x_atuo.automation.api import app
from x_atuo.automation.config import PolicyConfig
from x_atuo.automation.config import AutomationConfig
from x_atuo.automation.config import AISettings
from x_atuo.automation.config import SchedulerSettings
from x_atuo.automation.graph import AutomationGraph
from x_atuo.automation.graph import WorkflowAdapters
from x_atuo.automation.graph import _AI_MODERATION_METADATA_KEY
from x_atuo.automation.graph import _build_runtime_graph
from x_atuo.automation.policies import build_dedupe_key
from x_atuo.automation.scheduler import AutomationScheduler
from x_atuo.automation.state import FeedCandidate
from x_atuo.automation.state import AutomationRequest
from x_atuo.automation.state import ExecutionResult
from x_atuo.automation.state import RunStatus
from x_atuo.automation.state import WorkflowKind
from x_atuo.automation.state import make_initial_state
from x_atuo.automation.storage import AutomationStorage
import x_atuo.automation.api as automation_api
from x_atuo.core.ai_client import AIProviderError
from x_atuo.core.ai_client import OpenAICompatibleProvider
from x_atuo.core.ai_client import build_moderation_cache_key
from x_atuo.core.twitter_client import TwitterClient
from x_atuo.core.twitter_client import TwitterCredentials
from x_atuo.core.twitter_models import TweetAuthor, TweetRecord, TwitterCommandResult


def test_dedupe_key_is_stable() -> None:
    request = AutomationRequest.for_feed_engage(reply_text="hello")
    assert build_dedupe_key(request, text="hello") == build_dedupe_key(request, text="hello")


def test_daily_execution_limit_disabled_by_default() -> None:
    config = AutomationConfig(_env_file=None)
    assert config.policies.daily_execution_limit is None


def test_author_cooldown_disabled_by_default() -> None:
    config = AutomationConfig(_env_file=None)
    assert config.policies.per_author_cooldown_minutes is None


def test_author_alpha_config_defaults() -> None:
    config = AutomationConfig(_env_file=None)

    assert config.author_alpha.enabled is False
    assert config.author_alpha.excluded_authors == []
    assert config.author_alpha.device_follow_feed_count == 50
    assert config.author_alpha.daily_execution_limit == 700
    assert config.author_alpha.global_send_limit_15m == 50
    assert config.author_alpha.posts_per_author == 1
    assert config.author_alpha.target_revisit_cooldown_seconds == 3600
    assert config.author_alpha.max_targets_per_run == 5
    assert config.author_alpha.per_run_same_target_burst_limit == 2
    assert config.author_alpha.inter_reply_delay_seconds == 5
    assert config.author_alpha.target_switch_delay_seconds == 10
    assert config.author_alpha.score_min_daily_replies == 400
    assert config.author_alpha.score_prior_weight == 7.0
    assert config.author_alpha.score_penalty_constant == 200.0


def test_author_alpha_request_builder_exists() -> None:
    request = AutomationRequest.for_author_alpha_engage(job_name="job")

    assert request.workflow.value == "author-alpha-engage"
    assert request.job_name == "job"


def test_local_dotenv_overrides_shell_env(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "X_ATUO_AI__PROVIDER=openai_compatible",
                "X_ATUO_AI__MODEL=dotenv-model",
                "X_ATUO_AI__BASE_URL=https://example.com/v1",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("X_ATUO_AI__MODEL", "shell-model")

    config = AutomationConfig(_env_file=env_file)

    assert config.ai.provider == "openai_compatible"
    assert config.ai.model == "dotenv-model"
    assert config.ai.base_url == "https://example.com/v1"


def test_feed_engage_config_defaults_are_ready_to_run() -> None:
    config = AutomationConfig()
    request = AutomationRequest.for_feed_engage()

    assert request.approval_mode == "ai_auto"
    assert request.dry_run is False
    assert request.feed_options is not None
    assert request.feed_options.feed_count == config.twitter.default_feed_count
    assert request.feed_options.feed_type == config.twitter.default_feed_type
    assert "candidate" not in AutomationRequest.model_fields
    assert "candidates" not in AutomationRequest.model_fields
    assert config.twitter.proxy_url == "http://127.0.0.1:7890"
    assert config.twitter.default_feed_count == 10
    assert config.twitter.default_feed_type == "for-you"


def test_feed_engage_request_normalizes_legacy_approval_mode_to_ai_auto() -> None:
    request = AutomationRequest.for_feed_engage(approval_mode="human_review")

    assert request.approval_mode == "ai_auto"


def test_candidate_refresh_rounds_comes_from_policy_config() -> None:
    config = AutomationConfig(policies=PolicyConfig(candidate_refresh_rounds=5))

    graph = AutomationGraph(config)

    assert graph.max_candidate_refresh_rounds == 5


def test_route_after_fetch_feed_retries_immediately_when_refresh_pending() -> None:
    graph = AutomationGraph(AutomationConfig())
    request = AutomationRequest.for_feed_engage(job_name="job")
    state = make_initial_state(request)
    state["snapshot"].candidate_refresh_pending = True

    route = graph.route_after_fetch_feed(state)

    assert route == "fetch_feed"


def test_route_after_fetch_feed_prefilters_before_candidate_evaluation() -> None:
    graph = AutomationGraph(AutomationConfig())
    request = AutomationRequest.for_feed_engage(job_name="job")
    state = make_initial_state(request)
    state["snapshot"].candidates = [
        FeedCandidate(tweet_id="111", screen_name="author1", text="candidate", author_verified=True),
    ]

    route = graph.route_after_fetch_feed(state)

    assert route == "prefilter_candidates"


def test_route_after_prefilter_goes_directly_to_select_candidate_without_batch_moderation() -> None:
    graph = AutomationGraph(AutomationConfig(), WorkflowAdapters(policy_hooks=None))
    request = AutomationRequest.for_feed_engage(job_name="job")
    state = make_initial_state(request)
    state["snapshot"].candidates = [
        FeedCandidate(tweet_id="111", screen_name="author1", text="candidate", author_verified=True),
    ]

    route = graph.route_after_prefilter(state)

    assert route == "select_candidate"


def test_route_after_selection_goes_directly_to_candidate_policy_guard() -> None:
    graph = AutomationGraph(AutomationConfig())
    request = AutomationRequest.for_feed_engage(job_name="job")
    state = make_initial_state(request)

    route = graph.route_after_selection(state)

    assert route == "candidate_policy_guard"


def test_policy_guard_releases_unused_claimed_candidates_after_early_stop() -> None:
    released: dict[str, object] = {}

    class FakeHooks:
        def has_dedupe_key(self, dedupe_key: str) -> bool:
            return False

        def get_daily_execution_count(self, workflow, day) -> int:
            return 0

        def get_last_author_engagement(self, screen_name: str):
            return None

        def has_target_tweet_id(self, target_tweet_id: str) -> bool:
            return False

        def release_claimed_candidate_cache(self, *, workflow: str, run_id: str, tweet_ids: list[str]) -> int:
            released["workflow"] = workflow
            released["run_id"] = run_id
            released["tweet_ids"] = tweet_ids
            return len(tweet_ids)

    graph = AutomationGraph(AutomationConfig(), WorkflowAdapters(policy_hooks=FakeHooks()))
    request = AutomationRequest.for_feed_engage(job_name="job", run_id="run-123")
    state = make_initial_state(request)
    snapshot = state["snapshot"]
    snapshot.selected_candidate = FeedCandidate(
        tweet_id="111",
        screen_name="author1",
        text="one",
        author_verified=True,
        metadata={"_x_atuo_candidate_cache": True, "_x_atuo_claim_run_id": "run-123"},
    )
    snapshot.candidates = [
        snapshot.selected_candidate,
        FeedCandidate(tweet_id="222", screen_name="author2", text="two", author_verified=True, metadata={"_x_atuo_candidate_cache": True, "_x_atuo_claim_run_id": "run-123"}),
        FeedCandidate(tweet_id="333", screen_name="author3", text="three", author_verified=True, metadata={"_x_atuo_candidate_cache": True, "_x_atuo_claim_run_id": "run-123"}),
    ]
    snapshot.rendered_text = "draft one"

    result = asyncio.run(graph.policy_guard(state))

    assert [candidate.tweet_id for candidate in result["snapshot"].candidates] == ["111"]
    assert released == {
        "workflow": "feed_engage",
        "run_id": "run-123",
        "tweet_ids": ["222", "333"],
    }
    assert any(
        event.node == "policy_guard"
        and event.message == "unused claimed candidates released"
        and event.payload["count"] == 2
        and event.payload["tweet_ids"] == ["222", "333"]
        for event in result["snapshot"].events
    )


def test_policy_guard_retries_duplicate_candidate_before_releasing_claims() -> None:
    released: dict[str, object] = {}

    class FakeHooks:
        def has_dedupe_key(self, dedupe_key: str) -> bool:
            return True

        def get_daily_execution_count(self, workflow, day) -> int:
            return 0

        def get_last_author_engagement(self, screen_name: str):
            return None

        def has_target_tweet_id(self, target_tweet_id: str) -> bool:
            return False

        def release_claimed_candidate_cache(self, *, workflow: str, run_id: str, tweet_ids: list[str]) -> int:
            released["workflow"] = workflow
            released["run_id"] = run_id
            released["tweet_ids"] = tweet_ids
            return len(tweet_ids)

    graph = AutomationGraph(AutomationConfig(), WorkflowAdapters(policy_hooks=FakeHooks()))
    request = AutomationRequest.for_feed_engage(job_name="job", run_id="run-123")
    state = make_initial_state(request)
    snapshot = state["snapshot"]
    snapshot.selected_candidate = FeedCandidate(
        tweet_id="111",
        screen_name="author1",
        text="one",
        author_verified=True,
        metadata={"_x_atuo_candidate_cache": True, "_x_atuo_claim_run_id": "run-123"},
    )
    snapshot.candidates = [
        snapshot.selected_candidate,
        FeedCandidate(tweet_id="222", screen_name="author2", text="two", author_verified=True, metadata={"_x_atuo_candidate_cache": True, "_x_atuo_claim_run_id": "run-123"}),
    ]
    snapshot.rendered_text = "draft one"

    result = asyncio.run(graph.policy_guard(state))
    routed = graph.route_after_policy(result)

    assert released == {}
    assert result["snapshot"].selected_candidate is None
    assert [candidate.tweet_id for candidate in result["snapshot"].candidates] == ["222"]
    assert routed == "retry_candidate"


def test_prefilter_candidates_filters_unverified_without_policy_hooks() -> None:
    graph = AutomationGraph(AutomationConfig(), WorkflowAdapters(policy_hooks=None))
    request = AutomationRequest.for_feed_engage(job_name="job")
    state = make_initial_state(request)
    snapshot = state["snapshot"]
    snapshot.candidates = [
        FeedCandidate(tweet_id="111", screen_name="ghost", text="candidate", author_verified=False),
        FeedCandidate(tweet_id="222", screen_name="real", text="candidate", author_verified=True),
    ]

    result = asyncio.run(graph.prefilter_candidates(state))

    assert [candidate.tweet_id for candidate in result["snapshot"].candidates] == ["222"]
    assert any(
        event.node == "prefilter_candidates" and event.message == "candidate removed before selection" and event.payload["reason"] == "author not verified"
        for event in result["snapshot"].events
    )


def test_prefilter_candidates_filters_reply_restricted_before_selection() -> None:
    graph = AutomationGraph(AutomationConfig(), WorkflowAdapters(policy_hooks=None))
    request = AutomationRequest.for_feed_engage(job_name="job")
    state = make_initial_state(request)
    snapshot = state["snapshot"]
    snapshot.candidates = [
        FeedCandidate(
            tweet_id="111",
            screen_name="limited",
            text="candidate",
            author_verified=True,
            can_reply=False,
            reply_limit_reason="Only some accounts can reply.",
        ),
        FeedCandidate(tweet_id="222", screen_name="open", text="candidate", author_verified=True, can_reply=True),
    ]

    result = asyncio.run(graph.prefilter_candidates(state))

    assert [candidate.tweet_id for candidate in result["snapshot"].candidates] == ["222"]
    assert any(
        event.node == "prefilter_candidates"
        and event.message == "candidate removed before selection"
        and event.payload["reason"] == "Only some accounts can reply."
        for event in result["snapshot"].events
    )


def test_prefilter_candidates_preserves_reply_restriction_reason_when_pool_is_exhausted() -> None:
    graph = AutomationGraph(
        AutomationConfig(policies=PolicyConfig(candidate_refresh_rounds=0)),
        WorkflowAdapters(policy_hooks=None),
    )
    request = AutomationRequest.for_feed_engage(job_name="job")
    state = make_initial_state(request)
    snapshot = state["snapshot"]
    snapshot.candidates = [
        FeedCandidate(
            tweet_id="111",
            screen_name="limited",
            text="candidate",
            author_verified=True,
            can_reply=False,
            reply_limit_reason="Only some accounts can reply.",
        ),
    ]

    result = asyncio.run(graph.prefilter_candidates(state))
    routed = graph.route_after_prefilter(result)

    assert result["snapshot"].status is RunStatus.BLOCKED
    assert result["snapshot"].errors == ["Only some accounts can reply."]
    assert routed == "blocked"


def test_prepare_no_longer_preloads_candidates_from_request_surface() -> None:
    graph = AutomationGraph(
        AutomationConfig(),
        WorkflowAdapters(
            fetch_feed=lambda snapshot: [
                FeedCandidate(
                    tweet_id="111",
                    screen_name="cached",
                    text="cached candidate",
                    metadata={"_x_atuo_candidate_cache": True},
                )
            ]
        ),
    )
    request = AutomationRequest.for_feed_engage(job_name="job")
    state = make_initial_state(request)

    prepared = asyncio.run(graph.prepare(state))
    routed = graph.route_after_prepare(prepared)

    assert routed == "fetch_feed"
    assert prepared["snapshot"].candidates == []
    assert prepared["snapshot"].candidate_cache_persisted is False

    fetched = asyncio.run(graph.fetch_feed(prepared))

    assert fetched["snapshot"].candidates[0].tweet_id == "111"
    assert any(
        event.node == "fetch_feed" and event.message == "feed fetched"
        for event in fetched["snapshot"].events
    )


def test_prepare_ignores_frozen_selection_and_draft_metadata() -> None:
    graph = AutomationGraph(AutomationConfig())
    request = AutomationRequest.for_feed_engage(
        job_name="job",
        metadata={
            "frozen_selection_source": "ai",
            "frozen_selection_reason": "old reason",
            "frozen_drafted_by": "ai",
        },
    )
    state = make_initial_state(request)

    prepared = asyncio.run(graph.prepare(state))

    assert prepared["snapshot"].selection_source is None
    assert prepared["snapshot"].selection_reason is None
    assert prepared["snapshot"].drafting_source is None


def test_tweet_record_parses_created_at() -> None:
    tweet = TweetRecord.from_payload(
        {
            "id": "111",
            "text": "hello",
            "created_at": "2026-04-20T03:35:55+00:00",
            "author": {"screenName": "demo", "verified": True},
        }
    )

    assert tweet.created_at == datetime.fromisoformat("2026-04-20T03:35:55+00:00")


def test_tweet_record_parses_reply_limit_fields() -> None:
    tweet = TweetRecord.from_payload(
        {
            "id": "111",
            "text": "hello",
            "author": {"screenName": "demo", "verified": True},
            "canReply": False,
            "replyLimitHeadline": "Who can reply?",
            "replyLimitReason": "Only some accounts can reply.",
            "replyRestrictionPolicy": "Co",
        }
    )

    assert tweet.can_reply is False
    assert tweet.reply_limit_headline == "Who can reply?"
    assert tweet.reply_limit_reason == "Only some accounts can reply."
    assert tweet.reply_restriction_policy == "Co"


def test_twitter_client_fetch_tweet_records_conversation_policy_as_risk() -> None:
    class LocalTwitterClient(TwitterClient):
        def _build_native_client(self):
            class NativeAuthor:
                id = "user-1"
                name = "Demo"
                screen_name = "demo"
                profile_image_url = ""
                verified = True

            class NativeMetrics:
                likes = 0
                retweets = 0
                replies = 0
                quotes = 0
                views = 0
                bookmarks = 0

            class NativeTweet:
                id = "111"
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

            return type("NativeClient", (), {"fetch_tweet_detail": lambda self_, tweet_id, count=1: [NativeTweet()]})()

        def _fetch_tweet_detail_payload(self, tweet_id: str):
            return {
                "data": {
                    "threaded_conversation_with_injections_v2": {
                        "instructions": [
                            {
                                "entries": [
                                    {
                                        "content": {
                                            "itemContent": {
                                                "tweet_results": {
                                                    "result": {
                                                        "__typename": "TweetWithVisibilityResults",
                                                        "tweet": {
                                                            "rest_id": tweet_id,
                                                            "legacy": {
                                                                "conversation_control": {"policy": "Co"}
                                                            },
                                                        },
                                                    }
                                                }
                                            }
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                }
            }

    client = LocalTwitterClient(
        twitter_bin="twitter",
        credentials=TwitterCredentials(auth_token="token", ct0="csrf"),
    )

    tweet = client.fetch_tweet("111")

    assert tweet.can_reply is None
    assert tweet.reply_limit_headline is None
    assert tweet.reply_limit_reason is None
    assert tweet.reply_restriction_policy == "Co"


def test_twitter_client_fetch_tweet_locally_enriches_reply_limit() -> None:
    class LocalTwitterClient(TwitterClient):
        def _build_native_client(self):
            class NativeAuthor:
                id = "user-1"
                name = "Demo"
                screen_name = "demo"
                profile_image_url = ""
                verified = True

            class NativeMetrics:
                likes = 0
                retweets = 0
                replies = 0
                quotes = 0
                views = 0
                bookmarks = 0

            class NativeTweet:
                id = "111"
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

            return type("NativeClient", (), {"fetch_tweet_detail": lambda self_, tweet_id, count=1: [NativeTweet()]})()

        def _fetch_tweet_detail_payload(self, tweet_id: str):
            assert tweet_id == "111"
            return {
                "data": {
                    "threaded_conversation_with_injections_v2": {
                        "instructions": [
                            {
                                "entries": [
                                    {
                                        "content": {
                                            "itemContent": {
                                                "tweet_results": {
                                                    "result": {
                                                        "__typename": "TweetWithVisibilityResults",
                                                        "limitedActionResults": {
                                                            "limited_actions": [
                                                                {
                                                                    "action": "Reply",
                                                                    "prompt": {
                                                                        "headline": {"text": "Who can reply?"},
                                                                        "subtext": {"text": "Only some accounts can reply."},
                                                                    },
                                                                }
                                                            ]
                                                        },
                                                        "tweet": {"rest_id": "111"},
                                                    }
                                                }
                                            }
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                }
            }

    client = LocalTwitterClient(
        twitter_bin="twitter",
        credentials=TwitterCredentials(auth_token="token", ct0="csrf"),
    )

    tweet = client.fetch_tweet("111")

    assert tweet.can_reply is False
    assert tweet.reply_limit_headline == "Who can reply?"
    assert tweet.reply_limit_reason == "Only some accounts can reply."












def test_candidate_cache_cleanup_removes_expired_entries(tmp_path: Path) -> None:
    storage = AutomationStorage(tmp_path / "candidate-cache.sqlite3")
    storage.initialize()
    storage.upsert_candidate_cache_entries(
        workflow="feed_engage",
        source_run_id="run_1",
        candidates=[
            {
                "tweet_id": "111",
                "screen_name": "demo",
                "created_at": "2026-04-20T03:35:55+00:00",
                "text": "cached text",
                "metadata": {"id": "111", "text": "cached text", "author": {"screenName": "demo", "verified": True}},
            }
        ],
        expires_at="2000-01-01T00:00:00+00:00",
    )

    removed = storage.cleanup_candidate_cache()

    assert removed == 1
    assert storage.list_pending_candidate_cache(workflow="feed_engage", limit=10) == []


def test_candidate_cache_claims_pending_entries_once(tmp_path: Path) -> None:
    storage = AutomationStorage(tmp_path / "candidate-cache-claim.sqlite3")
    storage.initialize()
    storage.upsert_candidate_cache_entries(
        workflow="feed_engage",
        source_run_id="run_1",
        candidates=[
            {
                "tweet_id": "111",
                "screen_name": "demo1",
                "created_at": "2026-04-20T03:35:55+00:00",
                "text": "cached text 111",
                "metadata": {"id": "111", "text": "cached text 111", "author": {"screenName": "demo1", "verified": True}},
            },
            {
                "tweet_id": "222",
                "screen_name": "demo2",
                "created_at": "2026-04-20T03:36:55+00:00",
                "text": "cached text 222",
                "metadata": {"id": "222", "text": "cached text 222", "author": {"screenName": "demo2", "verified": True}},
            },
        ],
        expires_at="2999-01-01T00:00:00+00:00",
    )

    claimed = storage.claim_pending_candidate_cache(
        workflow="feed_engage",
        limit=2,
        run_id="run_claim_1",
        lease_expires_at="2999-01-01T00:30:00+00:00",
    )
    claimed_again = storage.claim_pending_candidate_cache(
        workflow="feed_engage",
        limit=2,
        run_id="run_claim_2",
        lease_expires_at="2999-01-01T00:30:00+00:00",
    )

    assert [item["tweet_id"] for item in claimed] == ["222", "111"]
    assert claimed_again == []
    assert storage.list_pending_candidate_cache(workflow="feed_engage", limit=10) == []


def test_candidate_cache_release_claimed_entries_returns_to_pending(tmp_path: Path) -> None:
    storage = AutomationStorage(tmp_path / "candidate-cache-release.sqlite3")
    storage.initialize()
    storage.upsert_candidate_cache_entries(
        workflow="feed_engage",
        source_run_id="run_1",
        candidates=[
            {
                "tweet_id": "111",
                "screen_name": "demo1",
                "created_at": "2026-04-20T03:35:55+00:00",
                "text": "cached text 111",
                "metadata": {"id": "111", "text": "cached text 111", "author": {"screenName": "demo1", "verified": True}},
            },
            {
                "tweet_id": "222",
                "screen_name": "demo2",
                "created_at": "2026-04-20T03:36:55+00:00",
                "text": "cached text 222",
                "metadata": {"id": "222", "text": "cached text 222", "author": {"screenName": "demo2", "verified": True}},
            },
        ],
        expires_at="2999-01-01T00:00:00+00:00",
    )

    claimed = storage.claim_pending_candidate_cache(
        workflow="feed_engage",
        limit=2,
        run_id="run_claim_1",
        lease_expires_at="2999-01-01T00:30:00+00:00",
    )
    released = storage.release_claimed_candidate_cache(
        workflow="feed_engage",
        run_id="run_claim_1",
        tweet_ids=["111"],
    )
    reclaimed = storage.claim_pending_candidate_cache(
        workflow="feed_engage",
        limit=1,
        run_id="run_claim_2",
        lease_expires_at="2999-01-01T00:31:00+00:00",
    )

    assert [item["tweet_id"] for item in claimed] == ["222", "111"]
    assert released == 1
    assert [item["tweet_id"] for item in reclaimed] == ["111"]


def test_candidate_cache_cleanup_releases_expired_claims(tmp_path: Path) -> None:
    storage = AutomationStorage(tmp_path / "candidate-cache-claim-expiry.sqlite3")
    storage.initialize()
    storage.upsert_candidate_cache_entries(
        workflow="feed_engage",
        source_run_id="run_1",
        candidates=[
            {
                "tweet_id": "111",
                "screen_name": "demo",
                "created_at": "2026-04-20T03:35:55+00:00",
                "text": "cached text",
                "metadata": {"id": "111", "text": "cached text", "author": {"screenName": "demo", "verified": True}},
            }
        ],
        expires_at="2999-01-01T00:00:00+00:00",
    )

    claimed = storage.claim_pending_candidate_cache(
        workflow="feed_engage",
        limit=1,
        run_id="run_claim_1",
        lease_expires_at="2000-01-01T00:00:00+00:00",
    )
    assert [item["tweet_id"] for item in claimed] == ["111"]

    storage.cleanup_candidate_cache()

    pending = storage.list_pending_candidate_cache(workflow="feed_engage", limit=10)
    assert [item["tweet_id"] for item in pending] == ["111"]


def test_record_engagement_persists_target_and_reply_urls(tmp_path: Path) -> None:
    storage = AutomationStorage(tmp_path / "engagement.sqlite3")
    storage.initialize()
    storage.upsert_job("job_seed", "feed_engage", config={})
    storage.create_run(
        run_id="run_seed",
        job_id="job_seed",
        job_type="feed_engage",
        endpoint="seed",
        request_payload={},
        status="completed",
    )
    storage.record_engagement(
        run_id="run_seed",
        target_tweet_id="111",
        target_author="demoauthor",
        target_tweet_url="https://x.com/demoauthor/status/111",
        reply_tweet_id="222",
        reply_url="https://x.com/i/status/222",
        followed=True,
    )
    conn = sqlite3.connect(tmp_path / "engagement.sqlite3")
    row = conn.execute(
        "SELECT target_tweet_url, reply_url FROM engagements WHERE run_id = 'run_seed'"
    ).fetchone()
    conn.close()
    assert row == ("https://x.com/demoauthor/status/111", "https://x.com/i/status/222")


def test_healthz() -> None:
    with TestClient(app) as client:
        response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_startup_clears_stale_running_runs(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "startup-cleanup.sqlite3"
    monkeypatch.setenv("X_ATUO_DB_PATH", str(db_path))
    monkeypatch.setenv("X_ATUO_SCHEDULER__ENABLED", "false")

    storage = AutomationStorage(db_path)
    storage.initialize()
    storage.upsert_job("job_1", "feed_engage", config={})
    storage.create_run(
        run_id="run_stale",
        job_id="job_1",
        job_type="feed_engage",
        endpoint="scheduler:feed-engage",
        request_payload={"feed_count": 5},
        status="running",
    )
    storage.update_run("run_stale", status="running", started_at="2026-04-20T00:00:00+00:00")

    with TestClient(app):
        run = storage.get_run("run_stale")

    assert run is not None
    assert run["run"]["status"] == "failed"
    assert run["run"]["error"] == "stale running cleared on service startup"
    assert run["run"]["finished_at"] is not None
    assert any(
        event["event_type"] == "stale_running_cleared"
        and event["node"] == "service"
        for event in run["audit_events"]
    )


def test_scheduler_registers_feed_engage_job(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "scheduler.sqlite3"))
    monkeypatch.setenv("X_ATUO_SCHEDULER__ENABLED", "true")
    monkeypatch.setenv("X_ATUO_SCHEDULER__AUTOSTART", "false")
    monkeypatch.setenv("X_ATUO_SCHEDULER__FEED_ENGAGE_ENABLED", "true")
    monkeypatch.setenv("X_ATUO_SCHEDULER__FEED_ENGAGE_TRIGGER", "interval")
    monkeypatch.setenv("X_ATUO_SCHEDULER__FEED_ENGAGE_SECONDS", "300")
    monkeypatch.setenv("X_ATUO_SCHEDULER__FEED_ENGAGE_JITTER_SECONDS", "137")

    with TestClient(app):
        job_ids = app.state.scheduler.list_job_ids()
        assert "scheduled-feed-engage" in job_ids
        assert app.state.scheduled_feed_engage.trigger_args["jitter"] == 137


def test_scheduler_registers_author_alpha_job(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "scheduler.sqlite3"))
    monkeypatch.setenv("X_ATUO_SCHEDULER__ENABLED", "true")
    monkeypatch.setenv("X_ATUO_SCHEDULER__AUTOSTART", "false")
    monkeypatch.setenv("X_ATUO_AUTHOR_ALPHA__ENABLED", "true")
    monkeypatch.setenv("X_ATUO_AUTHOR_ALPHA__TRIGGER", "interval")
    monkeypatch.setenv("X_ATUO_AUTHOR_ALPHA__SECONDS", "180")
    monkeypatch.setenv("X_ATUO_AUTHOR_ALPHA__JITTER_SECONDS", "23")

    with TestClient(app):
        job_ids = app.state.scheduler.list_job_ids()
        assert "scheduled-author-alpha-engage" in job_ids
        assert app.state.scheduled_author_alpha_engage.trigger_args["jitter"] == 23


def test_scheduler_dispatch_creates_author_alpha_execution_run(monkeypatch, tmp_path: Path) -> None:
    from x_atuo.automation.author_alpha_storage import AuthorAlphaStorage

    class FakeReplyClient:
        def reply(self, tweet_id: str, text: str):
            return TwitterCommandResult(
                action="reply",
                ok=True,
                target_tweet_id=tweet_id,
                tweet_id="alpha-reply-1",
                text=text,
            )

    class FakeNotificationsClient:
        def fetch_device_follow_feed(self, *, count: int):
            return {
                "timeline_id": "tweet_notifications",
                "top_cursor": "top-x",
                "bottom_cursor": "bottom-x",
                "posts": [
                    {
                        "id": "tweet-1",
                        "text": "hello world",
                        "created_at": "2026-04-27T00:00:00Z",
                        "url": "https://x.com/alpha_one/status/tweet-1",
                        "author": {
                            "screen_name": "alpha_one",
                            "name": "Alpha One",
                            "rest_id": "rest-alpha-one",
                            "verified": True,
                        },
                        "public_metrics": {"likes": 0, "replies": 0, "reposts": 0, "quotes": 0},
                        "reply_to_id": None,
                    }
                ],
            }

    class FakeDrafter:
        def draft_reply(self, candidate, context=None):
            return "author alpha draft"

    db_path = tmp_path / "scheduler.sqlite3"
    alpha_db_path = tmp_path / "author-alpha.sqlite3"
    monkeypatch.setenv("X_ATUO_DB_PATH", str(db_path))
    monkeypatch.setenv("X_ATUO_AUTHOR_ALPHA__DB_PATH", str(alpha_db_path))
    monkeypatch.setenv("X_ATUO_SCHEDULER__ENABLED", "true")
    monkeypatch.setenv("X_ATUO_SCHEDULER__AUTOSTART", "false")
    monkeypatch.setenv("X_ATUO_AUTHOR_ALPHA__ENABLED", "true")
    monkeypatch.setenv("X_ATUO_AUTHOR_ALPHA__TRIGGER", "interval")
    monkeypatch.setenv("X_ATUO_AUTHOR_ALPHA__SECONDS", "180")
    monkeypatch.setenv("X_ATUO_AUTHOR_ALPHA__INTER_REPLY_DELAY_SECONDS", "0")
    monkeypatch.setenv("X_ATUO_AUTHOR_ALPHA__TARGET_SWITCH_DELAY_SECONDS", "0")
    monkeypatch.setenv("X_ATUO_AI__PROVIDER", "mock")
    monkeypatch.setattr(automation_api.TwitterClient, "from_config", lambda *args, **kwargs: FakeReplyClient())
    monkeypatch.setattr(automation_api.XWebNotificationsClient, "from_settings", lambda *args, **kwargs: FakeNotificationsClient())
    monkeypatch.setattr(automation_api, "build_ai_provider", lambda settings: FakeDrafter())

    alpha_storage = AuthorAlphaStorage(alpha_db_path)
    alpha_storage.initialize()
    alpha_storage.upsert_author(
        screen_name="alpha_one",
        author_name="Alpha One",
        rest_id="rest-alpha-one",
        author_score=100.0,
        reply_count_7d=1,
        impressions_total_7d=100,
        avg_impressions_7d=100.0,
        max_impressions_7d=100,
        last_replied_at=None,
        last_post_seen_at="2026-04-27T00:00:00+00:00",
        last_scored_at="2026-04-27T00:00:00+00:00",
        source="test",
    )

    with TestClient(app):
        definition = app.state.scheduled_author_alpha_engage
        app.state.scheduler._dispatch_job(definition.request)

        deadline = time.monotonic() + 1.0
        row = None
        while time.monotonic() < deadline:
            with alpha_storage.connect() as connection:
                row = connection.execute(
                    "SELECT id, job_type, endpoint, status FROM alpha_runs ORDER BY created_at DESC LIMIT 1"
                ).fetchone()
            if row is not None and row[1:] == ("author_alpha_engage", "scheduler:author-alpha-engage", "completed"):
                break
            time.sleep(0.01)

    assert row is not None
    assert row[1:] == ("author_alpha_engage", "scheduler:author-alpha-engage", "completed")

    run_payload = alpha_storage.get_execution_run(str(row[0]))
    assert run_payload is not None
    assert run_payload["run"]["job_type"] == "author_alpha_engage"
    assert run_payload["run"]["status"] == "completed"
    assert run_payload["run"]["endpoint"] == "scheduler:author-alpha-engage"
    assert alpha_storage.get_target_success_count("tweet-1") == 2


def test_scheduler_dispatch_queue_processes_jobs_serially() -> None:
    lock = threading.Lock()
    processed: list[str] = []
    active = 0
    max_active = 0
    completed = threading.Event()

    async def dispatcher(request: AutomationRequest) -> None:
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        await asyncio.sleep(0.15)
        with lock:
            processed.append(request.job_name or "")
            active -= 1
            if len(processed) == 2:
                completed.set()

    scheduler = AutomationScheduler(SchedulerSettings(enabled=True, autostart=False), dispatcher)
    try:
        started_at = time.monotonic()
        scheduler._dispatch_job(AutomationRequest.for_feed_engage(job_name="job-1"))
        scheduler._dispatch_job(AutomationRequest.for_feed_engage(job_name="job-2"))
        enqueue_elapsed = time.monotonic() - started_at

        assert enqueue_elapsed < 0.1
        assert completed.wait(timeout=1.0)
        assert processed == ["job-1", "job-2"]
        assert max_active == 1
    finally:
        scheduler.shutdown(wait=True)


def test_scheduler_dispatch_queue_survives_dispatcher_exception() -> None:
    processed: list[str] = []
    completed = threading.Event()

    async def dispatcher(request: AutomationRequest) -> None:
        if request.job_name == "job-1":
            raise RuntimeError("boom")
        processed.append(request.job_name or "")
        completed.set()

    scheduler = AutomationScheduler(SchedulerSettings(enabled=True, autostart=False), dispatcher)
    try:
        scheduler._dispatch_job(AutomationRequest.for_feed_engage(job_name="job-1"))
        scheduler._dispatch_job(AutomationRequest.for_feed_engage(job_name="job-2"))

        assert completed.wait(timeout=1.0)
        assert processed == ["job-2"]
    finally:
        scheduler.shutdown(wait=True)


def test_scheduler_drops_jobs_when_backlog_is_full() -> None:
    processed: list[str] = []
    release = threading.Event()
    completed = threading.Event()

    async def dispatcher(request: AutomationRequest) -> None:
        if request.job_name == "job-1":
            release.wait(timeout=1.0)
        processed.append(request.job_name or "")
        if len(processed) == 2:
            completed.set()

    scheduler = AutomationScheduler(SchedulerSettings(enabled=True, autostart=False), dispatcher)
    try:
        scheduler._dispatch_job(AutomationRequest.for_feed_engage(job_name="job-1"))
        scheduler._dispatch_job(AutomationRequest.for_feed_engage(job_name="job-2"))
        scheduler._dispatch_job(AutomationRequest.for_feed_engage(job_name="job-3"))

        assert scheduler._dispatch_queue.qsize() <= 5
        release.set()
        assert completed.wait(timeout=1.0)
        assert processed == ["job-1", "job-2"]
    finally:
        scheduler.shutdown(wait=True)


def test_execute_job_marks_run_failed_when_graph_raises(tmp_path: Path, monkeypatch) -> None:
    storage = AutomationStorage(tmp_path / "execute-job-failed.sqlite3")
    storage.initialize()

    async def fake_call_graph(*args, **kwargs):
        raise RuntimeError("graph exploded")

    monkeypatch.setattr("x_atuo.automation.api._call_graph", fake_call_graph)

    result = asyncio.run(
        automation_api._execute_job(
            storage=storage,
            endpoint="scheduler:feed-engage",
            job_type="feed_engage",
            function_name="run_feed_engage",
            payload={"dry_run": True},
            requested_job_id="scheduled-feed-engage",
        )
    )

    assert result["status"] == "failed"
    run = storage.get_run(result["run_id"])
    assert run is not None
    assert run["run"]["status"] == "failed"
    assert run["run"]["finished_at"] is not None
    assert run["run"]["error"] == "graph exploded"


def test_dispatch_scheduled_request_uses_request_directly_without_call_graph(tmp_path: Path, monkeypatch) -> None:
    storage = AutomationStorage(tmp_path / "scheduler-direct-request.sqlite3")
    storage.initialize()

    request = AutomationRequest.for_feed_engage(
        job_name="scheduled-feed-engage",
        dry_run=False,
        approval_mode="ai_auto",
        metadata={"proxy": "http://proxy.example:8080", "trigger": "scheduler"},
    )

    async def fail_call_graph(*args, **kwargs):
        raise AssertionError("_call_graph should not be used by the scheduler production path")

    async def fake_run_request(request_obj, *, storage, endpoint, proxy=None, observability_runtime=None):
        assert request_obj.workflow is WorkflowKind.FEED_ENGAGE
        assert request_obj.job_name == "scheduled-feed-engage"
        assert request_obj.run_id is not None
        assert endpoint == "scheduler:feed-engage"
        assert proxy == "http://proxy.example:8080"
        return {"status": "completed", "run_id": request_obj.run_id, "result": None}

    monkeypatch.setattr("x_atuo.automation.api._call_graph", fail_call_graph)
    monkeypatch.setattr("x_atuo.automation.api._run_request", fake_run_request)

    result = asyncio.run(automation_api._dispatch_scheduled_request(request, storage))

    assert result["status"] == "completed"
    run = storage.get_run(result["run_id"])
    assert run is not None
    assert run["run"]["status"] == "completed"
    assert run["run"]["endpoint"] == "scheduler:feed-engage"


def test_record_dropped_scheduled_request_marks_run_blocked(tmp_path: Path) -> None:
    storage = AutomationStorage(tmp_path / "scheduler-dropped.sqlite3")
    storage.initialize()

    request = AutomationRequest.for_feed_engage(
        job_name="scheduled-feed-engage",
        dry_run=False,
    )

    run_id = automation_api._record_dropped_scheduled_request(
        request,
        storage,
        reason="scheduler backlog full",
    )

    run = storage.get_run(run_id)
    assert run is not None
    assert run["run"]["status"] == "blocked"
    assert run["run"]["error"] == "scheduler backlog full"
    assert any(event["event_type"] == "scheduler_queue_dropped" for event in run["audit_events"])


def test_scheduler_dispatch_creates_feed_engage_run(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str = "demo", verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {"id": tweet_id, "text": text}
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeCommand:
        def __init__(self, action: str, ok: bool = True, tweet_id: str | None = None, dry_run: bool = False):
            self.action = action
            self.ok = ok
            self.dry_run = dry_run
            self.target_tweet_id = "111"
            self.tweet_id = tweet_id
            self.screen_name = "demoauthor"
            self.text = "hello"
            self.payload = {"ok": ok}
            self.error_code = None
            self.error_message = None

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet("111", "demoauthor")]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "demoauthor")

        def reply(self, tweet_id: str, text: str):
            return FakeCommand("reply", ok=True, tweet_id="reply_sched", dry_run=False)

        def follow(self, screen_name: str):
            return FakeCommand("follow", ok=True, dry_run=False)

    db_path = tmp_path / "scheduler-run.sqlite3"
    monkeypatch.setenv("X_ATUO_DB_PATH", str(db_path))
    monkeypatch.setenv("X_ATUO_SCHEDULER__ENABLED", "true")
    monkeypatch.setenv("X_ATUO_SCHEDULER__AUTOSTART", "false")
    monkeypatch.setenv("X_ATUO_SCHEDULER__FEED_ENGAGE_ENABLED", "true")
    monkeypatch.setenv("X_ATUO_SCHEDULER__FEED_ENGAGE_TRIGGER", "interval")
    monkeypatch.setenv("X_ATUO_SCHEDULER__FEED_ENGAGE_SECONDS", "300")
    monkeypatch.setenv("X_ATUO_AI__PROVIDER", "mock")
    monkeypatch.setattr(
        "x_atuo.automation.graph.TwitterClient.from_config",
        lambda *args, **kwargs: FakeClient(),
    )

    with TestClient(app):
        definition = app.state.scheduled_feed_engage
        app.state.scheduler._dispatch_job(definition.request)

        deadline = time.monotonic() + 1.0
        row = None
        while time.monotonic() < deadline:
            conn = sqlite3.connect(db_path)
            row = conn.execute(
                "SELECT job_type, endpoint, status FROM runs ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            conn.close()
            if row == ("feed_engage", "scheduler:feed-engage", "completed"):
                break
            time.sleep(0.01)

    assert row == ("feed_engage", "scheduler:feed-engage", "completed")


def test_scheduler_dispatch_fails_without_ai_provider(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str = "demo", verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {"id": tweet_id, "text": text}
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet("111", "demoauthor")]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "demoauthor")

    db_path = tmp_path / "scheduler-run-no-ai.sqlite3"
    monkeypatch.setenv("X_ATUO_DB_PATH", str(db_path))
    monkeypatch.setenv("X_ATUO_SCHEDULER__ENABLED", "true")
    monkeypatch.setenv("X_ATUO_SCHEDULER__AUTOSTART", "false")
    monkeypatch.setenv("X_ATUO_SCHEDULER__FEED_ENGAGE_ENABLED", "true")
    monkeypatch.setenv("X_ATUO_SCHEDULER__FEED_ENGAGE_TRIGGER", "interval")
    monkeypatch.setenv("X_ATUO_SCHEDULER__FEED_ENGAGE_SECONDS", "300")
    monkeypatch.setenv("X_ATUO_AI__PROVIDER", "none")
    monkeypatch.setattr(
        "x_atuo.automation.graph.TwitterClient.from_config",
        lambda *args, **kwargs: FakeClient(),
    )

    with TestClient(app):
        definition = app.state.scheduled_feed_engage
        app.state.scheduler._dispatch_job(definition.request)

        deadline = time.monotonic() + 1.0
        row = None
        while time.monotonic() < deadline:
            conn = sqlite3.connect(db_path)
            row = conn.execute(
                "SELECT job_type, endpoint, status, error FROM runs ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            conn.close()
            if row is not None and row[2] == "failed":
                break
            time.sleep(0.01)

    assert row == (
        "feed_engage",
        "scheduler:feed-engage",
        "failed",
        "feed-engage requires an AI provider",
    )


def test_scheduler_dispatch_persists_reply_restriction_reason_in_run_payload(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str = "demo", verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {"id": tweet_id, "text": text}
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified
            self.can_reply = False
            self.reply_limit_reason = "Only some accounts can reply."
            self.reply_limit_headline = "Who can reply?"
            self.reply_restriction_policy = "Co"

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet("111", "demoauthor")]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "demoauthor")

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            raise AssertionError("prefilter exhaustion should block before AI moderation")

        def classify_reply_style(self, candidate):
            raise AssertionError("drafting should not run for blocked scheduler executions")

        def draft_reply(self, candidate, context=None):
            raise AssertionError("drafting should not run for blocked scheduler executions")

    db_path = tmp_path / "scheduler-reply-restriction.sqlite3"
    monkeypatch.setenv("X_ATUO_DB_PATH", str(db_path))
    monkeypatch.setenv("X_ATUO_SCHEDULER__ENABLED", "true")
    monkeypatch.setenv("X_ATUO_SCHEDULER__AUTOSTART", "false")
    monkeypatch.setenv("X_ATUO_SCHEDULER__FEED_ENGAGE_ENABLED", "true")
    monkeypatch.setenv("X_ATUO_SCHEDULER__FEED_ENGAGE_TRIGGER", "interval")
    monkeypatch.setenv("X_ATUO_SCHEDULER__FEED_ENGAGE_SECONDS", "300")
    monkeypatch.setenv("X_ATUO_POLICIES__CANDIDATE_REFRESH_ROUNDS", "0")
    monkeypatch.setattr(
        "x_atuo.automation.graph.TwitterClient.from_config",
        lambda *args, **kwargs: FakeClient(),
    )
    monkeypatch.setattr(
        "x_atuo.automation.graph.build_ai_provider",
        lambda settings: FakeAIProvider(),
    )

    with TestClient(app):
        definition = app.state.scheduled_feed_engage
        storage = app.state.storage
        app.state.scheduler._dispatch_job(definition.request)

        deadline = time.monotonic() + 1.0
        run_id = None
        while time.monotonic() < deadline:
            conn = sqlite3.connect(db_path)
            row = conn.execute(
                "SELECT id, status FROM runs ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            conn.close()
            if row is not None and row[1] == "blocked":
                run_id = row[0]
                break
            time.sleep(0.01)

        assert run_id is not None
        run = storage.get_run(run_id)

    assert run is not None
    assert run["run"]["status"] == "blocked"
    assert run["run"]["error"] == "Only some accounts can reply."
    assert run["run"]["response_payload"]["errors"] == ["Only some accounts can reply."]
    assert any(
        event["node"] == "prefilter_candidates"
        and event["message"] == "candidate removed before selection"
        and event["payload"]["reason"] == "Only some accounts can reply."
        for event in run["run"]["response_payload"]["events"]
    )
    assert any(
        event["event_type"] == "orchestration_finished"
        and event["payload"]["status"] == "blocked"
        for event in run["audit_events"]
    )
















def test_approve_route_removed() -> None:
    with TestClient(app) as client:
        response = client.post("/runs/does-not-exist/approve")
    assert response.status_code == 404


def test_explicit_engage_route_removed() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/explicit-engage",
            json={"tweet_id": "123", "reply_text": "hello"},
        )
    assert response.status_code == 404


def test_scheduler_first_api_removes_webhook_write_routes() -> None:
    with TestClient(app) as client:
        assert client.post("/hooks/twitter/feed-engage", json={}).status_code == 404
        assert client.post("/hooks/twitter/repo-post", json={}).status_code == 404
        assert client.post("/hooks/twitter/direct-post", json={}).status_code == 404


def test_explicit_engage_workflow_removed_from_state_model() -> None:
    assert "EXPLICIT_ENGAGE" not in WorkflowKind.__members__
    assert not hasattr(AutomationRequest, "for_explicit_engage")


def test_scheduler_first_service_keeps_only_feed_engage_workflow_surface() -> None:
    assert "FEED_ENGAGE" in WorkflowKind.__members__
    assert not hasattr(AutomationRequest, "for_repo_post")
    assert not hasattr(AutomationRequest, "for_direct_post")


def test_author_alpha_sync_status_route_returns_latest_state(monkeypatch) -> None:
    class FakeManager:
        def get_status(self) -> dict[str, object]:
            return {
                "active": False,
                "bootstrap_required": True,
                "active_run": None,
                "latest_run": {
                    "run_id": "bootstrap-1",
                    "run_type": "bootstrap",
                    "status": "completed",
                    "from_date": "2026-04-01",
                    "to_date": "2026-04-07",
                    "current_date": "2026-04-07",
                    "days_completed": 7,
                    "days_total": 7,
                    "resume_from_date": None,
                    "error": None,
                    "created_at": "2026-04-07T00:00:00+00:00",
                    "started_at": "2026-04-07T00:00:00+00:00",
                    "finished_at": "2026-04-07T00:10:00+00:00",
                },
                "bootstrap_checkpoint": {
                    "sync_scope": "bootstrap",
                    "last_completed_date": "2026-04-07",
                    "next_pending_date": None,
                    "last_run_id": "bootstrap-1",
                    "updated_at": "2026-04-07T00:10:00+00:00",
                },
                "reconcile_checkpoint": None,
            }

        def list_history(self, *, limit: int = 20):
            raise AssertionError("history route should not be called")

        def get_run(self, run_id: str):
            raise AssertionError(f"run lookup should not be called for {run_id}")

    monkeypatch.setattr(automation_api, "_build_author_alpha_sync_manager", lambda settings, storage: FakeManager())

    with TestClient(app) as client:
        response = client.get("/author-alpha/sync/status")

    assert response.status_code == 200
    assert response.json()["bootstrap_required"] is True
    assert response.json()["latest_run"]["run_id"] == "bootstrap-1"


def test_author_alpha_bootstrap_route_starts_background_run(monkeypatch) -> None:
    started: dict[str, object] = {}

    class FakeManager:
        def start_bootstrap(self, **kwargs) -> dict[str, object]:
            started.update(kwargs)
            return {
                "run_id": "bootstrap-queued",
                "run_type": "bootstrap",
                "status": "accepted",
                "from_date": kwargs["from_date"],
                "to_date": kwargs["to_date"],
                "resume": kwargs.get("resume", False),
                "max_days": kwargs.get("max_days"),
            }

        def get_status(self) -> dict[str, object]:
            raise AssertionError("status route should not be called")

        def list_history(self, *, limit: int = 20):
            raise AssertionError("history route should not be called")

        def get_run(self, run_id: str):
            raise AssertionError(f"run lookup should not be called for {run_id}")

    monkeypatch.setattr(automation_api, "_build_author_alpha_sync_manager", lambda settings, storage: FakeManager())

    with TestClient(app) as client:
        response = client.post(
            "/author-alpha/sync/bootstrap",
            json={"from_date": "2026-04-01", "to_date": "2026-04-07", "resume": True, "max_days": 2},
        )

    assert response.status_code == 202
    assert started == {
        "from_date": "2026-04-01",
        "to_date": "2026-04-07",
        "resume": True,
        "max_days": 2,
    }
    assert response.json()["run_id"] == "bootstrap-queued"


def test_author_alpha_sync_routes_reject_second_active_sync(monkeypatch) -> None:
    class FakeManager:
        def start_bootstrap(self, **kwargs) -> dict[str, object]:
            return {
                "run_id": "bootstrap-queued",
                "run_type": "bootstrap",
                "status": "accepted",
                "from_date": kwargs["from_date"],
                "to_date": kwargs["to_date"],
                "resume": kwargs.get("resume", False),
                "max_days": kwargs.get("max_days"),
            }

        def start_reconcile(self, **kwargs) -> dict[str, object]:
            raise automation_api.AuthorAlphaSyncActiveError("author-alpha sync run already active")

        def get_status(self) -> dict[str, object]:
            raise AssertionError("status route should not be called")

        def list_history(self, *, limit: int = 20):
            raise AssertionError("history route should not be called")

        def get_run(self, run_id: str):
            raise AssertionError(f"run lookup should not be called for {run_id}")

    monkeypatch.setattr(automation_api, "_build_author_alpha_sync_manager", lambda settings, storage: FakeManager())

    with TestClient(app) as client:
        first = client.post("/author-alpha/sync/bootstrap", json={"from_date": "2026-04-01", "to_date": "2026-04-07"})
        second = client.post("/author-alpha/sync/reconcile", json={})

    assert first.status_code == 202
    assert second.status_code == 409


def test_author_alpha_sync_stop_route_requests_cancellation(monkeypatch) -> None:
    class FakeManager:
        def stop_active_run(self) -> dict[str, object]:
            return {
                "run_id": "bootstrap-queued",
                "run_type": "bootstrap",
                "status": "stop_requested",
            }

        def get_status(self) -> dict[str, object]:
            raise AssertionError("status route should not be called")

        def start_bootstrap(self, **kwargs) -> dict[str, object]:
            raise AssertionError(f"bootstrap route should not be called: {kwargs}")

        def start_reconcile(self, **kwargs) -> dict[str, object]:
            raise AssertionError(f"reconcile route should not be called: {kwargs}")

        def list_history(self, *, limit: int = 20):
            raise AssertionError("history route should not be called")

        def get_run(self, run_id: str):
            raise AssertionError(f"run lookup should not be called for {run_id}")

    monkeypatch.setattr(automation_api, "_build_author_alpha_sync_manager", lambda settings, storage: FakeManager())

    with TestClient(app) as client:
        response = client.post("/author-alpha/sync/stop")

    assert response.status_code == 202
    assert response.json()["status"] == "stop_requested"


def test_author_alpha_sync_history_and_run_lookup_routes(monkeypatch) -> None:
    run_payload = {
        "run_id": "reconcile-1",
        "run_type": "reconcile",
        "status": "completed",
        "from_date": "2026-04-26",
        "to_date": "2026-04-26",
        "current_date": "2026-04-26",
        "days_completed": 1,
        "days_total": 1,
        "resume_from_date": None,
        "error": None,
        "created_at": "2026-04-27T00:00:00+00:00",
        "started_at": "2026-04-27T00:00:00+00:00",
        "finished_at": "2026-04-27T00:01:00+00:00",
    }

    class FakeManager:
        def get_status(self) -> dict[str, object]:
            raise AssertionError("status route should not be called")

        def start_bootstrap(self, **kwargs) -> dict[str, object]:
            raise AssertionError(f"bootstrap route should not be called: {kwargs}")

        def start_reconcile(self, **kwargs) -> dict[str, object]:
            raise AssertionError(f"reconcile route should not be called: {kwargs}")

        def list_history(self, *, limit: int = 20):
            assert limit == 5
            return [run_payload]

        def get_run(self, run_id: str):
            return run_payload if run_id == "reconcile-1" else None

    monkeypatch.setattr(automation_api, "_build_author_alpha_sync_manager", lambda settings, storage: FakeManager())

    with TestClient(app) as client:
        history = client.get("/author-alpha/sync/history", params={"limit": 5})
        found = client.get("/author-alpha/runs/reconcile-1")
        missing = client.get("/author-alpha/runs/does-not-exist")

    assert history.status_code == 200
    assert history.json()["runs"][0]["run_id"] == "reconcile-1"
    assert found.status_code == 200
    assert found.json()["run"]["run_id"] == "reconcile-1"
    assert missing.status_code == 404


def test_author_alpha_reset_route_clears_data(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "author-alpha.sqlite3"
    monkeypatch.setenv("X_ATUO_AUTHOR_ALPHA__DB_PATH", str(db_path))
    monkeypatch.setenv("X_ATUO_SCHEDULER__ENABLED", "false")

    from x_atuo.automation.author_alpha_storage import AuthorAlphaStorage

    storage = AuthorAlphaStorage(db_path)
    storage.initialize()
    storage.upsert_author(
        screen_name="alpha_one",
        author_name="Alpha One",
        rest_id="rest-alpha-one",
        author_score=100.0,
        reply_count_7d=1,
        impressions_total_7d=100,
        avg_impressions_7d=100.0,
        max_impressions_7d=100,
        last_replied_at=None,
        last_post_seen_at="2026-04-27T00:00:00+00:00",
        last_scored_at="2026-04-27T00:00:00+00:00",
        source="test",
    )
    storage.record_sync_run(
        run_id="bootstrap-1",
        run_type="bootstrap",
        status="failed",
        from_date="2026-04-21",
        to_date="2026-04-27",
        current_date="2026-04-21",
        days_completed=0,
        days_total=7,
        resume_from_date="2026-04-21",
        error="boom",
    )
    storage.record_engagement(
        run_id="alpha-run-1",
        target_author="alpha_one",
        target_tweet_id="tweet-1",
        target_tweet_url=None,
        reply_tweet_id="reply-1",
        reply_url=None,
        created_at="2026-04-27T00:00:00+00:00",
    )

    with TestClient(app) as client:
        response = client.post("/author-alpha/reset")

    assert response.status_code == 200
    assert response.json()["status"] == "cleared"
    assert storage.count_authors() == 0
    assert storage.count_reply_daily_metrics() == 0
    assert storage.count_author_daily_rollups() == 0
    assert storage.get_active_sync_run() is None
    assert storage.list_sync_runs(limit=10) == []


def test_author_alpha_run_lookup_returns_execution_run(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "author-alpha.sqlite3"
    monkeypatch.setenv("X_ATUO_AUTHOR_ALPHA__DB_PATH", str(db_path))
    monkeypatch.setenv("X_ATUO_SCHEDULER__ENABLED", "false")

    from x_atuo.automation.author_alpha_storage import AuthorAlphaStorage

    storage = AuthorAlphaStorage(db_path)
    storage.initialize()
    storage.create_execution_run(
        run_id="alpha-run-1",
        job_id="scheduled-author-alpha-engage",
        job_type="author_alpha_engage",
        endpoint="scheduler:author-alpha-engage",
        request_payload={"dry_run": False},
        status="running",
    )
    storage.add_execution_audit_event(
        run_id="alpha-run-1",
        event_type="trigger_received",
        node="service",
        payload={"endpoint": "scheduler:author-alpha-engage"},
    )
    storage.update_execution_run(
        "alpha-run-1",
        status="completed",
        response_payload={"status": "completed"},
        started_at="2026-04-27T00:00:00+00:00",
        finished_at="2026-04-27T00:01:00+00:00",
    )

    with TestClient(app) as client:
        response = client.get("/author-alpha/runs/alpha-run-1")

    assert response.status_code == 200
    assert response.json()["run"]["id"] == "alpha-run-1"
    assert response.json()["audit_events"][0]["event_type"] == "trigger_received"


def test_author_alpha_manual_execute_route_runs_once_without_scheduler(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "author-alpha.sqlite3"
    monkeypatch.setenv("X_ATUO_AUTHOR_ALPHA__DB_PATH", str(db_path))
    monkeypatch.setenv("X_ATUO_SCHEDULER__ENABLED", "false")
    monkeypatch.setenv("X_ATUO_AUTHOR_ALPHA__ENABLED", "true")
    monkeypatch.setenv("X_ATUO_AUTHOR_ALPHA__INTER_REPLY_DELAY_SECONDS", "0")
    monkeypatch.setenv("X_ATUO_AUTHOR_ALPHA__TARGET_SWITCH_DELAY_SECONDS", "0")
    monkeypatch.setenv("X_ATUO_AI__PROVIDER", "mock")

    from x_atuo.automation.author_alpha_storage import AuthorAlphaStorage

    class FakeReplyClient:
        def reply(self, tweet_id: str, text: str):
            return TwitterCommandResult(
                action="reply",
                ok=True,
                target_tweet_id=tweet_id,
                tweet_id="alpha-reply-1",
                text=text,
            )

    class FakeNotificationsClient:
        def fetch_device_follow_feed(self, *, count: int):
            return {
                "timeline_id": "tweet_notifications",
                "top_cursor": "top-x",
                "bottom_cursor": "bottom-x",
                "posts": [
                    {
                        "id": "tweet-1",
                        "text": "hello world",
                        "created_at": "2026-04-27T00:00:00Z",
                        "url": "https://x.com/alpha_one/status/tweet-1",
                        "author": {
                            "screen_name": "alpha_one",
                            "name": "Alpha One",
                            "rest_id": "rest-alpha-one",
                            "verified": True,
                        },
                        "public_metrics": {"likes": 0, "replies": 0, "reposts": 0, "quotes": 0},
                        "reply_to_id": None,
                    }
                ],
            }

    class FakeDrafter:
        def draft_reply(self, candidate, context=None):
            return "author alpha draft"

    monkeypatch.setattr(automation_api.TwitterClient, "from_config", lambda *args, **kwargs: FakeReplyClient())
    monkeypatch.setattr(automation_api.XWebNotificationsClient, "from_settings", lambda *args, **kwargs: FakeNotificationsClient())
    monkeypatch.setattr(automation_api, "build_ai_provider", lambda settings: FakeDrafter())

    storage = AuthorAlphaStorage(db_path)
    storage.initialize()
    storage.upsert_author(
        screen_name="alpha_one",
        author_name="Alpha One",
        rest_id="rest-alpha-one",
        author_score=100.0,
        reply_count_7d=1,
        impressions_total_7d=100,
        avg_impressions_7d=100.0,
        max_impressions_7d=100,
        last_replied_at=None,
        last_post_seen_at="2026-04-27T00:00:00+00:00",
        last_scored_at="2026-04-27T00:00:00+00:00",
        source="test",
    )

    with TestClient(app) as client:
        response = client.post("/author-alpha/execute", json={"dry_run": True})

    assert response.status_code == 200
    payload = response.json()
    assert payload["job_type"] == "author_alpha_engage"
    assert payload["endpoint"] == "manual:author-alpha-engage"
    assert payload["status"] in {"completed", "failed", "skipped"}


def test_feed_engage_manual_execute_route_runs_once_without_scheduler(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "manual-feed.sqlite3"
    monkeypatch.setenv("X_ATUO_DB_PATH", str(db_path))
    monkeypatch.setenv("X_ATUO_SCHEDULER__ENABLED", "false")

    storage = AutomationStorage(db_path)
    storage.initialize()

    async def fake_run_request(request_obj, *, storage, endpoint, proxy=None, observability_runtime=None):
        assert request_obj.workflow is WorkflowKind.FEED_ENGAGE
        assert request_obj.job_name == "manual-feed-engage"
        assert request_obj.feed_options is not None
        assert request_obj.feed_options.feed_count == 3
        assert request_obj.feed_options.feed_type == "following"
        assert request_obj.reply_text == "hello"
        assert endpoint == "manual:feed-engage"
        assert proxy == "http://127.0.0.1:7890"
        return {"status": "completed", "run_id": request_obj.run_id, "result": {"ok": True}}

    monkeypatch.setattr("x_atuo.automation.api._run_request", fake_run_request)

    with TestClient(app) as client:
        response = client.post(
            "/feed-engage/execute",
            json={"dry_run": True, "reply_text": "hello", "feed_count": 3, "feed_type": "following"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["job_type"] == "feed_engage"
    assert payload["endpoint"] == "manual:feed-engage"
    assert payload["status"] == "completed"


def test_author_alpha_manual_execute_route_rejects_removed_override_fields(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "author-alpha.sqlite3"
    monkeypatch.setenv("X_ATUO_AUTHOR_ALPHA__DB_PATH", str(db_path))
    monkeypatch.setenv("X_ATUO_SCHEDULER__ENABLED", "false")
    monkeypatch.setenv("X_ATUO_AUTHOR_ALPHA__ENABLED", "true")

    with TestClient(app) as client:
        response = client.post(
            "/author-alpha/execute",
            json={"dry_run": True, "reply_text": "manual", "max_targets_per_run": 1, "burst_limit": 1},
        )

    assert response.status_code == 422


def test_run_author_alpha_request_passes_real_asyncio_sleep(monkeypatch, tmp_path: Path) -> None:
    from x_atuo.automation.author_alpha_storage import AuthorAlphaStorage
    from x_atuo.automation.state import ExecutionResult, RunStatus, WorkflowStateModel

    class FakeNotificationsClient:
        pass

    class FakeReplyClient:
        pass

    class FakeDrafter:
        pass

    captured: dict[str, object] = {}

    async def fake_run_author_alpha_engage(
        request,
        *,
        config,
        storage,
        candidate_source,
        drafter,
        reply_client,
        sleep=None,
        now=None,
    ):
        captured["sleep"] = sleep
        return WorkflowStateModel(
            run_id=request.run_id or "run-1",
            request=request,
            status=RunStatus.COMPLETED,
            result=ExecutionResult(action="author_alpha_engage", ok=True, dry_run=True),
        )

    monkeypatch.setattr(automation_api.XWebNotificationsClient, "from_settings", lambda *args, **kwargs: FakeNotificationsClient())
    monkeypatch.setattr(automation_api.TwitterClient, "from_config", lambda *args, **kwargs: FakeReplyClient())
    monkeypatch.setattr(automation_api, "build_ai_provider", lambda settings: FakeDrafter())
    monkeypatch.setattr(automation_api, "run_author_alpha_engage", fake_run_author_alpha_engage)

    storage = AuthorAlphaStorage(tmp_path / "author-alpha.sqlite3")
    storage.initialize()
    request = AutomationRequest.for_author_alpha_engage(job_name="manual-author-alpha-engage", dry_run=True)

    result = asyncio.run(
        automation_api._run_author_alpha_request(
            request,
            settings=AutomationConfig(),
            storage=storage,
            endpoint="manual:author-alpha-engage",
        )
    )

    assert result["status"] == "completed"
    assert captured["sleep"] is asyncio.sleep


def test_openai_compatible_provider_parses_fenced_json() -> None:
    provider = OpenAICompatibleProvider(
        AISettings(
            provider="openai_compatible",
            model="demo-model",
            api_key="demo-key",
            base_url="https://example.com/v1",
        )
    )
    provider._chat = lambda system, user: """```json
{"text":"hello world","rationale":"because"}
```"""  # type: ignore[method-assign]

    result = provider.draft_reply(type("Candidate", (), {"model_dump": lambda self, mode="json": {"tweet_id": "1"}})())

    assert result.text == "hello world"
    assert result.rationale == "because"


def test_openai_compatible_provider_uses_statement_style_reply_prompt() -> None:
    provider = OpenAICompatibleProvider(
        AISettings(
            provider="openai_compatible",
            model="demo-model",
            api_key="demo-key",
            base_url="https://example.com/v1",
        )
    )
    prompts: list[tuple[str, str]] = []

    def fake_chat(system: str, user: str) -> str:
        prompts.append((system, user))
        return '{"text":"Solid breakdown. The tradeoff here feels pretty clear.","rationale":"statement style"}'

    provider._chat = fake_chat  # type: ignore[method-assign]

    result = provider.draft_reply(type("Candidate", (), {"model_dump": lambda self, mode="json": {"tweet_id": "1"}})())

    assert result.text == "Solid breakdown. The tradeoff here feels pretty clear."
    assert prompts[0][0] == (
        "Draft one short technical Twitter reply under 100 chars. "
        "Write like a sharp practitioner, not a summarizer. Lead with a judgment, useful angle, or tension. "
        "Keep it conversational and plainspoken. Prefer a direct statement, not a question. "
        "Avoid generic praise, repetition, and restating the post. One or two short sentences. No lists. No emojis. Return JSON with text and rationale."
    )


def test_openai_compatible_provider_uses_candidate_moderation_prompt() -> None:
    provider = OpenAICompatibleProvider(
        AISettings(
            provider="openai_compatible",
            model="demo-model",
            api_key="demo-key",
            base_url="https://example.com/v1",
        )
    )
    prompts: list[tuple[str, str]] = []

    def fake_chat(system: str, user: str) -> str:
        prompts.append((system, user))
        return '{"results":[{"tweet_id":"1","allowed":true,"category":null,"reason":"technical content"}]}'

    provider._chat = fake_chat  # type: ignore[method-assign]

    results = provider.moderate_candidates(
        [type("Candidate", (), {"model_dump": lambda self, mode="json": {"tweet_id": "1", "text": "langgraph guide", "metadata": {"_x_atuo_runtime": True, "topic": "dev"}}})()]
    )

    assert results[0].tweet_id == "1"
    assert results[0].allowed is True
    assert prompts[0][0] == (
        "Review Twitter feed candidates for reply safety. Reject anything about crime, violence, fraud, scams, drugs, war, military conflict, law enforcement, case news, adult or NSFW content, hate or harassment, self-harm or dangerous behavior, gambling or illicit activity, extremism, crypto shilling or guaranteed-profit investment claims, and medical or legal high-risk advice. Allow technical, product, engineering, builder, developer-adjacent, pets and animals, lifestyle, food, travel, scenic photography, entertainment, memes, and casual social content. Always allow posts from @elonmusk. Return JSON with a results array of {tweet_id, allowed, category, reason}."
    )
    payload = __import__("json").loads(prompts[0][1])
    assert payload == [{"tweet_id": "1", "text": "langgraph guide"}]


def test_moderation_cache_key_changes_when_non_text_payload_changes() -> None:
    candidate = FeedCandidate(
        tweet_id="111",
        screen_name="builder",
        text="same text",
        author_verified=True,
        metadata={"id": "111", "author": {"screenName": "builder", "verified": True}, "media": [{"type": "photo"}]},
    )
    original_key = build_moderation_cache_key(candidate, provider="openai_compatible", model="demo-model")

    candidate.metadata = {"id": "111", "author": {"screenName": "builder", "verified": True}, "media": [{"type": "video"}]}
    updated_key = build_moderation_cache_key(candidate, provider="openai_compatible", model="demo-model")

    assert updated_key != original_key




def test_openai_compatible_provider_uses_compact_draft_reply_payload_with_media_types() -> None:
    provider = OpenAICompatibleProvider(
        AISettings(
            provider="openai_compatible",
            model="demo-model",
            api_key="demo-key",
            base_url="https://example.com/v1",
        )
    )
    prompts: list[tuple[str, str]] = []

    def fake_chat(system: str, user: str) -> str:
        prompts.append((system, user))
        return '{"text":"Infra bottlenecks show up fast on video-heavy flows.","rationale":"compact payload"}'

    provider._chat = fake_chat  # type: ignore[method-assign]

    result = provider.draft_reply(
        type(
            "Candidate",
            (),
            {
                "model_dump": lambda self, mode="json": {
                    "tweet_id": "1",
                    "screen_name": "builder",
                    "text": "preview text",
                    "created_at": "2026-04-20T13:26:19Z",
                    "author_verified": True,
                    "metadata": {
                        "media": [
                            {"type": "video", "url": "https://example.com/video.mp4"},
                            {"type": "photo", "url": "https://example.com/image.jpg"},
                            {"type": "video", "url": "https://example.com/video-2.mp4"},
                        ],
                        "raw": "should_not_be_sent",
                    },
                }
            },
        )()
    )

    payload = __import__("json").loads(prompts[0][1])
    assert result.text == "Infra bottlenecks show up fast on video-heavy flows."
    assert payload["candidate"] == {
        "tweet_id": "1",
        "screen_name": "builder",
        "text": "preview text",
        "created_at": "2026-04-20T13:26:19Z",
        "author_verified": True,
        "media_types": ["video", "photo"],
    }
    assert "metadata" not in payload["candidate"]


def test_twitter_read_routes_use_core_client(monkeypatch) -> None:
    class FakeClient:
        def fetch_search(self, query: str, *, max_items: int = 20, product: str = "Top"):
            assert query == "openai"
            assert max_items == 2
            assert product == "Latest"
            return [FeedCandidate(tweet_id="tweet-search", screen_name="author_one", text="search result").model_dump()]

        def fetch_bookmarks(self, *, max_items: int = 50):
            assert max_items == 3
            return [FeedCandidate(tweet_id="tweet-bookmark", screen_name="author_one", text="bookmark result").model_dump()]

        def fetch_bookmark_folders(self):
            return [{"id": "folder-1", "name": "Important"}]

        def fetch_bookmark_folder_posts(self, folder_id: str, *, max_items: int = 50):
            assert folder_id == "folder-1"
            assert max_items == 4
            return [FeedCandidate(tweet_id="tweet-folder", screen_name="author_one", text="folder result").model_dump()]

        def fetch_user_likes(self, screen_name: str, *, max_items: int = 20):
            assert screen_name == "author_one"
            assert max_items == 5
            return [FeedCandidate(tweet_id="tweet-like", screen_name="author_one", text="liked").model_dump()]

        def fetch_followers(self, screen_name: str, *, max_items: int = 20):
            assert screen_name == "author_one"
            assert max_items == 6
            return [{"screen_name": "follower_one", "verified": True}]

        def fetch_following(self, screen_name: str, *, max_items: int = 20):
            assert screen_name == "author_one"
            assert max_items == 7
            return [{"screen_name": "following_one", "verified": False}]

        def fetch_article(self, tweet_id: str):
            assert tweet_id == "tweet-article-1"
            return FeedCandidate(
                tweet_id="tweet-article-1",
                screen_name="author_one",
                text="article preview",
                metadata={"article_title": "Longform Title", "article_text": "Longform body"},
            ).model_dump()

    monkeypatch.setattr(automation_api.TwitterClient, "from_config", lambda *args, **kwargs: FakeClient())

    with TestClient(app) as client:
        search = client.get("/twitter/search", params={"q": "openai", "limit": 2, "product": "Latest"})
        bookmarks = client.get("/twitter/bookmarks", params={"limit": 3})
        folders = client.get("/twitter/bookmarks/folders")
        folder_posts = client.get("/twitter/bookmarks/folders/folder-1", params={"limit": 4})
        likes = client.get("/twitter/users/author_one/likes", params={"limit": 5})
        followers = client.get("/twitter/users/author_one/followers", params={"limit": 6})
        following = client.get("/twitter/users/author_one/following", params={"limit": 7})
        article = client.get("/twitter/articles/tweet-article-1")

    assert search.status_code == 200
    assert search.json()["items"][0]["tweet_id"] == "tweet-search"
    assert bookmarks.status_code == 200
    assert bookmarks.json()["items"][0]["tweet_id"] == "tweet-bookmark"
    assert folders.status_code == 200
    assert folders.json()["items"][0]["id"] == "folder-1"
    assert folder_posts.status_code == 200
    assert folder_posts.json()["items"][0]["tweet_id"] == "tweet-folder"
    assert likes.status_code == 200
    assert likes.json()["items"][0]["tweet_id"] == "tweet-like"
    assert followers.status_code == 200
    assert followers.json()["items"][0]["screen_name"] == "follower_one"
    assert following.status_code == 200
    assert following.json()["items"][0]["screen_name"] == "following_one"
    assert article.status_code == 200
    assert article.json()["tweet"]["tweet_id"] == "tweet-article-1"


def test_runtime_draft_metrics_use_compact_prompt_payload(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {
                "id": tweet_id,
                "text": text,
                "author": {"screenName": screen_name, "verified": verified},
                "media": [{"type": "photo", "url": "https://example.com/image.jpg"}],
                "internal": "large ignored field",
            }
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified
            self.can_reply = True
            self.reply_limit_reason = None
            self.reply_limit_headline = None
            self.reply_restriction_policy = None

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet("111", "builder1", "preview")]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "builder1", "full api detail")

        def reply(self, tweet_id: str, text: str):
            return type("Reply", (), {
                "action": "reply",
                "ok": True,
                "dry_run": True,
                "target_tweet_id": tweet_id,
                "tweet_id": f"reply_{tweet_id}",
                "screen_name": "builder1",
                "text": text,
                "payload": {"ok": True},
                "error_code": None,
                "error_message": None,
            })()

        def follow(self, screen_name: str):
            return type("Follow", (), {
                "action": "follow",
                "ok": True,
                "dry_run": True,
                "target_tweet_id": None,
                "tweet_id": None,
                "screen_name": screen_name,
                "text": None,
                "payload": {"ok": True},
                "error_code": None,
                "error_message": None,
            })()

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            return [
                type("Moderation", (), {"tweet_id": candidates[0].tweet_id, "allowed": True, "category": None, "reason": "safe"})()
            ]

        def draft_reply(self, candidate, context=None):
            return type("Draft", (), {"text": "short reply", "rationale": "test"})()

    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: FakeAIProvider())

    storage = AutomationStorage(tmp_path / "runtime-draft-bytes.sqlite3")
    storage.initialize()
    graph = _build_runtime_graph(
        AutomationConfig(policies=PolicyConfig(candidate_refresh_rounds=0)),
        storage,
    )
    request = AutomationRequest.for_feed_engage(job_name="job", dry_run=True, approval_mode="ai_auto")

    snapshot = asyncio.run(graph.invoke(request))

    draft_event = next(event for event in snapshot.events if event.node == "draft_reply" and event.message == "base ai draft generated")
    assert draft_event.payload["ai_draft_input_bytes"] < len(__import__("json").dumps(snapshot.selected_candidate.model_dump(mode="json")).encode("utf-8"))


def test_openai_compatible_provider_uses_non_technical_reply_prompt() -> None:
    provider = OpenAICompatibleProvider(
        AISettings(
            provider="openai_compatible",
            model="demo-model",
            api_key="demo-key",
            base_url="https://example.com/v1",
        )
    )
    prompts: list[tuple[str, str]] = []

    def fake_chat(system: str, user: str) -> str:
        prompts.append((system, user))
        return '{"text":"That dog looks way too pleased with itself.","rationale":"non technical tone"}'

    provider._chat = fake_chat  # type: ignore[method-assign]

    result = provider.draft_reply(
        type(
            "Candidate",
            (),
            {
                "model_dump": lambda self, mode="json": {
                    "tweet_id": "1",
                    "screen_name": "petposter",
                    "text": "cloud-like puppy in the wind",
                    "metadata": {"media": [{"type": "photo"}]},
                }
            },
        )(),
        {"reply_style": "non_technical"},
    )

    assert result.text == "That dog looks way too pleased with itself."
    assert prompts[0][0] == (
        "Draft one short Twitter reply under 100 chars for a non-technical post. "
        "Sound natural, relaxed, and human. Lead with a light observation, mild reaction, gentle humor, or easy empathy. "
        "Do not force technical jargon, engineering framing, or heavy analysis onto the post. "
        "Keep it conversational and plainspoken. One or two short sentences. Prefer statements over questions. No lists. No emojis. Return JSON with text and rationale."
    )
























def test_runtime_observability_does_not_mutate_request_metadata(monkeypatch, tmp_path: Path) -> None:
    class GuardedMetadata(dict):
        def __setitem__(self, key, value):  # type: ignore[override]
            raise AssertionError("request metadata should not be mutated at runtime")

        def pop(self, key, default=None):  # type: ignore[override]
            raise AssertionError("request metadata should not be used for runtime observability")

        def update(self, *args, **kwargs):  # type: ignore[override]
            raise AssertionError("request metadata should not be updated at runtime")

    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {
                "id": tweet_id,
                "text": text,
                "author": {"screenName": screen_name, "verified": verified},
            }
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet("111", "builder1", "preview one")]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "builder1", "full text one")

        def reply(self, tweet_id: str, text: str):
            return type("Reply", (), {
                "action": "reply",
                "ok": True,
                "dry_run": False,
                "target_tweet_id": tweet_id,
                "tweet_id": f"reply_{tweet_id}",
                "screen_name": "builder1",
                "text": text,
                "payload": {"ok": True},
                "error_code": None,
                "error_message": None,
            })()

        def follow(self, screen_name: str):
            return type("Follow", (), {
                "action": "follow",
                "ok": True,
                "dry_run": False,
                "target_tweet_id": None,
                "tweet_id": None,
                "screen_name": screen_name,
                "text": None,
                "payload": {"ok": True},
                "error_code": None,
                "error_message": None,
            })()

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            return [
                type("Moderation", (), {"tweet_id": candidate.tweet_id, "allowed": True, "category": None, "reason": "ok"})()
                for candidate in candidates
            ]

        def draft_reply(self, candidate, context=None):
            return type("Draft", (), {"text": "short reply", "rationale": "test"})()

    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: FakeAIProvider())

    storage = AutomationStorage(tmp_path / "runtime-observability.sqlite3")
    storage.initialize()
    graph = _build_runtime_graph(AutomationConfig(), storage)
    request = AutomationRequest.for_feed_engage(job_name="job", dry_run=False, approval_mode="ai_auto")
    request.metadata = GuardedMetadata({"source": "user"})

    snapshot = asyncio.run(graph.invoke(request))

    assert snapshot.status.value == "completed"
    assert dict(snapshot.request.metadata) == {"source": "user"}
    fetch_event = next(event for event in snapshot.events if event.node == "fetch_feed" and event.message == "feed fetched")
    assert fetch_event.payload["candidate_source"] == "feed"


def test_runtime_select_candidate_uses_single_candidate_moderation_before_policy_guard(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {
                "id": tweet_id,
                "text": text,
                "author": {"screenName": screen_name, "verified": verified},
            }
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified
            self.can_reply = True
            self.reply_limit_reason = None
            self.reply_limit_headline = None
            self.reply_restriction_policy = None

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet("111", "builder1", "preview")]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "builder1", "full api detail")

    class FakeAIProvider:
        def __init__(self) -> None:
            self.moderation_calls: list[list[str | None]] = []

        def moderate_candidates(self, candidates):
            self.moderation_calls.append([candidate.text for candidate in candidates])
            return [
                type("Moderation", (), {"tweet_id": candidate.tweet_id, "allowed": True, "category": None, "reason": "safe"})()
                for candidate in candidates
            ]

        def draft_reply(self, candidate, context=None):
            return type("Draft", (), {"text": "short reply", "rationale": "test"})()

    provider = FakeAIProvider()
    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: provider)

    storage = AutomationStorage(tmp_path / "runtime-moderation-cache.sqlite3")
    storage.initialize()
    graph = _build_runtime_graph(
        AutomationConfig(policies=PolicyConfig(candidate_refresh_rounds=0)),
        storage,
    )
    request = AutomationRequest.for_feed_engage(job_name="job", dry_run=True, approval_mode="ai_auto")

    snapshot = asyncio.run(graph.invoke(request))

    assert snapshot.status.value == "completed"
    assert provider.moderation_calls == [["full api detail"]]


def test_runtime_select_candidate_stops_after_first_allowed_candidate(monkeypatch, tmp_path: Path) -> None:
    fetch_calls: list[str] = []

    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {
                "id": tweet_id,
                "text": text,
                "author": {"screenName": screen_name, "verified": verified},
            }
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified
            self.can_reply = True
            self.reply_limit_reason = None
            self.reply_limit_headline = None
            self.reply_restriction_policy = None

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [
                FakeTweet("111", "builder1", "preview one"),
                FakeTweet("222", "builder2", "preview two"),
                FakeTweet("333", "builder3", "preview three"),
            ]

        def fetch_tweet(self, tweet_id: str):
            fetch_calls.append(tweet_id)
            return FakeTweet(tweet_id, f"builder{tweet_id[0]}", f"full text {tweet_id}")

        def reply(self, tweet_id: str, text: str):
            return type("Reply", (), {
                "action": "reply",
                "ok": True,
                "dry_run": True,
                "target_tweet_id": tweet_id,
                "tweet_id": f"reply_{tweet_id}",
                "screen_name": "builder",
                "text": text,
                "payload": {"ok": True},
                "error_code": None,
                "error_message": None,
            })()

        def follow(self, screen_name: str):
            return type("Follow", (), {
                "action": "follow",
                "ok": True,
                "dry_run": True,
                "target_tweet_id": None,
                "tweet_id": None,
                "screen_name": screen_name,
                "text": None,
                "payload": {"ok": True},
                "error_code": None,
                "error_message": None,
            })()

    class FakeAIProvider:
        def __init__(self) -> None:
            self.moderation_calls: list[list[str | None]] = []

        def moderate_candidates(self, candidates):
            self.moderation_calls.append([candidate.text for candidate in candidates])
            return [
                type("Moderation", (), {"tweet_id": candidates[0].tweet_id, "allowed": True, "category": None, "reason": "safe"})()
            ]

        def draft_reply(self, candidate, context=None):
            return type("Draft", (), {"text": "short reply", "rationale": "test"})()

    provider = FakeAIProvider()
    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: provider)

    storage = AutomationStorage(tmp_path / "runtime-sequential-select.sqlite3")
    storage.initialize()
    graph = _build_runtime_graph(
        AutomationConfig(policies=PolicyConfig(candidate_refresh_rounds=0)),
        storage,
    )
    request = AutomationRequest.for_feed_engage(job_name="job", dry_run=True, approval_mode="ai_auto")

    snapshot = asyncio.run(graph.invoke(request))

    assert snapshot.status.value == "completed"
    assert fetch_calls == ["111"]
    assert provider.moderation_calls == [["full text 111"]]
    assert snapshot.selection_source == "ordered_candidates"
    select_event = next(event for event in snapshot.events if event.node == "select_candidate" and event.message == "candidate selected")
    assert select_event.payload["hydrated_count"] == 1
    assert select_event.payload["ai_moderation_candidate_count"] == 1


def test_runtime_select_candidate_checks_next_candidate_after_rejection(monkeypatch, tmp_path: Path) -> None:
    fetch_calls: list[str] = []

    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {
                "id": tweet_id,
                "text": text,
                "author": {"screenName": screen_name, "verified": verified},
            }
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified
            self.can_reply = True
            self.reply_limit_reason = None
            self.reply_limit_headline = None
            self.reply_restriction_policy = None

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [
                FakeTweet("111", "builder1", "preview one"),
                FakeTweet("222", "builder2", "preview two"),
            ]

        def fetch_tweet(self, tweet_id: str):
            fetch_calls.append(tweet_id)
            return FakeTweet(tweet_id, f"builder{tweet_id[0]}", f"full text {tweet_id}")

        def reply(self, tweet_id: str, text: str):
            return type("Reply", (), {
                "action": "reply",
                "ok": True,
                "dry_run": True,
                "target_tweet_id": tweet_id,
                "tweet_id": f"reply_{tweet_id}",
                "screen_name": "builder",
                "text": text,
                "payload": {"ok": True},
                "error_code": None,
                "error_message": None,
            })()

        def follow(self, screen_name: str):
            return type("Follow", (), {
                "action": "follow",
                "ok": True,
                "dry_run": True,
                "target_tweet_id": None,
                "tweet_id": None,
                "screen_name": screen_name,
                "text": None,
                "payload": {"ok": True},
                "error_code": None,
                "error_message": None,
            })()

    class FakeAIProvider:
        def __init__(self) -> None:
            self.moderation_calls: list[list[str | None]] = []

        def moderate_candidates(self, candidates):
            text = candidates[0].text
            self.moderation_calls.append([text])
            allowed = text == "full text 222"
            reason = "safe" if allowed else "war topic"
            category = None if allowed else "war"
            return [
                type("Moderation", (), {"tweet_id": candidates[0].tweet_id, "allowed": allowed, "category": category, "reason": reason})()
            ]

        def draft_reply(self, candidate, context=None):
            return type("Draft", (), {"text": "short reply", "rationale": "test"})()

    provider = FakeAIProvider()
    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: provider)

    storage = AutomationStorage(tmp_path / "runtime-sequential-next.sqlite3")
    storage.initialize()
    graph = _build_runtime_graph(
        AutomationConfig(policies=PolicyConfig(candidate_refresh_rounds=0)),
        storage,
    )
    request = AutomationRequest.for_feed_engage(job_name="job", dry_run=True, approval_mode="ai_auto")

    snapshot = asyncio.run(graph.invoke(request))

    assert snapshot.status.value == "completed"
    assert fetch_calls == ["111", "222"]
    assert provider.moderation_calls == [["full text 111"], ["full text 222"]]
    assert snapshot.selected_candidate is not None
    assert snapshot.selected_candidate.tweet_id == "222"


def test_candidate_cache_upsert_strips_internal_runtime_metadata(tmp_path: Path) -> None:
    storage = AutomationStorage(tmp_path / "candidate-cache-strip-runtime.sqlite3")
    storage.initialize()
    storage.upsert_candidate_cache_entries(
        workflow="feed_engage",
        source_run_id="source-run",
        candidates=[
            {
                "tweet_id": "111",
                "screen_name": "builder1",
                "created_at": "2026-04-20T03:35:55+00:00",
                "text": "cached full api detail",
                "metadata": {
                    "id": "111",
                    "text": "cached full api detail",
                    "author": {"screenName": "builder1", "verified": True},
                    "_x_atuo_hydrated": True,
                    _AI_MODERATION_METADATA_KEY: {
                        "tweet_id": "111",
                        "cache_key": "dead",
                        "allowed": True,
                        "category": None,
                        "reason": "safe",
                    },
                },
            }
        ],
        expires_at="2999-01-01T00:00:00+00:00",
    )

    pending = storage.list_pending_candidate_cache(workflow="feed_engage", limit=10)

    assert pending[0]["metadata"] == {
        "id": "111",
        "text": "cached full api detail",
        "author": {"screenName": "builder1", "verified": True},
    }


def test_runtime_cached_moderation_misses_when_non_text_payload_changes(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, topic: str):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {
                "id": tweet_id,
                "text": text,
                "topic": topic,
                "author": {"screenName": screen_name, "verified": True},
            }
            self.author = type("Author", (), {"screen_name": screen_name, "verified": True})()
            self.screen_name = screen_name
            self.verified = True
            self.can_reply = True
            self.reply_limit_reason = None
            self.reply_limit_headline = None
            self.reply_restriction_policy = None

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet("111", "builder1", "same text", "fresh")]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "builder1", "same text", "fresh")

    class FakeAIProvider:
        def __init__(self) -> None:
            self.moderation_calls = 0

        def moderate_candidates(self, candidates):
            self.moderation_calls += 1
            return [
                type("Moderation", (), {"tweet_id": candidates[0].tweet_id, "allowed": True, "category": None, "reason": "safe"})()
            ]

        def draft_reply(self, candidate, context=None):
            return type("Draft", (), {"text": "short reply", "rationale": "test"})()

    text = "same text"
    stale_candidate = FeedCandidate(
        tweet_id="111",
        screen_name="builder1",
        text=text,
        created_at=datetime.fromisoformat("2026-04-20T03:35:55+00:00"),
        author_verified=True,
        metadata={"id": "111", "text": text, "topic": "stale", "author": {"screenName": "builder1", "verified": True}},
    )
    storage = AutomationStorage(tmp_path / "runtime-moderation-cache-miss.sqlite3")
    storage.initialize()
    storage.upsert_candidate_cache_entries(
        workflow="feed_engage",
        source_run_id="source-run",
        candidates=[
            {
                "tweet_id": "111",
                "screen_name": "builder1",
                "created_at": "2026-04-20T03:35:55+00:00",
                "text": text,
                "metadata": {
                    "id": "111",
                    "text": text,
                    "topic": "stale",
                    "author": {"screenName": "builder1", "verified": True},
                    _AI_MODERATION_METADATA_KEY: {
                        "tweet_id": "111",
                        "cache_key": build_moderation_cache_key(stale_candidate, provider="none", model=None),
                        "allowed": True,
                        "category": None,
                        "reason": "safe",
                    },
                },
            }
        ],
        expires_at="2999-01-01T00:00:00+00:00",
    )
    provider = FakeAIProvider()
    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: provider)
    graph = _build_runtime_graph(
        AutomationConfig(policies=PolicyConfig(candidate_refresh_rounds=0)),
        storage,
    )
    request = AutomationRequest.for_feed_engage(job_name="job", dry_run=True, approval_mode="ai_auto")

    snapshot = asyncio.run(graph.invoke(request))

    assert snapshot.status is RunStatus.COMPLETED
    assert provider.moderation_calls == 1


def test_feed_engage_candidate_policy_guard_retries_duplicate_candidate() -> None:
    class FakeHooks:
        def has_dedupe_key(self, dedupe_key: str) -> bool:
            return True

        def get_daily_execution_count(self, workflow, day) -> int:
            return 0

        def get_last_author_engagement(self, screen_name: str):
            return None

        def has_target_tweet_id(self, target_tweet_id: str) -> bool:
            return False

    graph = AutomationGraph(AutomationConfig(), WorkflowAdapters(policy_hooks=FakeHooks()))
    request = AutomationRequest.for_feed_engage(job_name="job", reply_text="hello")
    state = make_initial_state(request)
    snapshot = state["snapshot"]
    snapshot.candidates = [
        FeedCandidate(tweet_id="111", screen_name="author1", text="preview one", author_verified=True),
        FeedCandidate(tweet_id="222", screen_name="author2", text="preview two", author_verified=True),
    ]
    snapshot.selected_candidate = snapshot.candidates[0]
    snapshot.selection_source = "ai"
    snapshot.selection_reason = "duplicate"
    snapshot.reply_context = {"reply_style": "technical"}
    snapshot.rendered_text = "draft one"
    snapshot.drafting_source = "ai"

    result = asyncio.run(graph.candidate_policy_guard(state))
    routed = graph.route_after_candidate_policy(result)

    assert result["snapshot"].selected_candidate is None
    assert [candidate.tweet_id for candidate in result["snapshot"].candidates] == ["222"]
    assert result["snapshot"].selection_source is None
    assert result["snapshot"].selection_reason is None
    assert result["snapshot"].reply_context == {}
    assert result["snapshot"].rendered_text is None
    assert result["snapshot"].drafting_source is None
    assert routed == "retry_candidate"


def test_feed_engage_select_candidate_fails_when_candidate_evaluation_errors() -> None:
    def fail_selection(snapshot):
        raise AIProviderError("selection unavailable")

    graph = AutomationGraph(
        AutomationConfig(),
        WorkflowAdapters(select_candidate=fail_selection),
    )
    request = AutomationRequest.for_feed_engage(job_name="job")
    state = make_initial_state(request)
    snapshot = state["snapshot"]
    snapshot.candidates = [
        FeedCandidate(tweet_id="111", screen_name="author1", text="preview one", author_verified=True),
    ]

    result = asyncio.run(graph.select_candidate(state))
    routed = graph.route_after_selection(result)

    assert result["snapshot"].status is RunStatus.FAILED
    assert "candidate evaluation failed: selection unavailable" in result["snapshot"].errors
    assert result["snapshot"].selected_candidate is None
    assert routed == "finalize"


def test_runtime_select_candidate_reports_moderation_failure_as_candidate_evaluation(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {
                "id": tweet_id,
                "text": text,
                "author": {"screenName": screen_name, "verified": verified},
            }
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified
            self.can_reply = True
            self.reply_limit_reason = None
            self.reply_limit_headline = None
            self.reply_restriction_policy = None

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet("111", "builder1", "preview")]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "builder1", "full api detail")

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            raise AIProviderError("moderation unavailable")

        def draft_reply(self, candidate, context=None):
            return type("Draft", (), {"text": "short reply", "rationale": "test"})()

    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: FakeAIProvider())

    storage = AutomationStorage(tmp_path / "runtime-candidate-evaluation.sqlite3")
    storage.initialize()
    graph = _build_runtime_graph(
        AutomationConfig(policies=PolicyConfig(candidate_refresh_rounds=0)),
        storage,
    )
    request = AutomationRequest.for_feed_engage(job_name="job", dry_run=True, approval_mode="ai_auto")

    snapshot = asyncio.run(graph.invoke(request))

    assert snapshot.status is RunStatus.FAILED
    assert "candidate evaluation failed: moderation unavailable" in snapshot.errors
    assert any(
        event.node == "select_candidate"
        and event.message == "candidate evaluation failed"
        and event.payload["error"] == "moderation unavailable"
        for event in snapshot.events
    )


def test_feed_engage_draft_text_fails_when_ai_draft_errors() -> None:
    def fail_draft(snapshot):
        raise AIProviderError("draft unavailable")

    graph = AutomationGraph(
        AutomationConfig(),
        WorkflowAdapters(draft_reply=fail_draft),
    )
    request = AutomationRequest.for_feed_engage(job_name="job")
    state = make_initial_state(request)
    snapshot = state["snapshot"]
    snapshot.selected_candidate = FeedCandidate(
        tweet_id="111",
        screen_name="author1",
        text="preview one",
        author_verified=True,
    )

    result = asyncio.run(graph.draft_text(state))
    routed = graph.route_after_policy(result)

    assert result["snapshot"].status is RunStatus.FAILED
    assert "ai draft failed: draft unavailable" in result["snapshot"].errors
    assert result["snapshot"].rendered_text is None
    assert routed == "blocked"


def test_feed_engage_execute_refreshes_after_no_candidate_succeeded() -> None:
    graph = AutomationGraph(
        AutomationConfig(),
        WorkflowAdapters(
            execute_engage=lambda snapshot: ExecutionResult(
                action="engage",
                ok=False,
                dry_run=False,
                error="No candidate succeeded",
                detail={"attempts": [{"tweet_id": "111", "screen_name": "author1", "outcome": "reply_restricted", "detail": "433"}]},
            ),
            policy_hooks=None,
        ),
    )
    request = AutomationRequest.for_feed_engage(job_name="job", dry_run=False)
    state = make_initial_state(request)
    snapshot = state["snapshot"]
    snapshot.candidates = [FeedCandidate(tweet_id="111", screen_name="author1", text="preview", author_verified=True)]
    snapshot.selected_candidate = snapshot.candidates[0]
    snapshot.selection_source = "ai"
    snapshot.selection_reason = "picked one"
    snapshot.reply_context = {"reply_style": "technical"}
    snapshot.rendered_text = "draft one"
    snapshot.drafting_source = "ai"
    snapshot.candidate_cache_persisted = True

    result = asyncio.run(graph.execute(state))
    routed = graph.route_after_execute(result)

    assert result["snapshot"].candidate_refresh_pending is True
    assert result["snapshot"].candidate_refresh_count == 1
    assert result["snapshot"].candidates == []
    assert result["snapshot"].selected_candidate is None
    assert result["snapshot"].selection_source is None
    assert result["snapshot"].selection_reason is None
    assert result["snapshot"].reply_context == {}
    assert result["snapshot"].rendered_text is None
    assert result["snapshot"].drafting_source is None
    assert result["snapshot"].candidate_cache_persisted is False
    assert routed == "fetch_feed"
    assert any(
        event.node == "execute"
        and event.message == "execution deferred to candidate refresh"
        for event in result["snapshot"].events
    )
