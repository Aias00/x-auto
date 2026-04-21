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
from x_atuo.automation.graph import _build_runtime_graph
from x_atuo.automation.schemas import FeedEngageRequest
from x_atuo.automation.policies import build_dedupe_key
from x_atuo.automation.scheduler import AutomationScheduler
from x_atuo.automation.state import FeedCandidate
from x_atuo.automation.state import AutomationRequest
from x_atuo.automation.state import WorkflowKind
from x_atuo.automation.state import make_initial_state
from x_atuo.automation.storage import AutomationStorage
import x_atuo.automation.api as automation_api
from x_atuo.core.ai_client import AIProviderError
from x_atuo.core.ai_client import OpenAICompatibleProvider
from x_atuo.core.ai_client import compose_reply_text
from x_atuo.core.twitter_models import TweetRecord


def test_dedupe_key_is_stable() -> None:
    request = AutomationRequest.for_direct_post(post_text="hello")
    assert build_dedupe_key(request, text="hello") == build_dedupe_key(request, text="hello")


def test_daily_execution_limit_disabled_by_default() -> None:
    config = AutomationConfig()
    assert config.policies.daily_execution_limit is None


def test_author_cooldown_disabled_by_default() -> None:
    config = AutomationConfig()
    assert config.policies.per_author_cooldown_minutes is None


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


def test_feed_engage_defaults_are_ready_to_run() -> None:
    payload = FeedEngageRequest().model_dump(mode="json")
    assert payload["mode"] == "ai_auto"
    assert payload["dry_run"] is False
    assert payload["proxy"] == "http://127.0.0.1:7890"
    assert payload["feed_count"] == 10
    assert payload["feed_type"] == "for-you"


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


def test_compose_reply_text_never_exceeds_max_length() -> None:
    acknowledgment = "A" * 10
    fuller_angle = "B" * 20

    text = compose_reply_text(acknowledgment, fuller_angle, max_length=20)

    assert len(text) <= 20


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


def test_feed_candidate_carries_created_at_from_feed(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self):
            self.tweet_id = "111"
            self.text = "candidate"
            self.created_at = datetime.fromisoformat("2026-04-20T03:35:55+00:00")
            self.raw = {
                "id": "111",
                "text": "candidate",
                "created_at": "2026-04-20T03:35:55+00:00",
                "author": {"screenName": "demo", "verified": True},
            }
            self.author = type("Author", (), {"screen_name": "demo", "verified": True})()
            self.screen_name = "demo"
            self.verified = True

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet()]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet()

        def reply(self, tweet_id: str, text: str):
            return type("Reply", (), {
                "action": "reply",
                "ok": True,
                "dry_run": False,
                "target_tweet_id": tweet_id,
                "tweet_id": "reply_111",
                "screen_name": "demo",
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

    monkeypatch.setattr(
        "x_atuo.automation.graph.TwitterClient.from_config",
        lambda *args, **kwargs: FakeClient(),
    )
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "candidate-created-at.sqlite3"))
    monkeypatch.setenv("X_ATUO_AI__PROVIDER", "none")

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"feed_count": 1, "mode": "deterministic", "dry_run": False},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["result"]["selected_candidate"]["created_at"] == "2026-04-20T03:35:55Z"


def test_feed_candidates_are_sorted_newest_first(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, created_at: str):
            self.tweet_id = tweet_id
            self.text = f"candidate-{tweet_id}"
            self.created_at = datetime.fromisoformat(created_at)
            self.raw = {
                "id": tweet_id,
                "text": self.text,
                "createdAt": created_at,
                "author": {"screenName": f"author-{tweet_id}", "verified": True},
            }
            self.author = type("Author", (), {"screen_name": f"author-{tweet_id}", "verified": True})()
            self.screen_name = f"author-{tweet_id}"
            self.verified = True

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [
                FakeTweet("older", "2026-04-20T03:30:00+00:00"),
                FakeTweet("newer", "2026-04-20T03:35:00+00:00"),
            ]

        def fetch_tweet(self, tweet_id: str):
            if tweet_id == "older":
                return FakeTweet("older", "2026-04-20T03:30:00+00:00")
            return FakeTweet("newer", "2026-04-20T03:35:00+00:00")

        def reply(self, tweet_id: str, text: str):
            return type("Reply", (), {
                "action": "reply",
                "ok": True,
                "dry_run": False,
                "target_tweet_id": tweet_id,
                "tweet_id": f"reply_{tweet_id}",
                "screen_name": f"author-{tweet_id}",
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

    monkeypatch.setattr(
        "x_atuo.automation.graph.TwitterClient.from_config",
        lambda *args, **kwargs: FakeClient(),
    )
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "candidate-sort.sqlite3"))
    monkeypatch.setenv("X_ATUO_AI__PROVIDER", "none")

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"feed_count": 2, "mode": "deterministic", "dry_run": False},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert body["result"]["selected_candidate"]["tweet_id"] == "newer"


def test_feed_engage_hydrates_only_newest_shortlist_after_preview_moderation(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, created_at: str):
            self.tweet_id = tweet_id
            self.text = f"preview-{tweet_id}"
            self.created_at = datetime.fromisoformat(created_at)
            self.raw = {
                "id": tweet_id,
                "text": self.text,
                "createdAt": created_at,
                "author": {"screenName": f"author-{tweet_id}", "verified": True},
            }
            self.author = type("Author", (), {"screen_name": f"author-{tweet_id}", "verified": True})()
            self.screen_name = f"author-{tweet_id}"
            self.verified = True

    fetch_tweet_calls: list[str] = []

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            assert max_items == 5
            return [
                FakeTweet("oldest", "2026-04-20T03:31:00+00:00"),
                FakeTweet("mid1", "2026-04-20T03:32:00+00:00"),
                FakeTweet("mid2", "2026-04-20T03:33:00+00:00"),
                FakeTweet("new2", "2026-04-20T03:34:00+00:00"),
                FakeTweet("new1", "2026-04-20T03:35:00+00:00"),
            ]

        def fetch_tweet(self, tweet_id: str):
            fetch_tweet_calls.append(tweet_id)
            created_at_by_id = {
                "oldest": "2026-04-20T03:31:00+00:00",
                "mid1": "2026-04-20T03:32:00+00:00",
                "mid2": "2026-04-20T03:33:00+00:00",
                "new2": "2026-04-20T03:34:00+00:00",
                "new1": "2026-04-20T03:35:00+00:00",
            }
            tweet = FakeTweet(tweet_id, created_at_by_id[tweet_id])
            tweet.text = f"full-{tweet_id}"
            tweet.raw["text"] = tweet.text
            return tweet

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            if len(candidates) == 5:
                assert [candidate.tweet_id for candidate in candidates] == ["new1", "new2", "mid2", "mid1", "oldest"]
                assert all((candidate.text or "").startswith("preview-") for candidate in candidates)
            else:
                assert len(candidates) == 1
                assert candidates[0].tweet_id == "new1"
                assert candidates[0].text == "full-new1"
            return [
                type("Moderation", (), {"tweet_id": candidate.tweet_id, "allowed": True, "category": None, "reason": "technical"})()
                for candidate in candidates
            ]

        def select_candidate(self, candidates):
            assert [candidate.tweet_id for candidate in candidates] == ["new1", "new2", "mid2"]
            assert [candidate.text for candidate in candidates] == ["full-new1", "full-new2", "full-mid2"]
            return type("Selection", (), {"tweet_id": "new1", "reason": "latest full-text shortlist"})()

        def draft_reply(self, candidate, context=None):
            return type("Draft", (), {"text": "This is the freshest one.", "rationale": "shortlist hydrated"})()

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: FakeAIProvider())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "shortlist-hydration.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"feed_count": 5, "mode": "ai_auto", "dry_run": True},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert sorted(fetch_tweet_calls) == ["mid2", "new1", "new2"]
    assert body["result"]["selected_candidate"]["tweet_id"] == "new1"
    assert body["result"]["selected_candidate"]["created_at"] == "2026-04-20T03:35:00Z"


def test_only_shortlist_candidates_are_hydrated_before_selection(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, created_at: str, text: str):
            self.tweet_id = tweet_id
            self.text = text
            self.created_at = datetime.fromisoformat(created_at)
            self.raw = {
                "id": tweet_id,
                "text": text,
                "createdAt": created_at,
                "author": {"screenName": f"author-{tweet_id}", "verified": True},
            }
            self.author = type("Author", (), {"screen_name": f"author-{tweet_id}", "verified": True})()
            self.screen_name = f"author-{tweet_id}"
            self.verified = True

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def __init__(self):
            self.fetch_tweet_calls: list[str] = []

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [
                FakeTweet("1", "2026-04-20T03:31:00+00:00", "preview-1"),
                FakeTweet("2", "2026-04-20T03:32:00+00:00", "preview-2"),
                FakeTweet("3", "2026-04-20T03:33:00+00:00", "preview-3"),
                FakeTweet("4", "2026-04-20T03:34:00+00:00", "preview-4"),
                FakeTweet("5", "2026-04-20T03:35:00+00:00", "preview-5"),
            ]

        def fetch_tweet(self, tweet_id: str):
            self.fetch_tweet_calls.append(tweet_id)
            return FakeTweet(tweet_id, f"2026-04-20T03:3{tweet_id}:00+00:00", f"full-{tweet_id}")

        def reply(self, tweet_id: str, text: str):
            return type("Reply", (), {
                "action": "reply",
                "ok": True,
                "dry_run": False,
                "target_tweet_id": tweet_id,
                "tweet_id": f"reply_{tweet_id}",
                "screen_name": f"author-{tweet_id}",
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
                type("Moderation", (), {"tweet_id": c.tweet_id, "allowed": True, "category": None, "reason": "ok"})()
                for c in candidates
            ]

        def select_candidate(self, candidates):
            assert [candidate.tweet_id for candidate in candidates] == ["5", "4", "3"]
            assert [candidate.text for candidate in candidates] == ["full-5", "full-4", "full-3"]
            return type("Selection", (), {"tweet_id": "4", "reason": "hydrated shortlist"})()

        def draft_reply(self, candidate, context=None):
            return type("Draft", (), {"text": "Short and direct.", "rationale": "ok"})()

    fake_client = FakeClient()
    monkeypatch.setattr(
        "x_atuo.automation.graph.TwitterClient.from_config",
        lambda *args, **kwargs: fake_client,
    )
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: FakeAIProvider())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "shortlist-hydration.sqlite3"))
    monkeypatch.setenv("X_ATUO_POLICIES__CANDIDATE_HYDRATION_COUNT", "3")

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"feed_count": 5, "mode": "ai_auto", "dry_run": False},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert body["result"]["selected_candidate"]["tweet_id"] == "4"
    assert sorted(fake_client.fetch_tweet_calls) == ["3", "4", "5"]


def test_storage_round_trip(tmp_path: Path) -> None:
    storage = AutomationStorage(tmp_path / "test.sqlite3")
    storage.initialize()
    storage.upsert_job("job_1", "feed_engage", config={"reply_text": "hi"})
    storage.create_run(
        run_id="run_1",
        job_id="job_1",
        job_type="feed_engage",
        endpoint="/hooks/twitter/feed-engage",
        request_payload={"reply_text": "hi"},
    )
    storage.update_run("run_1", status="completed", response_payload={"ok": True})
    run = storage.get_run("run_1")
    assert run is not None
    assert run["run"]["status"] == "completed"
    assert run["run"]["response_payload"]["ok"] is True


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
                "SELECT job_type, endpoint, status FROM runs ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            conn.close()
            if row == ("feed_engage", "scheduler:feed-engage", "completed"):
                break
            time.sleep(0.01)

    assert row == ("feed_engage", "scheduler:feed-engage", "completed")


def test_feed_engage_webhook_dry_run(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str = "demo", verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {"id": tweet_id, "text": text}
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeCommand:
        def __init__(self, action: str, ok: bool = True, tweet_id: str | None = None):
            self.action = action
            self.ok = ok
            self.dry_run = True
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
            return FakeCommand("reply", ok=True, tweet_id="reply_1")

        def follow(self, screen_name: str):
            return FakeCommand("follow", ok=True)

        def post(self, text: str, *, reply_to=None, images=None):
            raise AssertionError("post should not be called")

        def quote(self, tweet_id: str, text: str, *, images=None):
            raise AssertionError("quote should not be called")

    monkeypatch.setattr(
        "x_atuo.automation.graph.TwitterClient.from_config",
        lambda *args, **kwargs: FakeClient(),
    )
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "feed.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"reply_template": "Thanks for sharing", "dry_run": True, "feed_count": 1},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert body["result"]["result"]["action"] == "engage"
    assert body["result"]["result"]["dry_run"] is True


def test_repo_post_webhook_uses_repo_context(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "x_atuo.automation.graph.fetch_repo_context",
        lambda repo_url: {
            "repo_url": repo_url,
            "repo_name": "magika",
            "description": "AI-powered file type detection",
            "stars": 12345,
            "readme_excerpt": "Classifies files by content.",
            "metadata": {},
        },
    )
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "repo.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/repo-post",
            json={"repo_url": "https://github.com/google/magika", "dry_run": True},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    payload = body["result"]["result"]["detail"]["payload"]
    assert "google/magika" in payload["text"]
    assert "github.com/google/magika" in payload["text"]


def test_feed_engage_ai_mode_uses_provider_for_selection_and_draft(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {"id": tweet_id, "text": text}
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [
                FakeTweet("111", "firstauthor", "first option"),
                FakeTweet("222", "secondauthor", "better option"),
            ]

        def fetch_tweet(self, tweet_id: str):
            screen_name = "secondauthor" if tweet_id == "222" else "firstauthor"
            text = "selected option with more technical detail" if tweet_id == "222" else "selected"
            return FakeTweet(tweet_id, screen_name, text)

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    monkeypatch.setattr(
        "x_atuo.automation.graph.TwitterClient.from_config",
        lambda *args, **kwargs: FakeClient(),
    )
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "ai-feed.sqlite3"))
    monkeypatch.setenv("X_ATUO_AI__PROVIDER", "mock")

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 2, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    detail = body["result"]["result"]["detail"]
    assert body["result"]["result"]["target_tweet_id"] == "222"
    assert detail["selected_by"] == "ai"
    assert detail["drafted_by"] == "ai"
    assert body["result"]["rendered_text"] == "@secondauthor The real shift here is where the bottleneck moves next."
    assert len(body["result"]["rendered_text"]) <= 280


def test_feed_engage_uses_plainspoken_statement_fallback(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {"id": tweet_id, "text": text}
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet("111", "demoauthor", "interesting candidate")]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "demoauthor", "interesting candidate")

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    monkeypatch.setattr(
        "x_atuo.automation.graph.TwitterClient.from_config",
        lambda *args, **kwargs: FakeClient(),
    )
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "fallback-tone.sqlite3"))
    monkeypatch.setenv("X_ATUO_AI__PROVIDER", "none")

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 1},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert "interesting candidate" in body["result"]["rendered_text"].lower()
    assert body["result"]["rendered_text"] != "The real win here is how much complexity this strips out."


def test_feed_engage_base_reply_hydrates_selected_tweet_before_drafting(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {"id": tweet_id, "text": text}
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def __init__(self):
            self.fetch_tweet_calls = 0

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet("111", "demoauthor", "preview")]

        def fetch_tweet(self, tweet_id: str):
            self.fetch_tweet_calls += 1
            return FakeTweet(tweet_id, "demoauthor", "full technical breakdown")

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    fake_client = FakeClient()
    monkeypatch.setattr(
        "x_atuo.automation.graph.TwitterClient.from_config",
        lambda *args, **kwargs: fake_client,
    )
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "fallback-hydrate.sqlite3"))
    monkeypatch.setenv("X_ATUO_AI__PROVIDER", "none")

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 1, "mode": "deterministic"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert "full technical breakdown" in body["result"]["rendered_text"].lower()
    assert fake_client.fetch_tweet_calls == 1


def test_repo_post_ai_mode_uses_provider_when_post_text_missing(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "x_atuo.automation.graph.fetch_repo_context",
        lambda repo_url: {
            "repo_url": repo_url,
            "repo_name": "google/magika",
            "description": "AI-powered file type detection",
            "readme_excerpt": "Classifies files by content.",
            "stars": 12345,
            "metadata": {},
        },
    )
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "ai-repo.sqlite3"))
    monkeypatch.setenv("X_ATUO_AI__PROVIDER", "mock")

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/repo-post",
            json={"repo_url": "https://github.com/google/magika", "dry_run": True, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    detail = body["result"]["result"]["detail"]
    assert detail["drafted_by"] == "ai"
    assert "google/magika" in detail["payload"]["text"]


def test_ai_auto_executes_without_approval(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {"id": tweet_id, "text": text}
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet("111", "demoauthor", "interesting candidate")]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "demoauthor", "interesting candidate")

        def reply(self, tweet_id: str, text: str):
            return type("Reply", (), {
                "action": "reply",
                "ok": True,
                "dry_run": False,
                "target_tweet_id": tweet_id,
                "tweet_id": "reply_auto",
                "screen_name": "demoauthor",
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

    monkeypatch.setattr(
        "x_atuo.automation.graph.TwitterClient.from_config",
        lambda *args, **kwargs: FakeClient(),
    )
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "ai-auto.sqlite3"))
    monkeypatch.setenv("X_ATUO_AI__PROVIDER", "mock")

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"feed_count": 1, "mode": "ai_auto", "dry_run": False},
        )
        assert response.status_code == 202
        body = response.json()
        assert body["status"] == "completed"
        assert body["result"]["result"]["created_tweet_id"] == "reply_auto"
        assert body["result"]["result"]["reply_url"] == "https://x.com/i/status/reply_auto"
        assert body["result"]["result"]["target_tweet_url"] == "https://x.com/demoauthor/status/111"
        assert body["result"]["result"]["detail"]["selected_by"] == "ai"
        assert body["result"]["result"]["detail"]["drafted_by"] == "ai"
        assert "@demoauthor" in body["result"]["rendered_text"].lower()
        assert len(body["result"]["rendered_text"]) <= 280


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


def test_explicit_engage_workflow_removed_from_state_model() -> None:
    assert "EXPLICIT_ENGAGE" not in WorkflowKind.__members__
    assert not hasattr(AutomationRequest, "for_explicit_engage")


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
        [type("Candidate", (), {"model_dump": lambda self, mode="json": {"tweet_id": "1", "text": "langgraph guide"}})()]
    )

    assert results[0].tweet_id == "1"
    assert results[0].allowed is True
    assert prompts[0][0] == (
        "Review Twitter feed candidates for reply safety. Reject anything about crime, violence, fraud, scams, drugs, war, military conflict, law enforcement, case news, adult or NSFW content, hate or harassment, self-harm or dangerous behavior, gambling or illicit activity, extremism, crypto shilling or guaranteed-profit investment claims, and medical or legal high-risk advice. Allow technical, product, engineering, builder, developer-adjacent, pets and animals, lifestyle, food, travel, scenic photography, entertainment, memes, and casual social content. Always allow posts from @elonmusk. Return JSON with a results array of {tweet_id, allowed, category, reason}."
    )


def test_feed_engage_allows_elonmusk_candidates_even_if_ai_moderation_rejects(monkeypatch, tmp_path: Path) -> None:
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
            return [FakeTweet("111", "elonmusk", "DOGE update and government fraud thread")]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "elonmusk", "DOGE full text and government fraud details")

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            return [
                type(
                    "Moderation",
                    (),
                    {
                        "tweet_id": candidate.tweet_id,
                        "allowed": False,
                        "category": "Politics/Government",
                        "reason": "would normally be rejected",
                    },
                )()
                for candidate in candidates
            ]

        def select_candidate(self, candidates):
            assert [candidate.tweet_id for candidate in candidates] == ["111"]
            return type("Selection", (), {"tweet_id": "111", "reason": "only remaining candidate"})()

        def draft_reply(self, candidate, context=None):
            return type("Draft", (), {"text": "Execution follows incentive design, not slogans.", "rationale": "safe"})()

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: FakeAIProvider())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "elon-moderation-bypass.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 1, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert body["result"]["selected_candidate"]["tweet_id"] == "111"
    assert not any(
        event["node"] == "moderate_candidates" and event["message"] == "candidate filtered by ai moderation"
        for event in body["result"]["events"]
    )
    assert not any(
        event["node"] == "selected_candidate_review" and event["message"] == "selected candidate filtered by ai moderation"
        for event in body["result"]["events"]
    )


def test_openai_compatible_provider_uses_compact_reply_enhancement_payload() -> None:
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
        return '{"should_enrich":false,"reason":"base reply is already specific enough"}'

    provider._chat = fake_chat  # type: ignore[method-assign]

    decision = provider.decide_reply_enhancement(
        type(
            "Candidate",
            (),
            {
                "model_dump": lambda self, mode="json": {
                    "tweet_id": "1",
                    "screen_name": "builder",
                    "text": "preview text",
                    "metadata": {"raw": "should_not_be_sent"},
                }
            },
        )(),
        "preview text. The real test is whether it holds up in production.",
    )

    payload = __import__("json").loads(prompts[0][1])
    assert decision.should_enrich is False
    assert payload["candidate"] == {
        "tweet_id": "1",
        "screen_name": "builder",
        "text": "preview text",
    }
    assert "metadata" not in payload["candidate"]


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


def test_feed_engage_uses_non_technical_reply_style_for_pet_content(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {
                "id": tweet_id,
                "text": text,
                "media": [{"type": "photo"}],
                "author": {"screenName": screen_name, "verified": verified},
            }
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet("111", "cutepets", "Cloud puppy")]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "cutepets", "Cloud puppy")

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            return [
                type("Moderation", (), {"tweet_id": "111", "allowed": True, "category": None, "reason": "allowed"})()
                for _ in candidates
            ]

        def select_candidate(self, candidates):
            return type("Selection", (), {"tweet_id": "111", "reason": "only candidate"})()

        def classify_reply_style(self, candidate):
            return type("Style", (), {"style": "non_technical", "reason": "pet content"})()

        def draft_reply(self, candidate, context=None):
            assert context is not None
            assert context["reply_style"] == "non_technical"
            return type("Draft", (), {"text": "That dog looks absurdly calm for a cloud.", "rationale": "light observation"})()

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: FakeAIProvider())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "nontechnical-style.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 1, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert any(
        event["node"] == "draft_reply"
        and event["message"] == "reply style selected"
        and event["payload"]["reply_style"] == "non_technical"
        for event in body["result"]["events"]
    )


def test_feed_engage_normalizes_selection_reason_for_non_technical_content(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {
                "id": tweet_id,
                "text": text,
                "media": [{"type": "photo"}],
                "metrics": {"likes": 42, "replies": 2, "views": 240},
                "author": {"screenName": screen_name, "verified": verified},
            }
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet("111", "cutepets", "Cloud puppy")]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "cutepets", "Cloud puppy")

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            return [
                type("Moderation", (), {"tweet_id": "111", "allowed": True, "category": None, "reason": "allowed"})()
                for _ in candidates
            ]

        def select_candidate(self, candidates):
            return type(
                "Selection",
                (),
                {
                    "tweet_id": "111",
                    "reason": "Highest engagement metrics and high-resolution media make it optimal for technical analysis of content distribution and platform image delivery optimization.",
                },
            )()

        def classify_reply_style(self, candidate):
            return type("Style", (), {"style": "non_technical", "reason": "pet content"})()

        def draft_reply(self, candidate, context=None):
            return type("Draft", (), {"text": "That pup looks completely at ease.", "rationale": "soft tone"})()

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: FakeAIProvider())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "nontechnical-selection-reason.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 1, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    reason = body["result"]["result"]["detail"]["selection_reason"]
    assert reason == "Warm pet post, strong image appeal, and healthy engagement."


def test_ai_auto_uses_single_selection_and_single_draft(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {"id": tweet_id, "text": text}
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeCommand:
        def __init__(self, action: str, ok: bool = True, tweet_id: str | None = None):
            self.action = action
            self.ok = ok
            self.dry_run = False
            self.target_tweet_id = "111"
            self.tweet_id = tweet_id
            self.screen_name = "demoauthor"
            self.text = "hello"
            self.payload = {"ok": ok}
            self.error_code = None
            self.error_message = None

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def __init__(self):
            self.feed_calls = 0
            self.reply_calls: list[tuple[str, str]] = []

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            self.feed_calls += 1
            if self.feed_calls > 1:
                raise AssertionError("run should not refetch feed")
            return [FakeTweet("111", "demoauthor", "interesting candidate")]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "demoauthor", "interesting candidate")

        def reply(self, tweet_id: str, text: str):
            self.reply_calls.append((tweet_id, text))
            if text != "frozen draft":
                raise AssertionError(f"run should reuse drafted text, got: {text}")
            return FakeCommand("reply", ok=True, tweet_id="reply_locked")

        def follow(self, screen_name: str):
            return FakeCommand("follow", ok=True)

    class FakeAIProvider:
        def __init__(self):
            self.draft_calls = 0

        def select_candidate(self, candidates):
            return type("Selection", (), {"tweet_id": candidates[0].tweet_id, "reason": "pick first"})()

        def draft_reply(self, candidate):
            self.draft_calls += 1
            if self.draft_calls == 1:
                return type("Draft", (), {"text": "frozen draft", "rationale": "first"})()
            return type("Draft", (), {"text": "new draft", "rationale": "second"})()

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    fake_client = FakeClient()
    fake_provider = FakeAIProvider()

    monkeypatch.setattr(
        "x_atuo.automation.graph.TwitterClient.from_config",
        lambda *args, **kwargs: fake_client,
    )
    monkeypatch.setattr(
        "x_atuo.automation.graph.build_ai_provider",
        lambda settings: fake_provider,
    )
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "ai-auto-single.sqlite3"))

    with TestClient(app) as client:
        queued = client.post(
            "/hooks/twitter/feed-engage",
            json={"feed_count": 1, "mode": "ai_auto", "dry_run": False},
        )
        assert queued.status_code == 202
        body = queued.json()
        assert body["status"] == "completed"
        detail = body["result"]["result"]["detail"]
        assert detail["selected_by"] == "ai"
        assert detail["drafted_by"] == "ai"
        assert detail["selection_reason"] == "pick first"

    assert fake_client.feed_calls == 1
    assert fake_provider.draft_calls == 1
    assert fake_client.reply_calls == [("111", "frozen draft")]


def test_feed_engage_ai_moderation_filters_disallowed_candidates(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {"id": tweet_id, "text": text}
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [
                FakeTweet("111", "newshandle", "Breaking case update from law enforcement"),
                FakeTweet("222", "builder", "LangGraph caching trims most of the orchestration overhead"),
            ]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "builder", "selected")

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            if len(candidates) == 2:
                return [
                    type("Moderation", (), {
                        "tweet_id": candidates[0].tweet_id,
                        "allowed": False,
                        "category": "law_enforcement",
                        "reason": "law enforcement case news",
                    })(),
                    type("Moderation", (), {
                        "tweet_id": candidates[1].tweet_id,
                        "allowed": True,
                        "category": None,
                        "reason": "technical content",
                    })(),
                ]
            assert len(candidates) == 1
            return [
                type("Moderation", (), {
                    "tweet_id": candidates[0].tweet_id,
                    "allowed": True,
                    "category": None,
                    "reason": "technical content",
                })(),
            ]

        def select_candidate(self, candidates):
            assert [candidate.tweet_id for candidate in candidates] == ["222"]
            return type("Selection", (), {"tweet_id": "222", "reason": "safe technical pick"})()

        def draft_reply(self, candidate):
            return type("Draft", (), {"text": "Nice breakdown. This keeps the workflow a lot cleaner.", "rationale": "safe"})()

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    monkeypatch.setattr(
        "x_atuo.automation.graph.TwitterClient.from_config",
        lambda *args, **kwargs: FakeClient(),
    )
    monkeypatch.setattr(
        "x_atuo.automation.graph.build_ai_provider",
        lambda settings: FakeAIProvider(),
    )
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "moderation-filter.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 2, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert body["result"]["selected_candidate"]["tweet_id"] == "222"
    assert any(
        event["node"] == "moderate_candidates" and event["message"] == "candidate filtered by ai moderation"
        for event in body["result"]["events"]
    )


def test_feed_engage_ai_moderation_is_fail_closed_for_missing_decisions(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {"id": tweet_id, "text": text}
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [
                FakeTweet("111", "builder", "langgraph caching overview"),
                FakeTweet("222", "other", "another technical thread"),
            ]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "builder", "full technical thread")

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            return [
                type("Moderation", (), {
                    "tweet_id": "111",
                    "allowed": True,
                    "category": None,
                    "reason": "technical content",
                })(),
            ]

        def select_candidate(self, candidates):
            assert [candidate.tweet_id for candidate in candidates] == ["111"]
            return type("Selection", (), {"tweet_id": "111", "reason": "only explicitly allowed candidate"})()

        def draft_reply(self, candidate):
            return type("Draft", (), {"text": "Nice breakdown. This keeps the workflow a lot cleaner.", "rationale": "safe"})()

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    monkeypatch.setattr(
        "x_atuo.automation.graph.TwitterClient.from_config",
        lambda *args, **kwargs: FakeClient(),
    )
    monkeypatch.setattr(
        "x_atuo.automation.graph.build_ai_provider",
        lambda settings: FakeAIProvider(),
    )
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "moderation-fail-closed.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 2, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert body["result"]["selected_candidate"]["tweet_id"] == "111"
    assert any(
        event["node"] == "moderate_candidates"
        and event["message"] == "candidate filtered by ai moderation"
        and event["payload"].get("tweet_id") == "222"
        for event in body["result"]["events"]
    )


def test_feed_engage_ai_moderation_uses_preview_text_before_shortlist_hydration(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {"id": tweet_id, "text": text}
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet("111", "preview", "harmless preview")]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "preview", "war footage and battlefield update")

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called when full-text moderation blocks candidate")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called when full-text moderation blocks candidate")

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            assert candidates[0].text == "harmless preview"
            return [
                type("Moderation", (), {
                    "tweet_id": candidates[0].tweet_id,
                    "allowed": False,
                    "category": "preview_rejected",
                    "reason": "preview content rejected",
                })(),
            ]

        def select_candidate(self, candidates):
            raise AssertionError("select_candidate should not be called after moderation blocks candidate")

        def draft_reply(self, candidate):
            raise AssertionError("draft_reply should not be called after moderation blocks candidate")

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    monkeypatch.setattr(
        "x_atuo.automation.graph.TwitterClient.from_config",
        lambda *args, **kwargs: FakeClient(),
    )
    monkeypatch.setattr(
        "x_atuo.automation.graph.build_ai_provider",
        lambda settings: FakeAIProvider(),
    )
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "moderation-preview.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 1, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "blocked"
    assert "all candidates filtered by ai moderation" in body["result"]["errors"]


def test_feed_engage_rechecks_selected_candidate_with_full_text(monkeypatch, tmp_path: Path) -> None:
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
            return [
                FakeTweet("111", "builder1", "safe preview one"),
                FakeTweet("222", "builder2", "safe preview two"),
            ]

        def fetch_tweet(self, tweet_id: str):
            if tweet_id == "111":
                return FakeTweet("111", "builder1", "war footage and battlefield update")
            return FakeTweet("222", "builder2", "production caching rollout details")

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            if len(candidates) == 2:
                assert [candidate.text for candidate in candidates] == ["safe preview one", "safe preview two"]
                return [
                    type("Moderation", (), {"tweet_id": candidate.tweet_id, "allowed": True, "category": None, "reason": "preview ok"})()
                    for candidate in candidates
                ]
            candidate = candidates[0]
            if candidate.tweet_id == "111":
                assert candidate.text == "war footage and battlefield update"
                return [
                    type("Moderation", (), {"tweet_id": "111", "allowed": False, "category": "war", "reason": "war topic"})()
                ]
            assert candidate.text == "production caching rollout details"
            return [
                type("Moderation", (), {"tweet_id": "222", "allowed": True, "category": None, "reason": "full text ok"})()
            ]

        def select_candidate(self, candidates):
            if len(candidates) == 2:
                return type("Selection", (), {"tweet_id": "111", "reason": "pick first then recheck"})()
            return type("Selection", (), {"tweet_id": "222", "reason": "fallback after full review"})()

        def draft_reply(self, candidate, context=None):
            return type("Draft", (), {"text": "This rollout path looks much safer.", "rationale": "full review passed"})()

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: FakeAIProvider())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "selected-full-review.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 2, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert body["result"]["selected_candidate"]["tweet_id"] == "222"
    assert any(
        event["node"] == "selected_candidate_review"
        and event["message"] == "selected candidate filtered by ai moderation"
        and event["payload"]["tweet_id"] == "111"
        for event in body["result"]["events"]
    )


def test_feed_engage_uses_enriched_full_text_after_moderation(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {"id": tweet_id, "text": text}
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [
                FakeTweet("111", "builder1", "short preview"),
                FakeTweet("222", "builder2", "another preview"),
            ]

        def fetch_tweet(self, tweet_id: str):
            if tweet_id == "111":
                return FakeTweet("111", "builder1", "Deep dive into langgraph caching and orchestration tradeoffs")
            return FakeTweet("222", "builder2", "Minor UI update note")

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            return [
                type("Moderation", (), {
                    "tweet_id": candidate.tweet_id,
                    "allowed": True,
                    "category": None,
                    "reason": "technical content",
                })()
                for candidate in candidates
            ]

        def select_candidate(self, candidates):
            assert candidates[0].text == "Deep dive into langgraph caching and orchestration tradeoffs"
            assert candidates[1].text == "Minor UI update note"
            return type("Selection", (), {"tweet_id": "111", "reason": "full text is richer"})()

        def draft_reply(self, candidate):
            assert candidate.text == "Deep dive into langgraph caching and orchestration tradeoffs"
            return type("Draft", (), {"text": "Nice breakdown. The tradeoffs are easy to follow here.", "rationale": "full text used"})()

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    monkeypatch.setattr(
        "x_atuo.automation.graph.TwitterClient.from_config",
        lambda *args, **kwargs: FakeClient(),
    )
    monkeypatch.setattr(
        "x_atuo.automation.graph.build_ai_provider",
        lambda settings: FakeAIProvider(),
    )
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "moderation-enriched.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 2, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert body["result"]["selected_candidate"]["tweet_id"] == "111"


def test_feed_engage_reuses_prefetched_tweet_during_execute(monkeypatch, tmp_path: Path) -> None:
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

        def __init__(self):
            self.fetch_tweet_calls = 0

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [
                FakeTweet("111", "builder1", "preview one"),
                FakeTweet("222", "builder2", "preview two"),
            ]

        def fetch_tweet(self, tweet_id: str):
            self.fetch_tweet_calls += 1
            if tweet_id == "111":
                return FakeTweet("111", "builder1", "Deep dive into orchestration tradeoffs")
            return FakeTweet("222", "builder2", "Minor release note")

        def reply(self, tweet_id: str, text: str):
            return type("Reply", (), {
                "action": "reply",
                "ok": True,
                "dry_run": False,
                "target_tweet_id": tweet_id,
                "tweet_id": "reply_prefetched",
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
                type("Moderation", (), {
                    "tweet_id": candidate.tweet_id,
                    "allowed": True,
                    "category": None,
                    "reason": "technical content",
                })()
                for candidate in candidates
            ]

        def select_candidate(self, candidates):
            return type("Selection", (), {"tweet_id": "111", "reason": "richer technical angle"})()

        def draft_reply(self, candidate):
            return type("Draft", (), {"text": "The real shift here is where the bottleneck moves next.", "rationale": "angle"})()

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    fake_client = FakeClient()
    monkeypatch.setattr(
        "x_atuo.automation.graph.TwitterClient.from_config",
        lambda *args, **kwargs: fake_client,
    )
    monkeypatch.setattr(
        "x_atuo.automation.graph.build_ai_provider",
        lambda settings: FakeAIProvider(),
    )
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "prefetched-execute.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"feed_count": 2, "mode": "ai_auto", "dry_run": False},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert fake_client.fetch_tweet_calls == 2


def test_feed_engage_hydrates_shortlist_before_selection(monkeypatch, tmp_path: Path) -> None:
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

        def __init__(self):
            self._lock = threading.Lock()
            self.active_fetches = 0
            self.max_concurrent_fetches = 0
            self.fetch_calls: list[str] = []

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [
                FakeTweet("111", "builder1", "preview one"),
                FakeTweet("222", "builder2", "preview two"),
                FakeTweet("333", "builder3", "preview three"),
            ]

        def fetch_tweet(self, tweet_id: str):
            self.fetch_calls.append(tweet_id)
            with self._lock:
                self.active_fetches += 1
                self.max_concurrent_fetches = max(self.max_concurrent_fetches, self.active_fetches)
            try:
                time.sleep(0.05)
                return FakeTweet(tweet_id, f"builder{tweet_id}", f"full text {tweet_id}")
            finally:
                with self._lock:
                    self.active_fetches -= 1

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            return [
                type("Moderation", (), {
                    "tweet_id": candidate.tweet_id,
                    "allowed": True,
                    "category": None,
                    "reason": "technical content",
                })()
                for candidate in candidates
            ]

        def select_candidate(self, candidates):
            return type("Selection", (), {"tweet_id": candidates[0].tweet_id, "reason": "first candidate"})()

        def draft_reply(self, candidate, context=None):
            return type("Draft", (), {"text": "The bottleneck moves to orchestration next.", "rationale": "angle"})()

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    fake_client = FakeClient()
    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: fake_client)
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: FakeAIProvider())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "moderation-concurrency.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 3, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert sorted(fake_client.fetch_calls) == ["111", "222", "333"]
    assert fake_client.max_concurrent_fetches > 1


def test_feed_engage_records_lightweight_node_observability(monkeypatch, tmp_path: Path) -> None:
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
            return [
                FakeTweet("111", "builder1", "preview one"),
                FakeTweet("222", "builder2", "preview two"),
            ]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, f"builder{tweet_id}", f"full text {tweet_id}")

        def reply(self, tweet_id: str, text: str):
            return type("Reply", (), {
                "action": "reply",
                "ok": True,
                "dry_run": False,
                "target_tweet_id": tweet_id,
                "tweet_id": f"reply_{tweet_id}",
                "screen_name": f"builder{tweet_id}",
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

        def select_candidate(self, candidates):
            return type("Selection", (), {"tweet_id": candidates[0].tweet_id, "reason": "first candidate"})()

        def draft_reply(self, candidate, context=None):
            return type("Draft", (), {"text": "Tighter execution beats bigger prompts.", "rationale": "angle"})()

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: FakeAIProvider())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "observability.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": False, "feed_count": 2, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"

    events = {(event["node"], event["message"]): event["payload"] for event in body["result"]["events"]}

    fetch_payload = events[("fetch_feed", "feed fetched")]
    assert fetch_payload["candidate_source"] == "feed"
    assert fetch_payload["cache_hit_count"] == 0
    assert fetch_payload["duration_ms"] >= 0

    moderation_payload = events[("moderate_candidates", "ai moderation completed")]
    assert moderation_payload["duration_ms"] >= 0

    select_payload = events[("select_candidate", "candidate selected")]
    assert select_payload["hydrated_count"] == 2
    assert select_payload["reused_hydrated_count"] == 0
    assert select_payload["hydration_duration_ms"] >= 0

    review_payload = events[("selected_candidate_review", "selected candidate passed full-text review")]
    assert review_payload["duration_ms"] >= 0

    draft_payload = events[("draft_reply", "base ai draft generated")]
    assert draft_payload["duration_ms"] >= 0

    execute_payload = events[("execute", "execution completed")]
    assert execute_payload["duration_ms"] >= 0
    assert execute_payload["attempt_count"] == 1


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

        def select_candidate(self, candidates):
            return type("Selection", (), {"tweet_id": candidates[0].tweet_id, "reason": "first"})()

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


def test_feed_engage_reuses_cached_shortlist_before_fetching_new_feed(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, created_at: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.created_at = datetime.fromisoformat(created_at)
            self.raw = {
                "id": tweet_id,
                "text": text,
                "createdAt": created_at,
                "author": {"screenName": screen_name, "verified": verified},
            }
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def __init__(self):
            self.fetch_feed_calls = 0
            self.fetch_tweet_calls = 0

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            self.fetch_feed_calls += 1
            return [
                FakeTweet("111", "author1", "preview-111", "2026-04-20T03:31:00+00:00"),
                FakeTweet("222", "author2", "preview-222", "2026-04-20T03:32:00+00:00"),
                FakeTweet("333", "author3", "preview-333", "2026-04-20T03:33:00+00:00"),
            ]

        def fetch_tweet(self, tweet_id: str):
            self.fetch_tweet_calls += 1
            mapping = {
                "111": FakeTweet("111", "author1", "full-111", "2026-04-20T03:31:00+00:00"),
                "222": FakeTweet("222", "author2", "full-222", "2026-04-20T03:32:00+00:00"),
                "333": FakeTweet("333", "author3", "full-333", "2026-04-20T03:33:00+00:00"),
            }
            return mapping[tweet_id]

        def reply(self, tweet_id: str, text: str):
            return type("Reply", (), {
                "action": "reply",
                "ok": True,
                "dry_run": False,
                "target_tweet_id": tweet_id,
                "tweet_id": f"reply_{tweet_id}",
                "screen_name": f"author_{tweet_id}",
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

        def select_candidate(self, candidates):
            return type("Selection", (), {"tweet_id": candidates[0].tweet_id, "reason": "take newest pending"})()

        def draft_reply(self, candidate, context=None):
            return type("Draft", (), {"text": f"replying to {candidate.tweet_id}", "rationale": "ok"})()

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    db_path = tmp_path / "candidate-cache-reuse.sqlite3"
    fake_client = FakeClient()
    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: fake_client)
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: FakeAIProvider())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(db_path))
    monkeypatch.setenv("X_ATUO_POLICIES__CANDIDATE_HYDRATION_COUNT", "3")

    with TestClient(app) as client:
        first = client.post("/hooks/twitter/feed-engage", json={"dry_run": False, "feed_count": 3, "mode": "ai_auto"})
        second = client.post("/hooks/twitter/feed-engage", json={"dry_run": False, "feed_count": 3, "mode": "ai_auto"})

    assert first.status_code == 202
    assert second.status_code == 202
    first_body = first.json()
    second_body = second.json()
    assert first_body["status"] == "completed"
    assert second_body["status"] == "completed"
    assert first_body["result"]["selected_candidate"]["tweet_id"] == "333"
    assert second_body["result"]["selected_candidate"]["tweet_id"] == "222"
    assert fake_client.fetch_feed_calls == 1
    assert fake_client.fetch_tweet_calls == 3

    storage = AutomationStorage(db_path)
    pending = storage.list_pending_candidate_cache(workflow="feed_engage", limit=10)
    assert pending == []
    with storage.connect() as connection:
        remaining = connection.execute(
            "SELECT tweet_id, status, claim_run_id FROM candidate_cache WHERE workflow = 'feed_engage'"
        ).fetchall()
    assert [(row["tweet_id"], row["status"], bool(row["claim_run_id"])) for row in remaining] == [("111", "claimed", True)]


def test_feed_engage_blocks_when_ai_moderation_filters_all_candidates(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {"id": tweet_id, "text": text}
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [
                FakeTweet("111", "campaign", "Election strategy thread"),
                FakeTweet("222", "crimebeat", "Drug bust update from local police"),
            ]

        def fetch_tweet(self, tweet_id: str):
            raise AssertionError("fetch_tweet should not be called when moderation removes everything")

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called when moderation removes everything")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called when moderation removes everything")

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            return [
                type("Moderation", (), {
                    "tweet_id": candidate.tweet_id,
                    "allowed": False,
                    "category": "unsafe_topic",
                    "reason": "disallowed topic",
                })()
                for candidate in candidates
            ]

        def select_candidate(self, candidates):
            raise AssertionError("select_candidate should not be called after all candidates are filtered")

        def draft_reply(self, candidate):
            raise AssertionError("draft_reply should not be called after all candidates are filtered")

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    monkeypatch.setattr(
        "x_atuo.automation.graph.TwitterClient.from_config",
        lambda *args, **kwargs: FakeClient(),
    )
    monkeypatch.setattr(
        "x_atuo.automation.graph.build_ai_provider",
        lambda settings: FakeAIProvider(),
    )
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "moderation-block.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 2, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "blocked"
    assert "all candidates filtered by ai moderation" in body["result"]["errors"]


def test_feed_engage_deterministic_skips_reply_context_enrichment(monkeypatch, tmp_path: Path) -> None:
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

        def __init__(self):
            self.thread_calls = 0
            self.profile_calls = 0
            self.user_posts_calls = 0

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet("111", "builder", "preview")]

        def fetch_tweet_thread(self, tweet_id: str, *, max_replies: int = 5):
            self.thread_calls += 1
            return (FakeTweet(tweet_id, "builder", "preview"), [])

        def fetch_user_profile(self, screen_name: str):
            self.profile_calls += 1
            return {"screen_name": screen_name}

        def fetch_user_posts(self, screen_name: str, *, max_items: int = 5):
            self.user_posts_calls += 1
            return []

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    fake_client = FakeClient()
    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: fake_client)
    monkeypatch.setenv("X_ATUO_AI__PROVIDER", "none")
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "deterministic-skip-context.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 1, "mode": "deterministic"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert fake_client.thread_calls == 0
    assert fake_client.profile_calls == 0
    assert fake_client.user_posts_calls == 0


def test_feed_engage_skips_previously_engaged_target_tweet(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {"id": tweet_id, "text": text}
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet("111", "demoauthor", "interesting candidate")]

        def fetch_tweet(self, tweet_id: str):
            raise AssertionError("fetch_tweet should not be called for an already engaged tweet")

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called for an already engaged tweet")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called for an already engaged tweet")

    monkeypatch.setattr(
        "x_atuo.automation.graph.TwitterClient.from_config",
        lambda *args, **kwargs: FakeClient(),
    )
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "target-dedupe.sqlite3"))

    storage = AutomationStorage(tmp_path / "target-dedupe.sqlite3")
    storage.initialize()
    storage.upsert_job("seed-job", "feed_engage", config={})
    storage.create_run(
        run_id="seed-run-1",
        job_id="seed-job",
        job_type="feed_engage",
        endpoint="seed",
        request_payload={"source": "seed"},
        status="completed",
    )
    storage.record_engagement(
        run_id="seed-run-1",
        target_tweet_id="111",
        target_author="demoauthor",
        target_tweet_url="https://x.com/demoauthor/status/111",
        reply_tweet_id="reply-seed",
        reply_url="https://x.com/i/status/reply-seed",
        followed=True,
    )

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"feed_count": 1, "mode": "deterministic", "dry_run": False},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "blocked"
    assert "target tweet already engaged" in (body["result"]["errors"] or [])


def test_feed_engage_ai_auto_retries_duplicate_candidate_before_enrichment(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {
                "id": tweet_id,
                "text": text,
                "author": {"screenName": screen_name, "verified": verified, "name": screen_name},
            }
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified, "name": screen_name})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [
                FakeTweet("111", "demoauthor", "already engaged preview"),
                FakeTweet("222", "freshauthor", "fresh preview"),
            ]

        def fetch_tweet(self, tweet_id: str):
            if tweet_id == "111":
                return FakeTweet("111", "demoauthor", "already engaged full text")
            return FakeTweet("222", "freshauthor", "fresh full text")

        def fetch_tweet_thread(self, tweet_id: str, *, max_replies: int = 5):
            if tweet_id == "111":
                raise AssertionError("duplicate candidates should be retried before enrichment")
            return (FakeTweet("222", "freshauthor", "fresh full text"), [])

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            return [
                type("Moderation", (), {
                    "tweet_id": candidate.tweet_id,
                    "allowed": True,
                    "category": None,
                    "reason": "technical content",
                })()
                for candidate in candidates
            ]

        def select_candidate(self, candidates):
            return type("Selection", (), {"tweet_id": candidates[0].tweet_id, "reason": "first remaining candidate"})()

        def draft_reply(self, candidate, context=None):
            if candidate.tweet_id == "111":
                raise AssertionError("duplicate candidates should be retried before drafting")
            return type("Draft", (), {"text": "The cleaner win is skipping duplicate work early.", "rationale": "fallback"})()

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: FakeAIProvider())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "ai-auto-early-retry.sqlite3"))

    storage = AutomationStorage(tmp_path / "ai-auto-early-retry.sqlite3")
    storage.initialize()
    storage.upsert_job("seed-job", "feed_engage", config={})
    storage.create_run(
        run_id="seed-run-1",
        job_id="seed-job",
        job_type="feed_engage",
        endpoint="seed",
        request_payload={"source": "seed"},
        status="completed",
    )
    storage.record_engagement(
        run_id="seed-run-1",
        target_tweet_id="111",
        target_author="demoauthor",
        target_tweet_url="https://x.com/demoauthor/status/111",
        reply_tweet_id="reply-seed",
        reply_url="https://x.com/i/status/reply-seed",
        followed=True,
    )

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 2, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert body["result"]["selected_candidate"]["tweet_id"] == "222"
    assert any(
        event["node"] == "prefilter_candidates" and event["message"] == "already-engaged candidates removed"
        for event in body["result"]["events"]
    )


def test_feed_engage_prefilters_already_engaged_candidates_before_selection(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {
                "id": tweet_id,
                "text": text,
                "author": {"screenName": screen_name, "verified": verified, "name": screen_name},
            }
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified, "name": screen_name})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [
                FakeTweet("111", "demoauthor", "already engaged preview"),
                FakeTweet("222", "freshauthor", "fresh preview"),
            ]

        def fetch_tweet(self, tweet_id: str):
            if tweet_id == "111":
                return FakeTweet("111", "demoauthor", "already engaged full text")
            return FakeTweet("222", "freshauthor", "fresh full text")

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            return [
                type("Moderation", (), {
                    "tweet_id": candidate.tweet_id,
                    "allowed": True,
                    "category": None,
                    "reason": "technical content",
                })()
                for candidate in candidates
            ]

        def select_candidate(self, candidates):
            assert [candidate.tweet_id for candidate in candidates] == ["222"]
            return type("Selection", (), {"tweet_id": "222", "reason": "only remaining candidate"})()

        def draft_reply(self, candidate, context=None):
            return type("Draft", (), {"text": "The cleaner win is skipping duplicate work early.", "rationale": "base draft"})()

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: FakeAIProvider())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "candidate-prefilter.sqlite3"))

    storage = AutomationStorage(tmp_path / "candidate-prefilter.sqlite3")
    storage.initialize()
    storage.upsert_job("seed-job", "feed_engage", config={})
    storage.create_run(
        run_id="seed-run-1",
        job_id="seed-job",
        job_type="feed_engage",
        endpoint="seed",
        request_payload={"source": "seed"},
        status="completed",
    )
    storage.record_engagement(
        run_id="seed-run-1",
        target_tweet_id="111",
        target_author="demoauthor",
        target_tweet_url="https://x.com/demoauthor/status/111",
        reply_tweet_id="reply-seed",
        reply_url="https://x.com/i/status/reply-seed",
        followed=True,
    )

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 2, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert body["result"]["selected_candidate"]["tweet_id"] == "222"
    assert any(
        event["node"] == "prefilter_candidates" and event["message"] == "already-engaged candidates removed"
        for event in body["result"]["events"]
    )


def test_feed_engage_refreshes_candidates_when_prefilter_empties_pool(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {
                "id": tweet_id,
                "text": text,
                "author": {"screenName": screen_name, "verified": verified, "name": screen_name},
            }
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified, "name": screen_name})()
            self.screen_name = screen_name
            self.verified = verified

    fetch_calls = {"count": 0}

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            fetch_calls["count"] += 1
            if fetch_calls["count"] == 1:
                return [FakeTweet("111", "demoauthor", "already engaged preview")]
            return [FakeTweet("222", "freshauthor", "fresh preview")]

        def fetch_tweet(self, tweet_id: str):
            if tweet_id == "111":
                return FakeTweet("111", "demoauthor", "already engaged full text")
            return FakeTweet("222", "freshauthor", "fresh full text")

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "candidate-refresh-success.sqlite3"))
    monkeypatch.setenv("X_ATUO_AI__PROVIDER", "none")

    storage = AutomationStorage(tmp_path / "candidate-refresh-success.sqlite3")
    storage.initialize()
    storage.upsert_job("seed-job", "feed_engage", config={})
    storage.create_run(
        run_id="seed-run-1",
        job_id="seed-job",
        job_type="feed_engage",
        endpoint="seed",
        request_payload={"source": "seed"},
        status="completed",
    )
    storage.record_engagement(
        run_id="seed-run-1",
        target_tweet_id="111",
        target_author="demoauthor",
        target_tweet_url="https://x.com/demoauthor/status/111",
        reply_tweet_id="reply-seed",
        reply_url="https://x.com/i/status/reply-seed",
        followed=True,
    )

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 1, "mode": "deterministic"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert body["result"]["selected_candidate"]["tweet_id"] == "222"
    assert body["result"]["candidate_refresh_count"] == 1
    assert fetch_calls["count"] == 2
    assert any(
        event["node"] == "prefilter_candidates" and event["message"] == "candidate pool empty, refreshing feed"
        for event in body["result"]["events"]
    )

    persisted_run = storage.get_run(body["run_id"])
    assert persisted_run is not None
    assert persisted_run["run"]["response_payload"]["candidate_refresh_count"] == 1


def test_feed_engage_blocks_after_candidate_refresh_limit(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {
                "id": tweet_id,
                "text": text,
                "author": {"screenName": screen_name, "verified": verified, "name": screen_name},
            }
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified, "name": screen_name})()
            self.screen_name = screen_name
            self.verified = verified

    fetch_calls = {"count": 0}

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            fetch_calls["count"] += 1
            return [FakeTweet("111", "demoauthor", "already engaged preview")]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet("111", "demoauthor", "already engaged full text")

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in blocked run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in blocked run")

    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "candidate-refresh-blocked.sqlite3"))
    monkeypatch.setenv("X_ATUO_AI__PROVIDER", "none")

    storage = AutomationStorage(tmp_path / "candidate-refresh-blocked.sqlite3")
    storage.initialize()
    storage.upsert_job("seed-job", "feed_engage", config={})
    storage.create_run(
        run_id="seed-run-1",
        job_id="seed-job",
        job_type="feed_engage",
        endpoint="seed",
        request_payload={"source": "seed"},
        status="completed",
    )
    storage.record_engagement(
        run_id="seed-run-1",
        target_tweet_id="111",
        target_author="demoauthor",
        target_tweet_url="https://x.com/demoauthor/status/111",
        reply_tweet_id="reply-seed",
        reply_url="https://x.com/i/status/reply-seed",
        followed=True,
    )

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 1, "mode": "deterministic"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "blocked"
    assert fetch_calls["count"] == 3
    assert any(
        event["node"] == "prefilter_candidates" and event["message"] == "candidate pool empty, refreshing feed"
        for event in body["result"]["events"]
    )
    assert body["result"]["errors"] == ["target tweet already engaged"]


def test_feed_engage_refreshes_when_feed_returns_no_candidates(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {"id": tweet_id, "text": text}
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    fetch_calls = {"count": 0}

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            fetch_calls["count"] += 1
            if fetch_calls["count"] == 1:
                return []
            return [FakeTweet("222", "freshauthor", "fresh candidate")]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet("222", "freshauthor", "fresh candidate")

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "feed-empty-refresh.sqlite3"))
    monkeypatch.setenv("X_ATUO_AI__PROVIDER", "none")

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 1, "mode": "deterministic"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert body["result"]["selected_candidate"]["tweet_id"] == "222"
    assert body["result"]["candidate_refresh_count"] == 1
    assert fetch_calls["count"] == 2
    assert any(
        event["node"] == "fetch_feed" and event["message"] == "candidate pool empty, refreshing feed"
        for event in body["result"]["events"]
    )


def test_feed_engage_prefilters_unverified_authors_before_selection(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {"id": tweet_id, "text": text}
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [
                FakeTweet("111", "unverified_author", "unverified candidate", verified=False),
                FakeTweet("222", "verified_author", "verified candidate", verified=True),
            ]

        def fetch_tweet(self, tweet_id: str):
            if tweet_id == "111":
                return FakeTweet("111", "unverified_author", "unverified candidate", verified=False)
            return FakeTweet("222", "verified_author", "verified candidate", verified=True)

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            return [
                type("Moderation", (), {
                    "tweet_id": candidate.tweet_id,
                    "allowed": True,
                    "category": None,
                    "reason": "allowed",
                })()
                for candidate in candidates
            ]

        def select_candidate(self, candidates):
            assert [candidate.tweet_id for candidate in candidates] == ["222"]
            return type("Selection", (), {"tweet_id": "222", "reason": "only verified candidate remains"})()

        def draft_reply(self, candidate, context=None):
            return type("Draft", (), {"text": "Verified accounts keep the signal cleaner.", "rationale": "base draft"})()

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: FakeAIProvider())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "prefilter-unverified.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 2, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert body["result"]["selected_candidate"]["tweet_id"] == "222"
    assert any(
        event["node"] == "prefilter_candidates" and event["message"] == "unverified candidates removed"
        for event in body["result"]["events"]
    )
    assert any(
        event["node"] == "prefilter_candidates" and event["message"] == "candidate removed before selection" and event["payload"]["reason"] == "author not verified"
        for event in body["result"]["events"]
    )


def test_feed_engage_blocks_with_unverified_reason_when_prefilter_removes_everything(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {"id": tweet_id, "text": text}
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    fetch_calls = {"count": 0}

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            fetch_calls["count"] += 1
            return [FakeTweet("111", "ghost_author", "candidate", verified=False)]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet("111", "ghost_author", "candidate", verified=False)

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called")

    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "prefilter-unverified-blocked.sqlite3"))
    monkeypatch.setenv("X_ATUO_AI__PROVIDER", "none")

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 1, "mode": "deterministic"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "blocked"
    assert body["result"]["errors"] == ["author not verified"]
    assert fetch_calls["count"] == 3
    assert any(
        event["node"] == "prefilter_candidates" and event["message"] == "unverified candidates removed"
        for event in body["result"]["events"]
    )
    assert not any(
        event["node"] == "prefilter_candidates" and event["message"] == "already-engaged candidates removed"
        for event in body["result"]["events"]
    )


def test_feed_engage_refreshes_after_no_candidate_succeeded(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {"id": tweet_id, "text": text}
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    fetch_calls = {"count": 0}

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            fetch_calls["count"] += 1
            if fetch_calls["count"] == 1:
                return [FakeTweet("111", "restricted_author", "first candidate", verified=True)]
            return [FakeTweet("222", "verified_author", "second candidate", verified=True)]

        def fetch_tweet(self, tweet_id: str):
            if tweet_id == "111":
                return FakeTweet("111", "restricted_author", "first candidate", verified=True)
            return FakeTweet("222", "verified_author", "second candidate", verified=True)

        def reply(self, tweet_id: str, text: str):
            if tweet_id == "111":
                return type("Reply", (), {
                    "action": "reply",
                    "ok": False,
                    "dry_run": False,
                    "target_tweet_id": tweet_id,
                    "tweet_id": None,
                    "screen_name": "restricted_author",
                    "text": text,
                    "payload": {"ok": False},
                    "error_code": "433",
                    "error_message": "Restricted who can reply (433)",
                })()
            if tweet_id != "222":
                raise AssertionError("reply should only run for refreshed candidate")
            return type("Reply", (), {
                "action": "reply",
                "ok": True,
                "dry_run": False,
                "target_tweet_id": tweet_id,
                "tweet_id": "reply_after_refresh",
                "screen_name": "verified_author",
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

    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "execute-refresh.sqlite3"))
    monkeypatch.setenv("X_ATUO_AI__PROVIDER", "none")

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"feed_count": 1, "mode": "deterministic", "dry_run": False},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert body["result"]["result"]["target_tweet_id"] == "222"
    assert body["result"]["candidate_refresh_count"] == 1
    assert fetch_calls["count"] == 2
    assert any(
        event["node"] == "execute" and event["message"] == "candidate pool empty, refreshing feed" and event["payload"]["reason"] == "No candidate succeeded"
        for event in body["result"]["events"]
    )


def test_feed_engage_falls_back_to_next_candidate_when_first_already_engaged(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {"id": tweet_id, "text": text}
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [
                FakeTweet("111", "demoauthor", "already engaged"),
                FakeTweet("222", "freshauthor", "fresh candidate"),
            ]

        def fetch_tweet(self, tweet_id: str):
            if tweet_id != "222":
                raise AssertionError("fetch_tweet should only run for the fallback candidate")
            return FakeTweet("222", "freshauthor", "fresh candidate")

        def reply(self, tweet_id: str, text: str):
            if tweet_id != "222":
                raise AssertionError("reply should only run for the fallback candidate")
            return type("Reply", (), {
                "action": "reply",
                "ok": True,
                "dry_run": False,
                "target_tweet_id": tweet_id,
                "tweet_id": "reply_fallback",
                "screen_name": "freshauthor",
                "text": text,
                "payload": {"ok": True},
                "error_code": None,
                "error_message": None,
            })()

        def follow(self, screen_name: str):
            if screen_name != "freshauthor":
                raise AssertionError("follow should only run for the fallback candidate")
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

    monkeypatch.setattr(
        "x_atuo.automation.graph.TwitterClient.from_config",
        lambda *args, **kwargs: FakeClient(),
    )
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "target-dedupe-fallback.sqlite3"))
    monkeypatch.setenv("X_ATUO_AI__PROVIDER", "none")

    storage = AutomationStorage(tmp_path / "target-dedupe-fallback.sqlite3")
    storage.initialize()
    storage.upsert_job("seed-job", "feed_engage", config={})
    storage.create_run(
        run_id="seed-run-1",
        job_id="seed-job",
        job_type="feed_engage",
        endpoint="seed",
        request_payload={"source": "seed"},
        status="completed",
    )
    storage.record_engagement(
        run_id="seed-run-1",
        target_tweet_id="111",
        target_author="demoauthor",
        target_tweet_url="https://x.com/demoauthor/status/111",
        reply_tweet_id="reply-seed",
        reply_url="https://x.com/i/status/reply-seed",
        followed=True,
    )

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"feed_count": 2, "mode": "deterministic", "dry_run": False},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert body["result"]["result"]["target_tweet_id"] == "222"
    assert body["result"]["result"]["reply_url"] == "https://x.com/i/status/reply_fallback"
    assert any(
        event["node"] == "prefilter_candidates" and event["message"] == "already-engaged candidates removed"
        for event in body["result"]["events"]
    )


def test_feed_engage_falls_back_after_reply_restricted(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {"id": tweet_id, "text": text}
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def __init__(self):
            self.reply_calls: list[str] = []

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            if not hasattr(self, "fetch_count"):
                self.fetch_count = 0
            self.fetch_count += 1
            if self.fetch_count == 1:
                return [
                    FakeTweet("111", "restrictedauthor", "restricted candidate"),
                    FakeTweet("222", "cachedauthor", "fallback candidate"),
                ]
            return [FakeTweet("333", "freshauthor", "fresh candidate")]

        def fetch_tweet(self, tweet_id: str):
            if tweet_id == "111":
                return FakeTweet("111", "restrictedauthor", "restricted candidate")
            if tweet_id == "222":
                return FakeTweet("222", "cachedauthor", "fallback candidate")
            if tweet_id == "333":
                return FakeTweet("333", "freshauthor", "fresh candidate")
            raise AssertionError("unexpected candidate")

        def reply(self, tweet_id: str, text: str):
            self.reply_calls.append(tweet_id)
            if tweet_id == "111":
                return type("Reply", (), {
                    "action": "reply",
                    "ok": False,
                    "dry_run": False,
                    "target_tweet_id": tweet_id,
                    "tweet_id": None,
                    "screen_name": "restrictedauthor",
                    "text": text,
                    "payload": {"ok": False},
                    "error_code": "433",
                    "error_message": "Restricted who can reply (433)",
                })()
            if tweet_id == "222":
                return type("Reply", (), {
                    "action": "reply",
                    "ok": True,
                    "dry_run": False,
                    "target_tweet_id": tweet_id,
                    "tweet_id": "reply_after_restricted",
                    "screen_name": "cachedauthor",
                    "text": text,
                    "payload": {"ok": True},
                    "error_code": None,
                    "error_message": None,
                })()
            if tweet_id == "333":
                raise AssertionError("refresh should reuse cached shortlist before fetching fresh feed")
            raise AssertionError("unexpected candidate")

        def follow(self, screen_name: str):
            if screen_name != "cachedauthor":
                raise AssertionError("follow should only run for the successful fallback candidate")
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

    monkeypatch.setattr(
        "x_atuo.automation.graph.TwitterClient.from_config",
        lambda *args, **kwargs: FakeClient(),
    )
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "reply-restricted-fallback.sqlite3"))
    monkeypatch.setenv("X_ATUO_AI__PROVIDER", "none")

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"feed_count": 2, "mode": "deterministic", "dry_run": False},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert body["result"]["result"]["target_tweet_id"] == "222"
    assert body["result"]["result"]["reply_url"] == "https://x.com/i/status/reply_after_restricted"
    assert body["result"]["candidate_refresh_count"] == 1
    attempts = body["result"]["result"]["detail"]["attempts"]
    assert attempts[0]["outcome"] == "reply_restricted"
    assert attempts[0]["tweet_id"] == "111"
    assert attempts[1]["outcome"] == "replied"
    assert attempts[1]["tweet_id"] == "222"
    assert any(
        event["node"] == "execute" and event["message"] == "candidate pool empty, refreshing feed" and event["payload"]["reason"] == "No candidate succeeded"
        for event in body["result"]["events"]
    )


def test_feed_engage_ai_draft_does_not_use_author_history_or_reply_context(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {
                "id": tweet_id,
                "text": text,
                "author": {"screenName": screen_name, "verified": verified, "name": "Tech Dev Notes"},
            }
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified, "name": "Tech Dev Notes"})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def __init__(self):
            self.thread_calls = 0

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet("111", "techdevnotes", "Grok 4.3 has December 2025 knowledge cutoff")]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "techdevnotes", "Grok 4.3 has December 2025 knowledge cutoff")

        def fetch_tweet_thread(self, tweet_id: str, *, max_replies: int = 5):
            self.thread_calls += 1
            return (
                FakeTweet(tweet_id, "techdevnotes", "Grok 4.3 has December 2025 knowledge cutoff"),
                [],
            )

        def fetch_user_profile(self, screen_name: str):
            return {
                "screen_name": screen_name,
                "name": "Tech Dev Notes",
                "verified": True,
                "description": "AI product notes and model updates",
            }

        def fetch_user_posts(self, screen_name: str, *, max_items: int = 5):
            return [
                FakeTweet("h1", screen_name, "Model evals matter more than benchmark headlines."),
                FakeTweet("h2", screen_name, "Latency and orchestration still decide production UX."),
                FakeTweet("h3", screen_name, "Retrieval quality is where a lot of assistants still break down."),
                FakeTweet("h4", screen_name, "Tool calling is only useful if the fallback path is visible."),
                FakeTweet("h5", screen_name, "A newer cutoff helps, but product behavior matters more."),
            ]

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            return [
                type("Moderation", (), {"tweet_id": candidate.tweet_id, "allowed": True, "category": None, "reason": "technical"})()
                for candidate in candidates
            ]

        def select_candidate(self, candidates):
            return type("Selection", (), {"tweet_id": "111", "reason": "time-sensitive model update"})()

        def decide_reply_enhancement(self, candidate, base_reply_text):
            raise AssertionError("enrichment gate should not be called")

        def plan_reply_context(self, candidate, context):
            raise AssertionError("reply context planning should not be called")

        def draft_reply(self, candidate, context=None):
            assert context == {"reply_style": "technical"}
            return type(
                "Draft",
                (),
                {
                    "text": "December 2025 is still a pretty fresh cutoff.",
                    "rationale": "base ai draft only",
                },
            )()

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    fake_client = FakeClient()
    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: fake_client)
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: FakeAIProvider())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "reply-context.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 1, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert body["result"]["rendered_text"].startswith("December 2025 is still a pretty fresh cutoff.")
    assert fake_client.thread_calls == 0


def test_feed_engage_fallback_uses_tweet_text_without_extra_ai_draft(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {
                "id": tweet_id,
                "text": text,
                "author": {"screenName": screen_name, "verified": verified, "name": "Tech Dev Notes"},
            }
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified, "name": "Tech Dev Notes"})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def __init__(self):
            self.thread_calls = 0
            self.profile_calls = 0
            self.user_posts_calls = 0

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet("111", "techdevnotes", "Grok 4.3 has December 2025 knowledge cutoff")]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "techdevnotes", "Grok 4.3 has December 2025 knowledge cutoff")

        def fetch_tweet_thread(self, tweet_id: str, *, max_replies: int = 5):
            self.thread_calls += 1
            return (FakeTweet(tweet_id, "techdevnotes", "Grok 4.3 has December 2025 knowledge cutoff"), [])

        def fetch_user_profile(self, screen_name: str):
            self.profile_calls += 1
            return {"screen_name": screen_name, "name": "Tech Dev Notes", "verified": True}

        def fetch_user_posts(self, screen_name: str, *, max_items: int = 5):
            self.user_posts_calls += 1
            return [FakeTweet("h1", screen_name, "Recent launch notes")]

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            return [
                type("Moderation", (), {"tweet_id": candidate.tweet_id, "allowed": True, "category": None, "reason": "technical"})()
                for candidate in candidates
            ]

        def select_candidate(self, candidates):
            return type("Selection", (), {"tweet_id": "111", "reason": "needs context"})()

        def decide_reply_enhancement(self, candidate, base_reply_text):
            raise AssertionError("enrichment gate should not be called")

        def plan_reply_context(self, candidate, context):
            raise AssertionError("reply context planning should not be called")

        def draft_reply(self, candidate, context=None):
            raise AIProviderError("base ai draft failed")

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    fake_client = FakeClient()
    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: fake_client)
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: FakeAIProvider())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "reply-brief-compose.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 1, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert "grok 4.3 has december 2025 knowledge cutoff" in body["result"]["rendered_text"].lower()
    assert "retrieval is the default path" not in body["result"]["rendered_text"].lower()
    assert fake_client.thread_calls == 0
    assert fake_client.profile_calls == 0
    assert fake_client.user_posts_calls == 0


def test_feed_engage_ai_draft_does_not_use_live_search(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {
                "id": tweet_id,
                "text": text,
                "author": {"screenName": screen_name, "verified": verified, "name": "Tech Dev Notes"},
            }
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified, "name": "Tech Dev Notes"})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def __init__(self):
            self.thread_calls = 0

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet("111", "techdevnotes", "Grok 4.3 has December 2025 knowledge cutoff")]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "techdevnotes", "Grok 4.3 has December 2025 knowledge cutoff")

        def fetch_tweet_thread(self, tweet_id: str, *, max_replies: int = 5):
            self.thread_calls += 1
            return (FakeTweet(tweet_id, "techdevnotes", "Grok 4.3 has December 2025 knowledge cutoff"), [])

        def fetch_user_profile(self, screen_name: str):
            return {"screen_name": screen_name, "name": "Tech Dev Notes", "verified": True}

        def fetch_user_posts(self, screen_name: str, *, max_items: int = 5):
            return [FakeTweet("h1", screen_name, "Recent launch notes")]

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            return [
                type("Moderation", (), {"tweet_id": candidate.tweet_id, "allowed": True, "category": None, "reason": "technical"})()
                for candidate in candidates
            ]

        def select_candidate(self, candidates):
            return type("Selection", (), {"tweet_id": "111", "reason": "needs current context"})()

        def decide_reply_enhancement(self, candidate, base_reply_text):
            raise AssertionError("enrichment gate should not be called")

        def plan_reply_context(self, candidate, context):
            raise AssertionError("reply context planning should not be called")

        def draft_reply(self, candidate, context=None):
            assert context == {"reply_style": "technical"}
            return type(
                "Draft",
                (),
                {
                    "text": "December 2025 already sounds pretty fresh on its own.",
                    "rationale": "base ai draft only",
                },
            )()

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    fake_client = FakeClient()
    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: fake_client)
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: FakeAIProvider())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "reply-live-search.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 1, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert body["result"]["rendered_text"].startswith("December 2025 already sounds pretty fresh on its own.")
    assert fake_client.thread_calls == 0


def test_feed_engage_draft_reply_skips_enrichment_context(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {
                "id": tweet_id,
                "text": text,
                "author": {"screenName": screen_name, "verified": verified, "name": "Builder"},
            }
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified, "name": "Builder"})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def __init__(self):
            self.thread_calls = 0

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet("111", "builder", "A context-heavy technical post")]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "builder", "A context-heavy technical post")

        def fetch_tweet_thread(self, tweet_id: str, *, max_replies: int = 5):
            self.thread_calls += 1
            return (
                FakeTweet(tweet_id, "builder", "A context-heavy technical post"),
                [],
            )

        def fetch_user_profile(self, screen_name: str):
            return {
                "screen_name": screen_name,
                "name": "Builder",
                "verified": True,
                "description": "Builds agent systems",
                "raw": {"too": "large"},
            }

        def fetch_user_posts(self, screen_name: str, *, max_items: int = 5):
            return [
                FakeTweet("h1", screen_name, "Most failures come from orchestration, not the model."),
                FakeTweet("h2", screen_name, "Latency compounds across tool calls."),
                FakeTweet("h3", screen_name, "Retrieval defaults matter more than flashy demos."),
                FakeTweet("h4", screen_name, "Fallback paths need to be visible to operators."),
                FakeTweet("h5", screen_name, "Shipping UX beats benchmark wins."),
            ]

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            return [
                type("Moderation", (), {"tweet_id": candidate.tweet_id, "allowed": True, "category": None, "reason": "technical"})()
                for candidate in candidates
            ]

        def select_candidate(self, candidates):
            return type("Selection", (), {"tweet_id": "111", "reason": "good candidate"})()

        def decide_reply_enhancement(self, candidate, base_reply_text):
            raise AssertionError("enrichment gate should not be called")

        def plan_reply_context(self, candidate, context):
            raise AssertionError("reply context planning should not be called")

        def draft_reply(self, candidate, context=None):
            assert context == {"reply_style": "technical"}
            return type(
                "Draft",
                (),
                {
                    "text": "The real failure mode is when retrieval quietly becomes optional.",
                    "rationale": "base draft only",
                },
            )()

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    fake_client = FakeClient()
    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: fake_client)
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: FakeAIProvider())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "reply-compact-context.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 1, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert body["result"]["rendered_text"] == "The real failure mode is when retrieval quietly becomes optional."
    assert fake_client.thread_calls == 0


def test_feed_engage_clips_fallback_reply_to_280_chars(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {
                "id": tweet_id,
                "text": text,
                "author": {"screenName": screen_name, "verified": verified, "name": "Builder"},
            }
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified, "name": "Builder"})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet("111", "builder", "Long-form technical setup question " * 20)]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "builder", "Long-form technical setup question " * 20)

        def fetch_tweet_thread(self, tweet_id: str, *, max_replies: int = 5):
            return (FakeTweet(tweet_id, "builder", "Long-form technical setup question"), [])

        def fetch_user_profile(self, screen_name: str):
            return {"screen_name": screen_name, "name": "Builder", "verified": True, "description": "Builds agent systems"}

        def fetch_user_posts(self, screen_name: str, *, max_items: int = 5):
            return [FakeTweet("h1", screen_name, "History post")]

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            return [
                type("Moderation", (), {"tweet_id": candidate.tweet_id, "allowed": True, "category": None, "reason": "technical"})()
                for candidate in candidates
            ]

        def select_candidate(self, candidates):
            return type("Selection", (), {"tweet_id": "111", "reason": "good candidate"})()

        def decide_reply_enhancement(self, candidate, base_reply_text):
            raise AssertionError("enrichment gate should not be called")

        def plan_reply_context(self, candidate, context):
            raise AssertionError("enhancement should be skipped when lightweight gate says no")

        def draft_reply(self, candidate, context=None):
            raise AIProviderError("draft failed, force fallback")

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: FakeAIProvider())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "reply-fallback-clip.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 1, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert len(body["result"]["rendered_text"]) <= 280
    assert "long-form technical setup question" in body["result"]["rendered_text"].lower()
    assert "aaaaaaaaaa" not in body["result"]["rendered_text"].lower()


def test_feed_engage_uses_base_ai_draft_without_enrichment(monkeypatch, tmp_path: Path) -> None:
    class FakeTweet:
        def __init__(self, tweet_id: str, screen_name: str, text: str, verified: bool = True):
            self.tweet_id = tweet_id
            self.text = text
            self.raw = {
                "id": tweet_id,
                "text": text,
                "author": {"screenName": screen_name, "verified": verified, "name": "Builder"},
            }
            self.author = type("Author", (), {"screen_name": screen_name, "verified": verified, "name": "Builder"})()
            self.screen_name = screen_name
            self.verified = verified

    class FakeClient:
        credentials = type("Creds", (), {"ok": True})()

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [FakeTweet("111", "builder", "A concise original technical post")]

        def fetch_tweet(self, tweet_id: str):
            return FakeTweet(tweet_id, "builder", "A concise original technical post")

        def reply(self, tweet_id: str, text: str):
            raise AssertionError("reply should not be called in dry_run")

        def follow(self, screen_name: str):
            raise AssertionError("follow should not be called in dry_run")

    class FakeAIProvider:
        def moderate_candidates(self, candidates):
            return [
                type("Moderation", (), {"tweet_id": candidate.tweet_id, "allowed": True, "category": None, "reason": "technical"})()
                for candidate in candidates
            ]

        def select_candidate(self, candidates):
            return type("Selection", (), {"tweet_id": "111", "reason": "good candidate"})()

        def decide_reply_enhancement(self, candidate, base_reply_text):
            raise AssertionError("enrichment gate should not be called")

        def plan_reply_context(self, candidate, context):
            raise AssertionError("reply context planning should not be called")

        def draft_reply(self, candidate, context=None):
            assert context == {"reply_style": "technical"}
            return type(
                "Draft",
                (),
                {
                    "text": "The cleaner win here is how little setup this needs.",
                    "rationale": "base ai draft only",
                },
            )()

        def draft_repo_post(self, context):
            raise AssertionError("repo post drafting should not be called")

    monkeypatch.setattr("x_atuo.automation.graph.TwitterClient.from_config", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr("x_atuo.automation.graph.build_ai_provider", lambda settings: FakeAIProvider())
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "base-ai-draft-only.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 1, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "completed"
    assert body["result"]["drafted_by"] == "ai"
    assert body["result"]["rendered_text"] == "The cleaner win here is how little setup this needs."
