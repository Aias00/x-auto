from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from pathlib import Path

from fastapi.testclient import TestClient

from x_atuo.automation.api import app
from x_atuo.automation.config import AutomationConfig
from x_atuo.automation.config import AISettings
from x_atuo.automation.config import SchedulerSettings
from x_atuo.automation.schemas import FeedEngageRequest
from x_atuo.automation.policies import build_dedupe_key
from x_atuo.automation.scheduler import AutomationScheduler
from x_atuo.automation.state import AutomationRequest
from x_atuo.automation.storage import AutomationStorage
from x_atuo.core.ai_client import OpenAICompatibleProvider


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
    assert payload["feed_count"] == 5
    assert payload["feed_type"] == "for-you"


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
            return FakeTweet(tweet_id, screen_name, "selected")

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
    assert len(body["result"]["rendered_text"]) <= 120


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
        assert len(body["result"]["rendered_text"]) <= 120


def test_approve_route_removed() -> None:
    with TestClient(app) as client:
        response = client.post("/runs/does-not-exist/approve")
    assert response.status_code == 404


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
        event["node"] == "policy_guard" and event["message"] == "candidate blocked, trying next"
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

        def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None):
            return [
                FakeTweet("111", "restrictedauthor", "restricted candidate"),
                FakeTweet("222", "freshauthor", "fresh candidate"),
            ]

        def fetch_tweet(self, tweet_id: str):
            if tweet_id == "111":
                return FakeTweet("111", "restrictedauthor", "restricted candidate")
            if tweet_id == "222":
                return FakeTweet("222", "freshauthor", "fresh candidate")
            raise AssertionError("unexpected candidate")

        def reply(self, tweet_id: str, text: str):
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
                    "screen_name": "freshauthor",
                    "text": text,
                    "payload": {"ok": True},
                    "error_code": None,
                    "error_message": None,
                })()
            raise AssertionError("unexpected candidate")

        def follow(self, screen_name: str):
            if screen_name != "freshauthor":
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
    attempts = body["result"]["result"]["detail"]["attempts"]
    assert attempts[0]["outcome"] == "reply_restricted"
    assert attempts[1]["outcome"] == "replied"
