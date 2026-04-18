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
from x_atuo.automation.state import WorkflowKind
from x_atuo.automation.storage import AutomationStorage
import x_atuo.automation.api as automation_api
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
    assert len(body["result"]["rendered_text"]) <= 120


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
    assert body["result"]["rendered_text"] == "The real win here is how much complexity this strips out."


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
        "Review Twitter feed candidates for reply safety. Reject anything about politics, crime, violence, fraud, scams, drugs, war, military conflict, law enforcement, or case news. Allow technical, product, engineering, and builder content. Return JSON with a results array of {tweet_id, allowed, category, reason}."
    )


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


def test_feed_engage_ai_moderation_uses_full_tweet_text(monkeypatch, tmp_path: Path) -> None:
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
            assert candidates[0].text == "war footage and battlefield update"
            return [
                type("Moderation", (), {
                    "tweet_id": candidates[0].tweet_id,
                    "allowed": False,
                    "category": "war",
                    "reason": "war topic",
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
    monkeypatch.setenv("X_ATUO_DB_PATH", str(tmp_path / "moderation-full-text.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/hooks/twitter/feed-engage",
            json={"dry_run": True, "feed_count": 1, "mode": "ai_auto"},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "blocked"
    assert "all candidates filtered by ai moderation" in body["result"]["errors"]


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
