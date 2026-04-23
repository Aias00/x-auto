from __future__ import annotations

import asyncio
from importlib import import_module
import logging
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from x_atuo.automation import api as automation_api
from x_atuo.automation import graph as automation_graph
from x_atuo.automation.graph import AutomationGraph
from x_atuo.automation.config import AutomationConfig
from x_atuo.automation.state import AutomationRequest


def test_langfuse_langchain_import_surface_is_available() -> None:
    langfuse_langchain = import_module("langfuse.langchain")

    assert hasattr(langfuse_langchain, "CallbackHandler")


def test_langfuse_runtime_is_disabled_without_keys(monkeypatch) -> None:
    monkeypatch.delenv("LANGFUSE_PUBLIC_KEY", raising=False)
    monkeypatch.delenv("LANGFUSE_SECRET_KEY", raising=False)

    observability = import_module("x_atuo.automation.observability")

    runtime = observability.build_langfuse_runtime(AutomationConfig())

    assert runtime.is_enabled() is False
    assert runtime.build_graph_config(run_name="feed-engage") is None
    runtime.shutdown()


def test_langfuse_runtime_builds_graph_config_when_enabled(monkeypatch) -> None:
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-test")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-test")

    observability = import_module("x_atuo.automation.observability")

    fake_client = SimpleNamespace(shutdown=lambda: None)
    fake_callback = object()

    def fake_import_module(name: str):
        if name == "langfuse":
            return SimpleNamespace(get_client=lambda: fake_client)
        if name == "langfuse.langchain":
            return SimpleNamespace(CallbackHandler=lambda: fake_callback)
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(observability.importlib, "import_module", fake_import_module)

    runtime = observability.build_langfuse_runtime(AutomationConfig())
    graph_config = runtime.build_graph_config(run_name="feed-engage")

    assert runtime.is_enabled() is True
    assert graph_config == {"callbacks": [fake_callback], "run_name": "feed-engage"}
    runtime.shutdown()


def test_langfuse_runtime_degrades_to_disabled_on_import_failure(monkeypatch, caplog) -> None:
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-test")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-test")
    caplog.set_level(logging.ERROR)

    observability = import_module("x_atuo.automation.observability")

    def fake_import_module(name: str):
        if name == "langfuse":
            raise ModuleNotFoundError(name)
        raise AssertionError(f"unexpected import: {name}")

    monkeypatch.setattr(observability.importlib, "import_module", fake_import_module)

    runtime = observability.build_langfuse_runtime(AutomationConfig())

    assert runtime.is_enabled() is False
    assert runtime.build_graph_config(run_name="feed-engage") is None
    assert "langfuse import failed" in caplog.text


def test_langfuse_runtime_degrades_to_disabled_on_initialization_failure(monkeypatch, caplog) -> None:
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-test")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-test")
    caplog.set_level(logging.ERROR)

    observability = import_module("x_atuo.automation.observability")

    def fake_import_module(name: str):
        if name == "langfuse":
            return SimpleNamespace(get_client=lambda: (_ for _ in ()).throw(RuntimeError("boom")))
        if name == "langfuse.langchain":
            return SimpleNamespace(CallbackHandler=lambda: object())
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(observability.importlib, "import_module", fake_import_module)

    runtime = observability.build_langfuse_runtime(AutomationConfig())

    assert runtime.is_enabled() is False
    assert runtime.build_graph_config(run_name="feed-engage") is None
    assert "langfuse initialization failed" in caplog.text


def test_langfuse_runtime_swallows_callback_creation_failure(monkeypatch, caplog) -> None:
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-test")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-test")
    caplog.set_level(logging.ERROR)

    observability = import_module("x_atuo.automation.observability")

    fake_client = SimpleNamespace(shutdown=lambda: None)

    def fake_import_module(name: str):
        if name == "langfuse":
            return SimpleNamespace(get_client=lambda: fake_client)
        if name == "langfuse.langchain":
            return SimpleNamespace(
                CallbackHandler=lambda: (_ for _ in ()).throw(RuntimeError("callback boom"))
            )
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(observability.importlib, "import_module", fake_import_module)

    runtime = observability.build_langfuse_runtime(AutomationConfig())

    assert runtime.is_enabled() is True
    assert runtime.build_graph_config(run_name="feed-engage") == {"run_name": "feed-engage"}
    assert "langfuse callback creation failed" in caplog.text


def test_langfuse_runtime_swallows_shutdown_failure(monkeypatch, caplog) -> None:
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-test")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-test")
    caplog.set_level(logging.ERROR)

    observability = import_module("x_atuo.automation.observability")

    fake_client = SimpleNamespace(shutdown=lambda: (_ for _ in ()).throw(RuntimeError("shutdown boom")))

    def fake_import_module(name: str):
        if name == "langfuse":
            return SimpleNamespace(get_client=lambda: fake_client)
        if name == "langfuse.langchain":
            return SimpleNamespace(CallbackHandler=lambda: object())
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(observability.importlib, "import_module", fake_import_module)

    runtime = observability.build_langfuse_runtime(AutomationConfig())

    runtime.shutdown()

    assert "langfuse shutdown failed" in caplog.text


def test_langfuse_runtime_finishes_workflow_observation_via_context_manager_exit() -> None:
    observability = import_module("x_atuo.automation.observability")
    captured: dict[str, object] = {}

    class FakeObservation:
        trace_id = "trace-123"
        id = "observation-123"

        def __init__(self) -> None:
            self.ended = False
            self.end_calls = 0
            self.update_calls: list[dict[str, object]] = []

        def update(self, **kwargs: object) -> None:
            self.update_calls.append(kwargs)

        def end(self) -> None:
            self.end_calls += 1
            self.ended = True

    class FakeContextManager:
        def __init__(self, observation: FakeObservation, *, end_on_exit: bool) -> None:
            self.observation = observation
            self.end_on_exit = end_on_exit
            self.exit_calls: list[tuple[object, object, object]] = []

        def __enter__(self) -> FakeObservation:
            return self.observation

        def __exit__(self, exc_type, exc, tb) -> bool:
            self.exit_calls.append((exc_type, exc, tb))
            if self.end_on_exit:
                self.observation.ended = True
            return False

    fake_observation = FakeObservation()

    def fake_start_as_current_observation(**kwargs: object) -> FakeContextManager:
        context_manager = FakeContextManager(
            fake_observation,
            end_on_exit=bool(kwargs.get("end_on_exit", True)),
        )
        captured["kwargs"] = kwargs
        captured["context_manager"] = context_manager
        return context_manager

    runtime = observability.LangfuseRuntime(
        client=SimpleNamespace(start_as_current_observation=fake_start_as_current_observation),
        callback_handler_factory=lambda: object(),
    )

    workflow_observation = runtime.start_workflow_observation(
        run_name="x-atuo.feed-engage",
        metadata={"workflow": "feed-engage"},
    )

    runtime.finish_workflow_observation(
        workflow_observation,
        output={"status": "completed", "run_id": "run-123"},
    )

    assert workflow_observation is not None
    assert workflow_observation.trace_context == {
        "trace_id": "trace-123",
        "parent_span_id": "observation-123",
    }
    assert captured["kwargs"].get("end_on_exit", True) is True
    assert fake_observation.update_calls == [{"output": {"status": "completed", "run_id": "run-123"}}]
    assert captured["context_manager"].exit_calls == [(None, None, None)]
    assert fake_observation.ended is True
    assert fake_observation.end_calls == 0


def test_automation_graph_invoke_passes_graph_config() -> None:
    graph = AutomationGraph(AutomationConfig())
    request = AutomationRequest.for_feed_engage(reply_text="hello")
    captured: dict[str, object] = {}

    async def fake_ainvoke(state, config=None):
        captured["config"] = config
        return {"snapshot": state["snapshot"]}

    graph.graph = SimpleNamespace(ainvoke=fake_ainvoke)

    asyncio.run(graph.invoke(request, graph_config={"callbacks": ["cb"], "run_name": "demo"}))

    assert captured["config"] == {"callbacks": ["cb"], "run_name": "demo"}


def test_api_run_request_continues_without_langfuse(monkeypatch) -> None:
    request = AutomationRequest.for_feed_engage(reply_text="hello")
    storage = object()
    captured: dict[str, object] = {}

    snapshot = SimpleNamespace(
        status=SimpleNamespace(value="completed"),
        run_id="run-123",
        result=SimpleNamespace(model_dump=lambda mode="json": {"tweet_id": "123"}),
        candidate_refresh_count=0,
        selected_candidate=None,
        rendered_text="hello",
        selection_source=None,
        selection_reason=None,
        drafting_source="rule",
        errors=[],
        events=[],
    )

    class FakeGraph:
        async def invoke(self, graph_request, graph_config=None):
            captured["request"] = graph_request
            captured["graph_config"] = graph_config
            return snapshot

    fake_module = SimpleNamespace(
        _build_runtime_graph=lambda config, graph_storage, proxy=None: FakeGraph(),
        _persist_snapshot=lambda graph_storage, graph_snapshot: captured.update(
            persisted_storage=graph_storage,
            persisted_snapshot=graph_snapshot,
        ),
    )

    monkeypatch.setattr(automation_api, "_load_graph_module", lambda: fake_module)
    result = asyncio.run(
        automation_api._run_request(
            request,
            storage=storage,
            endpoint="scheduler:feed-engage",
        )
    )

    assert result["status"] == "completed"
    assert result["run_id"] == "run-123"
    assert result["result"] == {"tweet_id": "123"}
    assert captured["request"] is request
    assert captured["graph_config"] is None
    assert captured["persisted_storage"] is storage
    assert captured["persisted_snapshot"] is snapshot


def test_api_run_request_uses_provided_observability_runtime(monkeypatch) -> None:
    request = AutomationRequest.for_feed_engage(
        reply_text="hello",
        run_id="run-123",
        job_name="job-123",
        dry_run=True,
        approval_mode="ai_auto",
    )
    storage = object()
    calls: dict[str, object] = {}

    snapshot = SimpleNamespace(
        status=SimpleNamespace(value="completed"),
        run_id="run-123",
        result=None,
        candidate_refresh_count=0,
        selected_candidate=None,
        rendered_text="hello",
        selection_source=None,
        selection_reason=None,
        drafting_source="rule",
        errors=[],
        events=[],
    )

    class FakeGraph:
        async def invoke(self, graph_request, graph_config=None):
            calls["request"] = graph_request
            calls["graph_config"] = graph_config
            return snapshot

    fake_module = SimpleNamespace(
        _build_runtime_graph=lambda config, graph_storage, proxy=None: FakeGraph(),
        _persist_snapshot=lambda graph_storage, graph_snapshot: calls.update(
            persisted_storage=graph_storage,
            persisted_snapshot=graph_snapshot,
        ),
    )

    class FakeRuntime:
        def start_workflow_observation(self, **kwargs):
            calls["start"] = kwargs
            return "observation"

        def build_graph_config(self, *, run_name, observation=None):
            calls["build"] = {"run_name": run_name, "observation": observation}
            return {"callbacks": ["cb"], "run_name": run_name}

        def finish_workflow_observation(self, observation, *, output=None, error=None):
            calls["finish"] = {
                "observation": observation,
                "output": output,
                "error": error,
            }

    monkeypatch.setattr(automation_api, "_load_graph_module", lambda: fake_module)

    result = asyncio.run(
        automation_api._run_request(
            request,
            storage=storage,
            endpoint="scheduler:feed-engage",
            observability_runtime=FakeRuntime(),
        )
    )

    assert result["status"] == "completed"
    assert calls["request"] is request
    assert calls["start"] == {
        "run_name": "x-atuo.feed-engage",
        "metadata": {
            "run_id": "run-123",
            "job_id": "job-123",
            "workflow": "feed-engage",
            "endpoint": "scheduler:feed-engage",
            "dry_run": True,
            "approval_mode": "ai_auto",
            "environment": "development",
        },
    }
    assert calls["build"] == {
        "run_name": "x-atuo.feed-engage",
        "observation": "observation",
    }
    assert calls["graph_config"] == {"callbacks": ["cb"], "run_name": "x-atuo.feed-engage"}
    assert calls["finish"] == {
        "observation": "observation",
        "output": {"status": "completed", "run_id": "run-123"},
        "error": None,
    }
    assert calls["persisted_storage"] is storage
    assert calls["persisted_snapshot"] is snapshot


def test_api_run_request_marks_failed_snapshot_for_observability(monkeypatch) -> None:
    request = AutomationRequest.for_feed_engage(
        reply_text="hello",
        run_id="run-failed",
        job_name="job-failed",
    )
    storage = object()
    calls: dict[str, object] = {}

    snapshot = SimpleNamespace(
        status=SimpleNamespace(value="failed"),
        run_id="run-failed",
        result=None,
        candidate_refresh_count=0,
        selected_candidate=None,
        rendered_text="hello",
        selection_source=None,
        selection_reason=None,
        drafting_source="rule",
        errors=["policy denied", "draft failed"],
        events=[],
    )

    class FakeGraph:
        async def invoke(self, graph_request, graph_config=None):
            calls["graph_config"] = graph_config
            return snapshot

    fake_module = SimpleNamespace(
        _build_runtime_graph=lambda config, graph_storage, proxy=None: FakeGraph(),
        _persist_snapshot=lambda graph_storage, graph_snapshot: calls.update(
            persisted_storage=graph_storage,
            persisted_snapshot=graph_snapshot,
        ),
    )

    class FakeRuntime:
        def start_workflow_observation(self, **kwargs):
            return "observation"

        def build_graph_config(self, *, run_name, observation=None):
            return {"run_name": run_name}

        def finish_workflow_observation(self, observation, *, output=None, error=None):
            calls["finish"] = {
                "observation": observation,
                "output": output,
                "error": error,
            }

    monkeypatch.setattr(automation_api, "_load_graph_module", lambda: fake_module)

    result = asyncio.run(
        automation_api._run_request(
            request,
            storage=storage,
            endpoint="scheduler:feed-engage",
            observability_runtime=FakeRuntime(),
        )
    )

    assert result["status"] == "failed"
    assert calls["persisted_snapshot"] is snapshot
    assert str(calls["finish"]["error"]) == "policy denied; draft failed"
    assert calls["finish"]["output"] == {"status": "failed", "run_id": "run-failed"}


def test_graph_build_request_binding_for_feed_engage_uses_feed_options_and_proxy() -> None:
    request, proxy = automation_graph.build_request_binding(
        "run_feed_engage",
        run_id="run-feed",
        job_id="job-feed",
        payload={
            "dry_run": True,
            "mode": "human_review",
            "reply_template": "Ship it",
            "feed_type": "following",
            "feed_count": 7,
            "metadata": {"source": "webhook"},
            "idempotency_key": "idem-1",
            "proxy": "http://proxy.example:8080",
            "candidate": {
                "tweet_id": "tweet-1",
                "screen_name": "demo",
                "text": "hi",
            },
            "candidates": [
                {
                    "tweet_id": "tweet-2",
                    "screen_name": "demo2",
                    "text": "hello",
                }
            ],
        },
    ) or (None, None)

    assert request is not None
    assert request.workflow.value == "feed-engage"
    assert request.job_name == "job-feed"
    assert request.run_id == "run-feed"
    assert request.dry_run is True
    assert request.approval_mode == "ai_auto"
    assert request.reply_text == "Ship it"
    assert request.feed_options is not None
    assert request.feed_options.feed_type == "following"
    assert request.feed_options.feed_count == 7
    assert "candidate" not in request.model_dump(mode="json")
    assert "candidates" not in request.model_dump(mode="json")
    assert request.metadata == {"source": "webhook"}
    assert request.idempotency_key == "idem-1"
    assert proxy == "http://proxy.example:8080"


def test_graph_build_request_binding_uses_scheduler_production_defaults_when_values_omitted() -> None:
    request, proxy = automation_graph.build_request_binding(
        "run_feed_engage",
        run_id="run-defaults",
        job_id="job-defaults",
        payload={},
    ) or (None, None)

    config = AutomationConfig()

    assert request is not None
    assert request.workflow.value == "feed-engage"
    assert request.dry_run is False
    assert request.approval_mode == "ai_auto"
    assert request.feed_options is not None
    assert request.feed_options.feed_type == config.twitter.default_feed_type
    assert request.feed_options.feed_count == config.twitter.default_feed_count
    assert proxy == config.twitter.proxy_url


def test_lifespan_stores_and_shuts_down_observability_runtime(monkeypatch) -> None:
    events: list[str] = []
    runtime = SimpleNamespace(shutdown=lambda: events.append("runtime.shutdown"))

    class FakeStorage:
        def __init__(self, path):
            self.path = path

        def initialize(self) -> None:
            events.append("storage.initialize")

        def clear_stale_running_runs(self, *, reason: str) -> None:
            events.append(f"storage.clear:{reason}")

    class FakeScheduler:
        def __init__(self, config, dispatcher, on_queue_full):
            self.config = config
            self.dispatcher = dispatcher
            self.on_queue_full = on_queue_full

        def register_job(self, definition) -> None:
            events.append("scheduler.register_job")

        def maybe_start(self) -> None:
            events.append("scheduler.maybe_start")

        def shutdown(self, *, wait: bool) -> None:
            events.append(f"scheduler.shutdown:{wait}")

    monkeypatch.setattr(automation_api, "AutomationStorage", FakeStorage)
    monkeypatch.setattr(automation_api, "AutomationScheduler", FakeScheduler)
    monkeypatch.setattr(automation_api, "_build_scheduled_feed_engage", lambda settings: None)
    monkeypatch.setattr(automation_api, "build_langfuse_runtime", lambda settings: runtime)

    app = FastAPI(lifespan=automation_api.lifespan)

    async def exercise_lifespan() -> None:
        async with automation_api.lifespan(app):
            assert app.state.observability_runtime is runtime

    asyncio.run(exercise_lifespan())

    assert "storage.initialize" in events
    assert "scheduler.maybe_start" in events
    assert events[-2:] == ["scheduler.shutdown:False", "runtime.shutdown"]


def test_lifespan_still_shuts_down_runtime_when_scheduler_shutdown_raises(monkeypatch) -> None:
    events: list[str] = []
    runtime = SimpleNamespace(shutdown=lambda: events.append("runtime.shutdown"))

    class FakeStorage:
        def __init__(self, path):
            self.path = path

        def initialize(self) -> None:
            events.append("storage.initialize")

        def clear_stale_running_runs(self, *, reason: str) -> None:
            events.append(f"storage.clear:{reason}")

    class FakeScheduler:
        def __init__(self, config, dispatcher, on_queue_full):
            self.config = config
            self.dispatcher = dispatcher
            self.on_queue_full = on_queue_full

        def register_job(self, definition) -> None:
            raise AssertionError("unexpected scheduler registration")

        def maybe_start(self) -> None:
            events.append("scheduler.maybe_start")

        def shutdown(self, *, wait: bool) -> None:
            events.append(f"scheduler.shutdown:{wait}")
            raise RuntimeError("scheduler boom")

    monkeypatch.setattr(automation_api, "AutomationStorage", FakeStorage)
    monkeypatch.setattr(automation_api, "AutomationScheduler", FakeScheduler)
    monkeypatch.setattr(automation_api, "_build_scheduled_feed_engage", lambda settings: None)
    monkeypatch.setattr(automation_api, "build_langfuse_runtime", lambda settings: runtime)

    app = FastAPI(lifespan=automation_api.lifespan)

    async def exercise_lifespan() -> None:
        try:
            async with automation_api.lifespan(app):
                assert app.state.observability_runtime is runtime
        except RuntimeError as exc:
            assert str(exc) == "scheduler boom"

    asyncio.run(exercise_lifespan())

    assert events[-2:] == ["scheduler.shutdown:False", "runtime.shutdown"]
