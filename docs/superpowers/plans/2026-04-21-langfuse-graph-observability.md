# Langfuse Graph Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add optional Langfuse tracing for graph runs so each workflow execution can be inspected in Langfuse with LangGraph graph visibility, while preserving current behavior when Langfuse is unconfigured.

**Architecture:** Introduce a thin `automation.observability` adapter that owns Langfuse-specific imports, callback creation, and root trace lifecycle. Keep `graph.py` Langfuse-agnostic by only adding optional LangGraph runtime config pass-through, and wire the integration at `_run_request()` plus FastAPI lifespan so all current workflows share one integration point.

**Tech Stack:** FastAPI, LangGraph, SQLite-backed run storage, Langfuse Python SDK, pytest, uv

---

## File Map

- Create: `src/x_atuo/automation/observability.py`
  Purpose: optional Langfuse runtime wrapper, no-op fallback, graph callback config builder, workflow observation lifecycle, shutdown hook.
- Create: `tests/test_langfuse_observability.py`
  Purpose: focused tests for disabled mode, enabled mode, graph config pass-through, API integration behavior, and non-fatal Langfuse failures.
- Modify: `src/x_atuo/automation/graph.py`
  Purpose: allow `AutomationGraph.invoke()` to accept and pass through optional LangGraph runtime config.
- Modify: `src/x_atuo/automation/api.py`
  Purpose: initialize/shutdown observability runtime and wrap `_run_request()` with Langfuse observation creation/finalization.
- Modify: `pyproject.toml`
  Purpose: add `langfuse` dependency.
- Modify: `README.md`
  Purpose: document optional Langfuse configuration and no-op fallback behavior.
- Modify: `progress.md`
  Purpose: capture implementation progress, verification, and remaining risks for this task.

## Execution Notes

- Follow `@superpowers:test-driven-development` for code tasks.
- Use `@superpowers:verification-before-completion` before claiming the implementation is done.
- Keep Langfuse metadata minimal: `run_id`, `job_id`, `workflow`, `endpoint`, `dry_run`, `approval_mode`, `environment`.
- Do not send full request payloads, proxy URLs, tweet bodies, or credentials into Langfuse metadata.
- Treat missing Langfuse config, import errors, callback creation errors, and trace finalization errors as non-fatal.

### Task 1: Add the Optional Langfuse Runtime Wrapper

**Files:**
- Create: `src/x_atuo/automation/observability.py`
- Create: `tests/test_langfuse_observability.py`
- Modify: `pyproject.toml`

- [ ] **Step 1: Write failing tests for disabled and enabled runtime behavior**

```python
def test_langfuse_runtime_is_disabled_without_keys(monkeypatch):
    monkeypatch.delenv("LANGFUSE_PUBLIC_KEY", raising=False)
    monkeypatch.delenv("LANGFUSE_SECRET_KEY", raising=False)

    runtime = build_langfuse_runtime(AutomationConfig())

    assert runtime.is_enabled() is False
    assert runtime.build_graph_config(run_name="x-atuo.feed-engage") is None


def test_langfuse_runtime_builds_graph_config_when_enabled(monkeypatch):
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-lf-demo")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-lf-demo")

    fake_callback = object()

    class FakeHandler:
        def __call__(self):
            return fake_callback

    monkeypatch.setitem(sys.modules, "langfuse.langchain", type("M", (), {"CallbackHandler": FakeHandler})())

    runtime = build_langfuse_runtime(AutomationConfig())

    config = runtime.build_graph_config(run_name="x-atuo.feed-engage")
    assert config == {"callbacks": [fake_callback], "run_name": "x-atuo.feed-engage"}
```

- [ ] **Step 2: Run the focused tests to verify they fail first**

Run:

```bash
uv run pytest -q tests/test_langfuse_observability.py -k "runtime_is_disabled_without_keys or runtime_builds_graph_config_when_enabled"
```

Expected:

- `FAIL` because `x_atuo.automation.observability` does not exist yet.

- [ ] **Step 3: Implement the minimal runtime wrapper and dependency**

Implementation outline:

```python
# src/x_atuo/automation/observability.py
from dataclasses import dataclass
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class LangfuseRuntime:
    enabled: bool = False
    client: Any | None = None
    callback_factory: Any | None = None

    def is_enabled(self) -> bool:
        return self.enabled

    def build_graph_config(self, *, run_name: str) -> dict[str, Any] | None:
        if not self.enabled or self.callback_factory is None:
            return None
        try:
            return {"callbacks": [self.callback_factory()], "run_name": run_name}
        except Exception:
            logger.exception("langfuse callback creation failed")
            return None

    def shutdown(self) -> None:
        if self.client is None:
            return
        try:
            self.client.shutdown()
        except Exception:
            logger.exception("langfuse shutdown failed")


def build_langfuse_runtime(config: Any) -> LangfuseRuntime:
    if not (os.getenv("LANGFUSE_PUBLIC_KEY") and os.getenv("LANGFUSE_SECRET_KEY")):
        return LangfuseRuntime()
    try:
        from langfuse import get_client
        from langfuse.langchain import CallbackHandler
    except Exception:
        logger.exception("langfuse import failed")
        return LangfuseRuntime()
    try:
        return LangfuseRuntime(enabled=True, client=get_client(), callback_factory=CallbackHandler)
    except Exception:
        logger.exception("langfuse initialization failed")
        return LangfuseRuntime()
```

Dependency change:

```toml
dependencies = [
  "fastapi>=0.115",
  "uvicorn>=0.30",
  "apscheduler>=3.10",
  "langgraph>=0.2.60",
  "langchain-core>=0.3",
  "langfuse",
  "pydantic>=2.8",
  "pydantic-settings>=2.4",
  "pyyaml>=6.0",
]
```

- [ ] **Step 4: Re-run the focused runtime tests until they pass**

Run:

```bash
uv run pytest -q tests/test_langfuse_observability.py -k "runtime_is_disabled_without_keys or runtime_builds_graph_config_when_enabled"
```

Expected:

- `PASS`

- [ ] **Step 5: Commit the runtime wrapper using Lore protocol**

```bash
git add pyproject.toml src/x_atuo/automation/observability.py tests/test_langfuse_observability.py
git commit -F - <<'EOF'
Add an optional Langfuse runtime wrapper for graph tracing

Introduce a small observability boundary that can create Langfuse
graph callbacks when credentials are present and cleanly degrade to
no-op behavior when they are not.

Constraint: Langfuse must remain optional at runtime
Rejected: Import Langfuse directly inside graph execution paths | would couple business flow to tracing SDK details
Confidence: high
Scope-risk: narrow
Directive: Keep payload metadata minimal; do not expand trace metadata with raw tweet or credential data without explicit review
Tested: Focused runtime wrapper tests
Not-tested: Real Langfuse network delivery against a live project
EOF
```

### Task 2: Pass LangGraph Runtime Config Through `AutomationGraph.invoke()`

**Files:**
- Modify: `src/x_atuo/automation/graph.py`
- Modify: `tests/test_langfuse_observability.py`

- [ ] **Step 1: Write the failing invoke pass-through test**

```python
@pytest.mark.asyncio
async def test_automation_graph_invoke_passes_graph_config(monkeypatch):
    graph = AutomationGraph(AutomationConfig())
    request = AutomationRequest.for_direct_post(post_text="hello")

    captured = {}

    async def fake_ainvoke(state, config=None):
        captured["config"] = config
        return {"snapshot": state["snapshot"]}

    graph.graph = type("CompiledGraph", (), {"ainvoke": fake_ainvoke})()

    await graph.invoke(request, graph_config={"callbacks": ["cb"], "run_name": "demo"})

    assert captured["config"] == {"callbacks": ["cb"], "run_name": "demo"}
```

- [ ] **Step 2: Run the invoke test and verify it fails**

Run:

```bash
uv run pytest -q tests/test_langfuse_observability.py -k "invoke_passes_graph_config"
```

Expected:

- `FAIL` because `AutomationGraph.invoke()` does not yet accept `graph_config`.

- [ ] **Step 3: Implement the optional `graph_config` pass-through**

```python
async def invoke(
    self,
    request: AutomationRequest,
    graph_config: dict[str, Any] | None = None,
) -> WorkflowStateModel:
    state = make_initial_state(request)
    try:
        if graph_config is not None:
            result = await self.graph.ainvoke(state, config=graph_config)
        else:
            result = await self.graph.ainvoke(state)
        return result["snapshot"]
    except Exception as exc:
        snapshot = state["snapshot"]
        snapshot.mark_failed(str(exc))
        return snapshot
```

- [ ] **Step 4: Re-run the invoke test**

Run:

```bash
uv run pytest -q tests/test_langfuse_observability.py -k "invoke_passes_graph_config"
```

Expected:

- `PASS`

- [ ] **Step 5: Commit the additive graph API change**

```bash
git add src/x_atuo/automation/graph.py tests/test_langfuse_observability.py
git commit -F - <<'EOF'
Allow graph invocation to accept optional LangGraph runtime config

Add an additive invoke parameter so callers can attach runtime config
such as callbacks without coupling the graph module to any tracing SDK.

Constraint: Existing invocation call sites must continue working unchanged
Rejected: Import Langfuse directly into graph.py | wrong boundary for SDK-specific behavior
Confidence: high
Scope-risk: narrow
Directive: Keep graph.py SDK-agnostic; only standard LangGraph config should cross this boundary
Tested: Focused invoke pass-through test
Not-tested: End-to-end callback flow from API entrypoint
EOF
```

### Task 3: Wire Langfuse Into FastAPI Lifecycle and `_run_request()`

**Files:**
- Modify: `src/x_atuo/automation/api.py`
- Modify: `src/x_atuo/automation/observability.py`
- Modify: `tests/test_langfuse_observability.py`

- [ ] **Step 1: Write failing API integration tests**

```python
@pytest.mark.asyncio
async def test_run_request_continues_without_langfuse(monkeypatch):
    request = AutomationRequest.for_direct_post(post_text="hello")
    fake_runtime = build_langfuse_runtime(AutomationConfig())

    monkeypatch.setattr("x_atuo.automation.api._build_runtime_graph", lambda *args, **kwargs: FakeGraph())
    result = await _run_request(request, storage=FakeStorage(), observability_runtime=fake_runtime)

    assert result["status"] in {"completed", "failed", "blocked"}


@pytest.mark.asyncio
async def test_run_request_uses_langfuse_graph_config(monkeypatch):
    request = AutomationRequest.for_direct_post(post_text="hello")
    calls = {}

    class FakeRuntime:
        def build_graph_config(self, **kwargs):
            calls["graph_config"] = kwargs
            return {"callbacks": ["cb"], "run_name": "x-atuo.direct-post"}

        def start_workflow_observation(self, **kwargs):
            calls["start"] = kwargs
            return object()

        def finish_workflow_observation(self, observation, **kwargs):
            calls["finish"] = kwargs

    # assert graph.invoke receives the graph_config and finish is called
```

- [ ] **Step 2: Run the API integration tests to verify the gap**

Run:

```bash
uv run pytest -q tests/test_langfuse_observability.py -k "run_request_continues_without_langfuse or run_request_uses_langfuse_graph_config"
```

Expected:

- `FAIL` because `_run_request()` does not currently know about the observability runtime or graph config.

- [ ] **Step 3: Implement lifecycle storage and `_run_request()` wrapping**

Implementation outline:

```python
# lifespan()
runtime = build_langfuse_runtime(settings)
app.state.observability_runtime = runtime

try:
    yield
finally:
    runtime.shutdown()
    scheduler.shutdown(wait=False)


# _run_request()
async def _run_request(
    request: AutomationRequest,
    *,
    storage: Any,
    proxy: str | None = None,
    observability_runtime: Any | None = None,
) -> dict[str, Any]:
    config = AutomationConfig()
    runtime = observability_runtime or build_langfuse_runtime(config)
    graph = _build_runtime_graph(config, storage, proxy=proxy)

    observation = runtime.start_workflow_observation(
        run_name=f"x-atuo.{request.workflow.value}",
        run_id=request.run_id,
        job_id=request.job_name,
        workflow=request.workflow.value,
        dry_run=request.dry_run,
        approval_mode=request.approval_mode,
        environment=config.environment,
    )
    graph_config = runtime.build_graph_config(run_name=f"x-atuo.{request.workflow.value}")

    try:
        snapshot = await graph.invoke(request, graph_config=graph_config)
    finally:
        runtime.finish_workflow_observation(
            observation,
            status=snapshot.status.value if "snapshot" in locals() else "failed",
        )
```

Observability runtime methods should internally swallow Langfuse SDK exceptions and log them.

- [ ] **Step 4: Re-run the focused API integration tests**

Run:

```bash
uv run pytest -q tests/test_langfuse_observability.py -k "run_request_continues_without_langfuse or run_request_uses_langfuse_graph_config"
```

Expected:

- `PASS`

- [ ] **Step 5: Commit the API wiring**

```bash
git add src/x_atuo/automation/api.py src/x_atuo/automation/observability.py tests/test_langfuse_observability.py
git commit -F - <<'EOF'
Trace workflow runs through Langfuse without making it mandatory

Wrap the shared workflow execution path with an optional Langfuse
observation and pass Langfuse's LangGraph callback config into graph
execution when tracing is enabled.

Constraint: Missing Langfuse credentials must not affect service startup or request handling
Rejected: Create traces separately in each webhook handler | duplicates integration logic across workflows
Confidence: high
Scope-risk: moderate
Directive: Keep tracing failures non-fatal and centralized in observability.py
Tested: Focused API observability tests
Not-tested: Live trace delivery to a real Langfuse project
EOF
```

### Task 4: Document Configuration and Run Full Verification

**Files:**
- Modify: `README.md`
- Modify: `progress.md`
- Test: `tests/test_langfuse_observability.py`
- Test: `tests/test_smoke.py`

- [ ] **Step 1: Add the Langfuse runtime notes to README**

Add a concise section like:

```md
## Langfuse Graph Observability

Optional Langfuse support is available for workflow tracing.

Required:
- `LANGFUSE_PUBLIC_KEY`
- `LANGFUSE_SECRET_KEY`

Optional:
- `LANGFUSE_BASE_URL`
- `LANGFUSE_TRACING_ENVIRONMENT`

If the required keys are absent, the service runs without Langfuse and does not fail startup.
```

- [ ] **Step 2: Update `progress.md` for the implementation pass**

Record:

- files changed
- verification commands and results
- any observed Langfuse limitations
- explicit note that tracing is optional and no-op when unconfigured

- [ ] **Step 3: Run the focused and full test suites**

Run:

```bash
uv run pytest -q tests/test_langfuse_observability.py
uv run pytest -q
python3 -m compileall src tests
```

Expected:

- all tests `PASS`
- `compileall` completes with no errors

- [ ] **Step 4: Commit docs and verification updates**

```bash
git add README.md progress.md tests/test_langfuse_observability.py
git commit -F - <<'EOF'
Document optional Langfuse graph tracing and verification status

Capture the runtime configuration contract and verification evidence
for the new optional Langfuse tracing path.

Constraint: README must describe no-op fallback when credentials are missing
Confidence: high
Scope-risk: narrow
Directive: Keep README aligned with the actual env vars supported by observability.py
Tested: Focused Langfuse tests, full pytest suite, compileall
Not-tested: Manual trace inspection in a production Langfuse workspace
EOF
```

## Final Verification Checklist

- [ ] `tests/test_langfuse_observability.py` exists and covers disabled mode, enabled mode, invoke pass-through, and non-fatal failure behavior.
- [ ] `AutomationGraph.invoke()` accepts optional `graph_config` and preserves old call sites.
- [ ] `_run_request()` can run with or without Langfuse.
- [ ] FastAPI lifespan shuts Langfuse down cleanly when initialized.
- [ ] README documents the four Langfuse-related environment variables and the no-op fallback.
- [ ] `progress.md` summarizes changed files, verification, simplifications, and remaining risks.

## Execution Handoff

Plan consumers should implement in order, one task at a time. Do not batch Tasks 1-3 together; each task creates a verification checkpoint and narrows rollback scope.
