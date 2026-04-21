# Langfuse Graph Observability Design

## Context

`x-atuo` already persists run state, audit events, and workflow outcomes in SQLite, and exposes run lookup through the FastAPI API. What it does not have is a first-class external tracing view for a single workflow execution. For the current need, the target is narrower than "full observability": connect each graph run to Langfuse so a workflow execution can be inspected as a Langfuse trace with LangGraph graph visibility.

The integration must be safe to ship in a service context:

- If `LANGFUSE_PUBLIC_KEY` and `LANGFUSE_SECRET_KEY` are not configured, the service must start and run normally.
- The graph implementation should not become tightly coupled to Langfuse-specific code.
- Existing run persistence in SQLite remains the local source of truth.

## Goals

- Create one Langfuse trace per workflow execution.
- Preserve LangGraph graph visibility in Langfuse by passing the official Langfuse LangChain callback handler into graph execution.
- Attach stable run metadata so Langfuse traces can be correlated with local run records.
- Degrade to a no-op when Langfuse is unconfigured or temporarily unavailable.
- Keep the implementation localized and reversible.

## Non-Goals

- No manual span per graph node.
- No replacement of SQLite run/audit storage.
- No broad OpenTelemetry or Prometheus integration in this change.
- No bulk export of raw request payloads, proxy configuration, or tweet content into Langfuse metadata.

## Chosen Approach

Use a thin observability adapter around graph execution:

1. Add a small `automation.observability` module that owns all Langfuse-specific imports and lifecycle behavior.
2. Extend `AutomationGraph.invoke()` to accept an optional LangGraph runtime config and pass it through to `self.graph.ainvoke(...)`.
3. In `_run_request()`, create a root Langfuse observation/trace for the workflow run, build a LangGraph config containing `langfuse.langchain.CallbackHandler()`, and invoke the graph with that config.
4. End the observation after success or failure and annotate it with minimal status/output metadata.
5. On FastAPI shutdown, flush/shutdown Langfuse if it was initialized.

This keeps `graph.py` Langfuse-agnostic and puts the integration at the service orchestration boundary where `run_id`, `job_id`, workflow type, and execution outcome are already available.

## Alternatives Considered

### 1. Callback Only

Pass only `CallbackHandler()` to `graph.ainvoke(...)` and do not create a root workflow observation explicitly.

Pros:

- Smallest patch.

Cons:

- Weak correlation between local `run_id` / `job_id` and the resulting Langfuse trace.
- Less control over top-level naming and metadata shape.

Rejected because this project already has stable workflow-level identifiers, and not binding them at the root trace would waste that structure.

### 2. Root Trace + Callback + Manual Node Spans

Create a root workflow trace, pass the callback, and also instrument internal graph nodes manually.

Pros:

- Maximum detail.

Cons:

- Duplicates information the LangGraph callback already emits.
- Increases maintenance cost across every node.
- Raises the chance of trace noise and inconsistent nesting.

Rejected because the current requirement is graph-level visibility, not custom node telemetry.

## Runtime Design

### Configuration Contract

Enable Langfuse only when both of these are present:

- `LANGFUSE_PUBLIC_KEY`
- `LANGFUSE_SECRET_KEY`

Optional configuration:

- `LANGFUSE_BASE_URL`
- `LANGFUSE_TRACING_ENVIRONMENT`

Fallback behavior:

- If Langfuse environment variables are missing, the integration returns no-op behavior.
- If Langfuse imports fail or initialization raises, execution continues without external tracing.

Environment naming:

- Prefer `LANGFUSE_TRACING_ENVIRONMENT` when set.
- Otherwise fall back to `AutomationConfig.environment`.

### Trace Naming

Each workflow execution creates a top-level trace with a stable, service-specific name:

- `x-atuo.feed-engage`
- `x-atuo.repo-post`
- `x-atuo.direct-post`

### Trace Metadata

Attach only stable operational metadata:

- `run_id`
- `job_id`
- `workflow`
- `endpoint`
- `dry_run`
- `approval_mode`
- `environment`

Do not attach:

- Full request JSON
- Proxy URL
- Raw credentials
- Full tweet text / candidate lists by default

### Error Handling

Langfuse failures are non-blocking:

- If callback creation fails, run the graph without callback config.
- If trace start/update/end fails, log locally and continue.
- A Langfuse outage must not change HTTP responses, run status, or SQLite persistence behavior.

## Code Changes

### 1. `src/x_atuo/automation/observability.py`

Add a new module that contains:

- `LangfuseRuntime` or equivalent small wrapper object
- `build_langfuse_runtime(config: AutomationConfig) -> LangfuseRuntime`
- `is_enabled() -> bool`
- `start_workflow_observation(...)`
- `build_graph_config(...)`
- `finish_workflow_observation(...)`
- `shutdown()`

Responsibilities:

- Own all Langfuse imports
- Guard initialization and callback creation
- Normalize no-op behavior when disabled
- Keep API and graph layers free of Langfuse SDK details

### 2. `src/x_atuo/automation/graph.py`

Change:

- `AutomationGraph.invoke(self, request: AutomationRequest) -> WorkflowStateModel`

to:

- `AutomationGraph.invoke(self, request: AutomationRequest, graph_config: dict[str, Any] | None = None) -> WorkflowStateModel`

Behavior:

- Call `self.graph.ainvoke(state, config=graph_config)` when config is provided
- Otherwise preserve current behavior

No Langfuse imports are added here.

### 3. `src/x_atuo/automation/api.py`

Integrate at two lifecycle points.

#### FastAPI lifespan

- Build the Langfuse runtime once at startup and store it on `app.state`
- Shutdown the runtime on service exit

#### `_run_request()`

- Load the runtime
- Start a root workflow observation
- Build the LangGraph config from the runtime
- Call `graph.invoke(request, graph_config=...)`
- On success, record final workflow status and selected high-signal fields
- On failure, record error/status
- Always finish the observation in `finally`

This is the single shared entrypoint for `feed_engage`, `repo_post`, and `direct_post`, so one integration point covers all current workflows.

### 4. `pyproject.toml`

Add:

- `langfuse`

No other new dependency is required for this change.

### 5. `README.md`

Add a short section documenting:

- optional Langfuse support
- required environment variables
- default no-op behavior when unconfigured

## Data Flow

1. Request arrives through webhook or internal scheduler dispatch.
2. `_run_request()` creates the `AutomationRequest`.
3. `_run_request()` asks the observability runtime to open a workflow observation and build the LangGraph config.
4. `AutomationGraph.invoke()` runs `self.graph.ainvoke(...)` with the provided config.
5. Langfuse's callback handler captures LangGraph execution and displays the graph trace.
6. SQLite persistence continues unchanged through existing `_persist_snapshot()` and API-layer run storage.
7. `_run_request()` closes the Langfuse observation with final status metadata.

## Testing Plan

### Unit / Integration Coverage

Add targeted tests for:

1. `AutomationGraph.invoke()` passes `graph_config` through to `ainvoke()`.
2. Langfuse-disabled mode:
   - missing env vars returns no-op runtime
   - `_run_request()` still succeeds without Langfuse installed/configured
3. Langfuse-enabled mode:
   - callback builder is invoked
   - root workflow observation is started and finished
   - failures inside Langfuse helpers do not fail the workflow

### Regression Verification

Run:

- targeted tests for the new observability module and invoke pass-through
- full `pytest -q`

This change is considered complete only if all pre-existing workflow behavior remains green.

## Rollout Plan

1. Merge code with Langfuse unconfigured in default environments.
2. Enable Langfuse by setting:
   - `LANGFUSE_PUBLIC_KEY`
   - `LANGFUSE_SECRET_KEY`
   - optionally `LANGFUSE_BASE_URL`
   - optionally `LANGFUSE_TRACING_ENVIRONMENT`
3. Validate that one local run produces:
   - a successful API response
   - an unchanged SQLite run record
   - a Langfuse trace discoverable by `run_id`

## Risks

### Compatibility Risk

`graph.invoke()` gains a new optional parameter. This is low risk because it is additive and preserves the existing call shape.

### Dependency Risk

Adding `langfuse` introduces a new runtime dependency. This is acceptable because the integration is optional at runtime and guarded by configuration.

### Operational Risk

If Langfuse is slow or unavailable, callback/trace operations must not affect workflow execution. This is mitigated by defensive no-op wrappers and non-fatal error handling.

## Success Criteria

- Service starts normally with no Langfuse credentials configured.
- A configured environment emits one Langfuse trace per workflow run.
- Langfuse shows the LangGraph execution as a graph trace.
- Traces can be correlated with SQLite runs using `run_id`.
- Existing API and workflow tests remain green.
