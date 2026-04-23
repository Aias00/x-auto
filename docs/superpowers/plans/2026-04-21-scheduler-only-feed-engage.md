# Scheduler-Only Feed-Engage Refactor Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the service to a scheduler-first architecture where `feed-engage` is the only executable workflow and FastAPI remains only as a read-only host for health and run lookup.

**Architecture:** Keep FastAPI as the process host and resource lifecycle manager, but remove all webhook write endpoints and all non-`feed-engage` workflow entrypoints. Scheduler dispatch becomes the only execution source; the shared orchestration path, SQLite state, and optional Langfuse tracing stay in place.

**Tech Stack:** FastAPI, LangGraph, APScheduler, SQLite, pytest, uv

---

## Milestones

1. Planning and impact map
2. Scheduler-first entrypoint refactor
3. Workflow surface reduction to `feed-engage` only
4. Verification, docs, and final cleanup

## File Map

- Modify: `src/x_atuo/automation/api.py`
  Purpose: keep lifecycle, keep read-only endpoints, remove webhook write handlers, preserve scheduler as the only execution source.
- Modify: `src/x_atuo/automation/state.py`
  Purpose: collapse workflow kinds and request helpers to `feed-engage` only if feasible without introducing dead API.
- Modify: `src/x_atuo/automation/schemas.py`
  Purpose: remove webhook request/response schemas that are no longer needed; keep health/run lookup schemas.
- Modify: `src/x_atuo/automation/graph.py`
  Purpose: remove non-feed-engage request bindings and unused repo/direct workflow branches where they become dead.
- Modify: `src/x_atuo/core/ai_client.py`
  Purpose: remove repo-post-specific drafting surface if it becomes dead.
- Modify: `src/x_atuo/core/github_repo_client.py`
  Purpose: likely becomes dead once repo-post is removed; evaluate whether to delete or leave unused.
- Modify: `tests/test_smoke.py`
  Purpose: remove webhook and non-feed-engage tests; keep scheduler and read-only API coverage.
- Modify: `tests/test_langfuse_observability.py`
  Purpose: remove webhook-specific observability tests; replace with scheduler/read-only API coverage where needed.
- Modify: `README.md`
  Purpose: document scheduler-first runtime, remove webhook usage docs, keep read-only API notes.
- Modify: `progress.md`
  Purpose: milestone status, verification, failures, next steps, and final report.

## Milestone Details

### Milestone 1: Planning and Impact Map

- [ ] Confirm the new architecture target in docs/progress:
  - FastAPI remains as host
  - only `GET /healthz` and `GET /runs/{run_id}` remain
  - scheduler is the sole execution trigger
  - only `feed-engage` remains as a workflow
- [ ] Inventory all webhook, repo-post, and direct-post references in code/tests/docs.
- [ ] Update `progress.md` with the milestone list, current milestone, and next step.
- [ ] Run no code changes beyond planning artifacts in this milestone.

Verification:
- Planning artifacts exist and list the affected files clearly.

### Milestone 2: Scheduler-First Entrypoint Refactor

- [ ] Remove `/hooks/twitter/feed-engage`, `/hooks/twitter/repo-post`, `/hooks/twitter/direct-post` from `api.py`.
- [ ] Keep `lifespan()`, `GET /healthz`, and `GET /runs/{run_id}`.
- [ ] Ensure scheduler dispatch remains fully functional through `_dispatch_scheduled_request()` / `_execute_job()`.
- [ ] Remove webhook-only helpers from `api.py` once no longer referenced.
- [ ] Write/adjust tests first so the removed write endpoints fail and scheduler/read-only API still work.

Verification:
- Focused tests for read-only API and scheduler path pass.

### Milestone 3: Workflow Surface Reduction to `feed-engage` Only

- [ ] Remove `repo-post` and `direct-post` workflow kinds / request builders / request bindings / execution branches.
- [ ] Remove repo-post/direct-post schemas and tests.
- [ ] Remove or isolate dead provider methods / GitHub repo-post code paths if no longer reachable.
- [ ] Keep feed-engage path green and keep candidate cache / moderation / drafting behavior intact.

Verification:
- Focused tests for `feed-engage` and scheduler pass.
- No remaining references to removed workflows in runtime paths.

### Milestone 4: Verification, Docs, and Final Cleanup

- [ ] Update `README.md` to reflect scheduler-only execution and read-only API.
- [ ] Update `progress.md` with milestone summaries, challenges, solutions, and final report.
- [ ] Run:
  - `uv run pytest -q`
  - `python3 -m compileall src tests`
- [ ] If verification fails, fix before closing the milestone.

Verification:
- Full test suite passes
- `compileall` succeeds
- Docs match delivered architecture

## Notes

- Prefer deletion over deprecation unless keeping a compatibility shell materially reduces risk.
- Do not introduce a new process host if FastAPI can continue owning lifecycle with smaller change.
- Keep Langfuse optional and scheduler-owned; removing webhooks should not weaken observability for scheduled runs.
