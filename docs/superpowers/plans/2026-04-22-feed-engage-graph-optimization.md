# Feed-Engage Graph Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Optimize the remaining `feed-engage` workflow so it is cheaper to run, easier to maintain, and better aligned with the scheduler-only architecture.

**Architecture:** Reorder the workflow so deterministic cheap filtering runs before AI moderation, then reduce repeated state-reset logic and remove scheduler-only dead compatibility paths. Finish by making the scheduler path more natively request-driven instead of routing through payload reconstruction.

**Tech Stack:** FastAPI, LangGraph, APScheduler, SQLite, pytest, uv

---

## Milestones

1. Cheap prefilter before AI moderation
2. Candidate reset helper consolidation
3. `prepare()` cleanup for scheduler-only mode
4. Scheduler-native request driving

## File Map

- Modify: `src/x_atuo/automation/graph.py`
  Purpose: reorder nodes, add helper(s), and simplify scheduler-only paths.
- Modify: `src/x_atuo/automation/api.py`
  Purpose: reduce payload-to-request round-trips when the scheduler is already holding an `AutomationRequest`.
- Modify: `tests/test_smoke.py`
  Purpose: add direct graph coverage for the active `feed-engage` branches and protect routing/order changes.
- Modify: `progress.md`
  Purpose: milestone progress, failures, summaries, and final report.

## Milestone Details

### Milestone 1: Cheap Prefilter Before AI Moderation

- [ ] Add failing tests proving deterministic candidate filtering happens before AI moderation.
- [ ] Split current prefilter behavior into:
  - `cheap_prefilter_candidates()` for unverified/already-engaged filtering
  - `shortlist_prepare_candidates()` for shortlist truncation
- [ ] Route flow as:
  - `fetch_feed -> cheap_prefilter_candidates -> moderate_candidates -> shortlist_prepare_candidates -> select_candidate`
- [ ] Run minimal focused verification and keep green state.

Verification:
- Focused graph/smoke tests for route order and candidate visibility pass.

### Milestone 2: Candidate Reset Helper Consolidation

- [ ] Add failing tests around candidate rejection/retry state cleanup.
- [ ] Extract shared helper(s) for:
  - candidate rejection reset
  - feed refresh reset
- [ ] Replace duplicated field-clearing logic in:
  - `_schedule_candidate_refresh()`
  - `selected_candidate_review()`
  - `_retry_blocked_candidate()`
- [ ] Run focused verification and keep green state.

Verification:
- Retry/refresh tests confirm stale selection/draft state is not carried forward.

### Milestone 3: `prepare()` Cleanup for Scheduler-Only Mode

- [ ] Add failing tests if needed for the scheduler-only assumptions.
- [ ] Remove dead compatibility handling from `prepare()`:
  - duplicate `RUNNING` assignment
  - `frozen_selection_source`
  - `frozen_selection_reason`
  - `frozen_drafted_by`
- [ ] Keep `request.candidates` support for test/injection paths.
- [ ] Run focused verification and keep green state.

Verification:
- Existing direct-graph tests still pass and preloaded candidates still work.

### Milestone 4: Scheduler-Native Request Driving

- [ ] Add failing tests for the scheduler path using direct `AutomationRequest` execution.
- [ ] Reduce or bypass payload-to-request reconstruction in the scheduler production path.
- [ ] Keep Langfuse metadata and run persistence intact.
- [ ] Run full verification and update docs/progress final report.

Verification:
- `uv run pytest -q`
- `python3 -m compileall src tests`

## Notes

- Follow TDD strictly: failing test first, then minimal implementation, then focused verification.
- Do not widen scope into new product behavior; this is an internal graph/runtime optimization pass only.
- Keep FastAPI as read-only host unless a future task explicitly removes it.
