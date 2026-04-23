# Feed-Engage AI-Required Follow-Up Plan

**Goal:** Remove deterministic execution fallback from the active `feed-engage` runtime so the scheduler production path requires AI.

## Milestones

1. Lock AI-required behavior with failing tests
2. Remove deterministic runtime fallbacks and require an AI provider
3. Align config, docs, and tests with `ai_auto`-only execution semantics
4. Run focused and full verification, then update progress

## Scope

- Modify: `src/x_atuo/automation/graph.py`
  - Require an AI provider in `_build_runtime_graph()`
  - Remove runtime deterministic fallback in candidate selection and draft generation
  - Fail fast when AI selection or AI drafting fails
- Modify: `src/x_atuo/automation/config.py`
  - Update job default approval mode to `ai_auto`
- Modify: `src/x_atuo/automation/state.py`
  - Keep request defaults aligned with `ai_auto`
- Modify: `tests/test_smoke.py`
  - Add RED tests for AI-required runtime behavior
  - Update scheduler/runtime tests that assumed provider `none`
- Modify: `tests/test_langfuse_observability.py`
  - Remove `human_review` expectations from request binding / runtime metadata tests
- Modify: `README.md`
  - Remove deterministic mode language
- Modify: `progress.md`
  - Track milestone status, verification, and outcome

## Notes

- This is a runtime-behavior cleanup, not a new workflow.
- Keep `request.candidate` / `request.candidates` injection support unless the tests prove they block the AI-required runtime change.
- Follow TDD strictly: fail first, then minimal implementation, then focused verification.
