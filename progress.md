# Progress

## Current Status

- Mainline is now the scheduled runtime flow only:
  - `fetch_feed -> prefilter_candidates -> select_candidate -> candidate_policy_guard -> draft_text -> policy_guard -> execute`
- Candidate evaluation is sequential and stops at the first acceptable candidate.
- Candidate cache persists business-relevant fields only; runtime-only `_x_atuo_*` metadata is stripped before SQLite writes.
- Moderation cache keys and moderation prompt payloads now share the same normalized input shape.
- Dead runtime surfaces for shortlist, selected-candidate review, AI selection, and batch moderation have been removed.

## Verification

- `uv run pytest -q` -> `78 passed in 0.88s`
- `python3 -m compileall src tests` -> success

## Remaining Risks

- `progress.md` is now a current-state document, not a step-by-step work log.
- If deeper historical context is needed, use the archive below.

## Archive

- Historical task-by-task progress has moved to `docs/superpowers/history/2026-04-24-progress-archive.md`
