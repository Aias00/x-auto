# Graph Status Cleanup Plan

**Goal:** Remove one remaining dead graph edge configuration and make AI dependency exceptions report a consistent `failed` status across the active `feed-engage` workflow.

## Milestones

1. Lock AI-exception status behavior with failing tests
2. Remove the dead `prepare -> draft_text` edge mapping
3. Change AIProviderError handling in moderation/review from `blocked` to `failed`
4. Run focused and full verification, then update progress

## Scope

- Modify: `src/x_atuo/automation/graph.py`
  - Remove the unused `draft_text` branch from the `prepare` conditional edge mapping
  - Change `moderate_candidates()` and `selected_candidate_review()` AI dependency exceptions to call `mark_failed(...)`
- Modify: `tests/test_smoke.py`
  - Add focused tests for AI moderation/review exception status semantics
  - Update any existing assertions that depended on `blocked`
- Modify: `progress.md`
  - Track milestone status, failures, verification, and final summary

## Notes

- This is a cleanup-only pass; no intended product behavior changes beyond error-state consistency.
- The workflow should still `blocked` on policy/moderation outcomes, but AI dependency failures should become `failed`.
