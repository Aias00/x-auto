# Progress

## Task
Preserve reply restriction reasons when prefilter exhausts the candidate pool, and remove the remaining dead `fetch_feed` edge noise from the active `feed-engage` graph.

## Milestones
1. Lock reply-restriction exhaustion behavior with failing tests
2. Remove the dead `fetch_feed -> moderate_candidates` edge mapping
3. Propagate reply restriction reasons through prefilter exhaustion
4. Run focused and full verification, then update the final report

## Current Milestone
Completed

## Done
- Previous cleanup passes remain intact:
  - Cheap candidate prefilter now runs before AI moderation.
  - Candidate rejection / feed refresh state resets are centralized.
  - `prepare()` no longer carries dead frozen-selection compatibility logic.
  - The scheduler production path now drives direct `AutomationRequest` execution.
  - `feed-engage` runtime now requires AI and no longer degrades to deterministic selection/drafting.
- Previous request-surface cleanup remains intact:
  - `AutomationRequest` no longer accepts request-side candidate injection.
  - `build_request_binding()` now only binds scheduler-relevant request data.
- Completed Milestone 1:
  - Added a RED test proving reply restriction reasons must survive when prefilter exhausts the candidate pool.
- Completed Milestone 2:
  - Removed the dead `fetch_feed -> moderate_candidates` edge mapping.
- Completed Milestone 3:
  - Carried reply restriction reasons into the prefilter exhausted-pool reason builder.
  - Kept `blocked` semantics intact while making exhausted-pool diagnostics more specific.
- Additional follow-up coverage:
  - Added a mock-driven scheduler end-to-end test that verifies a reply-restricted blocked run persists the real reason into stored `response_payload.errors` and the serialized graph events.
- Additional persistence alignment:
  - Blocked graph runs now also copy their first blocked reason into top-level `runs.error`, so blocked run producers use a consistent storage contract.

## Verification
- Milestone 1 verification:
  - `uv run pytest -q tests/test_smoke.py -k 'prefilter_candidates_preserves_reply_restriction_reason_when_pool_is_exhausted or prefilter_candidates_filters_reply_restricted_before_selection or route_after_fetch_feed_prefilters_before_ai_moderation'` -> `3 passed`
- Previous pass final verification:
  - `uv run pytest -q` -> `65 passed in 0.74s`
  - `python3 -m compileall src tests` -> success
- Final verification:
  - `uv run pytest -q` -> `70 passed in 0.77s`
  - `python3 -m compileall src tests` -> success
- Additional follow-up verification:
  - `uv run pytest -q tests/test_smoke.py -k 'scheduler_dispatch_persists_reply_restriction_reason_in_run_payload'` -> `1 passed`
  - `uv run pytest -q` -> `71 passed in 0.78s`
  - `python3 -m compileall src tests` -> success
- Additional persistence verification:
  - `uv run pytest -q tests/test_smoke.py -k 'scheduler_dispatch_persists_reply_restriction_reason_in_run_payload'` -> `1 passed`
  - `python3 -m compileall src/x_atuo/automation/api.py tests/test_smoke.py` -> success
  - `uv run pytest -q` -> `71 passed in 0.77s`

## Simplifications Made
- This cleanup is scoped to graph diagnostics and edge-map hygiene, not to workflow capability changes.
- Reply restriction exhaustion now preserves real reasons instead of collapsing to a generic fallback string.
- `fetch_feed` conditional edges now match the actual route function return values.
- The scheduler persistence contract for blocked reply-restriction runs is now protected by one focused end-to-end test instead of being inferred from lower-level graph tests alone.
- Blocked run reasons are now available in both `response_payload.errors` and top-level `run.error`, matching the existing queue-drop blocked-run contract.

## Failures
- Initial RED failures were expected and resolved:
  - reply restriction exhaustion ended with the generic `"prefilter removed all candidates"` reason instead of the real restriction message
- Corrected before final verification by carrying reply restriction reasons into the exhausted-pool reason builder.

## Remaining Risks
- Langfuse coverage is still mostly mock-based around the SDK surface; if stronger confidence is needed, add a real credentialed smoke test in a non-production workspace.
- `blocked` still intentionally covers true moderation denials, policy denials, and exhausted candidate pools, so downstream dashboards should still distinguish those from `failed` dependency errors.

## Final Report
- Project status: completed successfully.
- Main achievements:
  - Reply restriction exhaustion now preserves the real restriction reason in blocked/refresh diagnostics.
  - Removed the remaining dead `fetch_feed` edge entry.
  - Improved graph readability without widening workflow behavior.
  - Added mock-based scheduler persistence coverage for blocked reply-restriction runs.
  - Aligned blocked graph-run persistence so the first blocked reason also lands in `runs.error`.
- Main challenges:
  - The new reply restriction filtering logic and the old exhausted-pool reason builder were out of sync.
- How they were resolved:
  - Added a focused RED test for prefilter exhaustion, then updated only the reason-builder path and reran full verification.
- Future recommendations:
  - If you want to keep tightening the graph, the next likely cleanup is removing other static edge-map noise where route functions already fully constrain reachable branches.
