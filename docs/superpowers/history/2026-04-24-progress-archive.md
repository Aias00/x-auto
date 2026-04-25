# Progress Archive

This file preserves the historical step-by-step work log that previously lived in `progress.md`.

## Task Update 2026-04-24 — Remove Dead Batch Moderation Surface

## Task
Delete the remaining batch `moderate_candidates` graph surface so the codebase only exposes the scheduled-runtime candidate-evaluation path.

## Milestones
1. Remove dead graph batch-moderation node and routing
2. Remove dead batch-moderation runtime helper
3. Delete tests that only covered the removed surface
4. Run verification

## Current Milestone
Completed

## Done
- Removed the batch `moderate_candidates` node from `AutomationGraph`.
- Removed the dead batch moderation runtime helper from `_build_runtime_graph(...)`.
- Removed the no-longer-used `WorkflowAdapters.moderate_candidates` field.
- Deleted standalone tests that only exercised the removed batch moderation path.
- Kept the scheduled-runtime path centered on `prefilter -> select_candidate -> candidate_policy_guard -> draft_text -> policy_guard -> execute`.
- Narrowed moderation prompt payloads to stable core candidate fields instead of carrying broad raw metadata.
- Stripped `_x_atuo_*` runtime metadata before writing candidate cache entries to SQLite, while still re-attaching runtime-only tags after cache reads.

## Verification
- `uv run pytest -q tests/test_smoke.py -k 'route_after_fetch_feed_prefilters_before_candidate_evaluation or route_after_prefilter_goes_directly_to_select_candidate_without_batch_moderation or runtime_ or candidate_policy_guard or policy_guard or candidate_cache or candidate_evaluation_errors'` -> `19 passed`
- `uv run pytest -q` -> `78 passed in 0.86s`
- `python3 -m compileall src tests` -> success

## Simplifications Made
- There is now one candidate-evaluation flow in the codebase instead of a live runtime path plus a dead batch-moderation branch.
- Candidate cache persistence now stores business payloads rather than runtime-only bookkeeping fields.

## Failures
- None beyond expected dead-surface fallout during cleanup.

## Remaining Risks

## Task Update 2026-04-24 — Remove Dead Selection And Review Surfaces

## Task
Delete dead code that no longer participates in the active runtime path: `selected_candidate_review`, AI selection provider surfaces, and unused full-candidate byte helpers.

## Milestones
1. Remove dead graph review surface
2. Remove dead provider selection surface
3. Clean up stale tests and wording
4. Run full verification

## Current Milestone
Completed

## Done
- Removed `selected_candidate_review` from `AutomationGraph` and `WorkflowAdapters`.
- Removed provider-side AI selection surfaces from `x_atuo.core.ai_client`, including `AISelectionResult` and `select_candidate(...)`.
- Removed the unused `_candidate_payload_bytes(...)` helper from `graph.py`.
- Deleted standalone tests that only exercised the removed `selected_candidate_review` path.
- Tightened config/provider wording from “selection and drafting” to the current “moderation and drafting” surface.

## Verification
- Full verification run after cleanup:
  - `uv run pytest -q` -> `83 passed in 1.05s`
  - `python3 -m compileall src tests` -> success

## Simplifications Made
- Reduced the gap between the active runtime path and the codebase surface area, so the code now exposes fewer non-executable branches.

## Failures
- No functional regressions were introduced during dead-surface removal.

## Remaining Risks

## Task Update 2026-04-24 — Clean Up Shortlist Terminology And Draft Byte Metrics

## Task
Align runtime/docs terminology with the current candidate-pool traversal behavior and make `ai_draft_input_bytes` reflect the real compact draft prompt payload.

## Milestones
1. Remove stale shortlist terminology from active docs/runtime labels
2. Measure draft input bytes from the actual prompt payload
3. Add focused regression coverage
4. Run focused/full verification

## Current Milestone
Completed

## Done
- Renamed active runtime labels from shortlist-specific wording to candidate-pool / ordered-candidate wording.
- Updated README to describe the current newest-first candidate-pool traversal behavior instead of a shortlist stage.
- Added a shared draft-prompt payload helper and changed `ai_draft_input_bytes` to measure the actual compact draft payload instead of full `candidate.model_dump()`.
- Added focused regression coverage proving:
  - draft bytes are smaller than the full serialized candidate when internal metadata exists
  - runtime selection source now reflects ordered candidate traversal

## Verification
- `uv run pytest -q tests/test_smoke.py -k 'compact_draft_reply_payload_with_media_types or runtime_draft_metrics_use_compact_prompt_payload or runtime_select_candidate_stops_after_first_allowed_candidate or runtime_reuses_cached_candidate_moderation_without_ai_call'` -> `4 passed`
- `uv run pytest -q tests/test_smoke.py -k 'draft or runtime_ or select_candidate or moderate_candidates'` -> `14 passed`
- `uv run pytest -q` -> `85 passed in 1.00s`
- `python3 -m compileall src tests` -> success

## Simplifications Made
- Reused provider-side payload helpers so metrics and prompt construction share the same input shape.

## Failures
- The remaining issues were terminology/observability mismatches rather than runtime logic failures.
- Both are resolved in focused verification.

## Remaining Risks

## Task Update 2026-04-24 — Align Moderation Cache Key With Real Prompt Input

## Task
Fix moderation cache reuse so it keys off the same normalized candidate payload sent to the moderation provider instead of only `tweet_id + text`.

## Milestones
1. Define a shared normalized moderation payload
2. Rebuild moderation cache keys from that payload
3. Update cache-hit and cache-miss regression tests
4. Run focused/full verification

## Current Milestone
Completed

## Done
- Added shared moderation helpers in `x_atuo.core.ai_client` for:
  - normalized moderation candidate payloads
  - prompt payload construction
  - moderation cache-key generation
- Changed `OpenAICompatibleProvider.moderate_candidates(...)` to send the normalized moderation payload instead of raw `model_dump()` output with internal runtime metadata.
- Replaced the old moderation cache `text_hash` check with a provider/model-aware `cache_key` built from the normalized moderation payload.
- Updated moderation input-byte metrics to use the actual moderation payload shape.
- Added regression coverage proving:
  - internal `_x_atuo_*` metadata is excluded from moderation prompt input
  - non-text payload changes invalidate the moderation cache key
  - cached moderation still reuses correctly when payloads match
  - cached moderation misses when a non-text moderation-relevant field changes

## Verification
- `uv run pytest -q tests/test_smoke.py -k 'candidate_moderation_prompt or moderation_cache_key_changes_when_non_text_payload_changes or runtime_cached_moderation_misses_when_non_text_payload_changes or runtime_reuses_cached_candidate_moderation_without_ai_call'` -> `4 passed`
- `uv run pytest -q tests/test_smoke.py -k 'moderate_candidates or select_candidate or runtime_ or candidate_moderation_prompt or moderation_cache_key'` -> `12 passed`
- `uv run pytest -q` -> `84 passed in 0.98s`
- `python3 -m compileall src tests` -> success

## Simplifications Made
- Reused one shared normalization path for both provider prompting and cache-key computation, so the cache cannot silently drift from the actual moderation input shape.

## Failures
- Initial cache-key mismatch was expected:
  - the old cache only tracked `tweet_id + text_hash`
  - runtime prompt input still included a broader candidate payload
- Both issues are resolved in focused verification.

## Remaining Risks

## Task Update 2026-04-24 — Fix Candidate Evaluation Error Attribution

## Task
Correct `select_candidate` failure attribution so runtime moderation failures are no longer reported as AI selection failures.

## Milestones
1. Add regression coverage for runtime moderation failure labeling
2. Rename select-candidate failure surface to candidate evaluation failure
3. Re-run focused/full verification

## Current Milestone
Completed

## Done
- Renamed the `AutomationGraph.select_candidate()` exception surface from `ai selection failed` to `candidate evaluation failed`.
- Added a runtime regression test proving single-candidate moderation failures inside the active `select_candidate` path are reported with the new label.
- Updated the standalone graph test that exercises adapter failure wrapping at the node boundary.

## Verification
- `uv run pytest -q tests/test_smoke.py -k 'candidate_evaluation_errors or runtime_select_candidate_reports_moderation_failure_as_candidate_evaluation'` -> `2 passed`
- `uv run pytest -q tests/test_smoke.py -k 'select_candidate or candidate_policy_guard or policy_guard or runtime_'` -> `13 passed`
- `uv run pytest -q` -> `82 passed in 1.07s`
- `python3 -m compileall src tests` -> success

## Simplifications Made
- Used a neutral node-level error label that remains correct whether the underlying adapter failure comes from moderation, hydration, or selection logic.

## Failures
- Initial mismatch was expected: the node still logged `ai selection failed` even though the active runtime path no longer performed multi-candidate AI selection.
- The mismatch is resolved in focused verification.

## Remaining Risks

## Task Update 2026-04-24 — Delay Claim Release Until Policy Guard Passes

## Task
Prevent early claim release from breaking downstream `policy_guard` retries by moving unused-claim release from `candidate_policy_guard` to the successful `policy_guard` branch.

## Milestones
1. Add regression coverage for `policy_guard` retry semantics
2. Move claim release to `policy_guard`
3. Verify candidate retry behavior still works
4. Run focused/full verification and report outcome

## Current Milestone
Completed

## Done
- Added a RED test proving `policy_guard` still retries to the next candidate on retryable duplicate reasons before any claim release occurs.
- Moved `_release_unused_claimed_candidates(...)` from `candidate_policy_guard` to the allow branch of `policy_guard`.
- Added a focused success-path test proving unused claimed candidates are released only after `policy_guard` passes.
- Kept `candidate_policy_guard` candidate-level retry behavior unchanged.

## Verification
- `uv run pytest -q tests/test_smoke.py -k 'policy_guard_releases_unused_claimed_candidates_after_early_stop or policy_guard_retries_duplicate_candidate_before_releasing_claims or feed_engage_candidate_policy_guard_retries_duplicate_candidate'` -> `3 passed`
- `uv run pytest -q tests/test_smoke.py -k 'policy_guard or candidate_policy_guard or route_after or candidate_cache or select_candidate or runtime_'` -> `19 passed`
- `uv run pytest -q` -> `81 passed in 0.92s`
- `python3 -m compileall src tests` -> success

## Simplifications Made
- Release timing now aligns with the last retryable candidate-selection gate instead of introducing extra state bookkeeping.
- The existing `_release_unused_claimed_candidates(...)` helper remains unchanged; only its call site moved.

## Failures
- Initial RED failures were expected:
  - claim release happened before `policy_guard`
  - retryable `policy_guard` failures could no longer fall through to the next candidate
- Both issues are resolved in focused verification.

## Remaining Risks

## Task Update 2026-04-24 — Remove Shortlist Node And Release Unused Claims

## Task
Delete the `shortlist_prepare_candidates` graph node and release cache-claimed candidates back to `pending` once sequential early-stop selection commits to a final candidate.

## Milestones
1. Remove shortlist node and related routing
2. Preserve ordered sequential selection directly after prefilter
3. Release unused claimed candidates after candidate policy passes
4. Update docs/progress and run verification

## Current Milestone
Completed

## Done
- Removed `shortlist_prepare_candidates` from the graph path; `prefilter_candidates` now routes directly to `select_candidate` unless a standalone batch moderation adapter is explicitly wired.
- Removed the unused `candidate_hydration_count` policy setting from runtime config.
- Kept the ordered sequential candidate evaluation path as the active runtime behavior after prefilter.
- Moved claim release to the point where the selected candidate has passed `candidate_policy_guard`, so unused claimed candidates are returned to `pending` without breaking in-run retry semantics before that point.
- Added/updated focused tests for:
  - direct `prefilter -> select_candidate` routing when batch moderation is absent
  - `prefilter -> moderate_candidates` routing when batch moderation is explicitly present
  - direct `select_candidate -> candidate_policy_guard` routing
  - releasing unused claimed candidates after early-stop selection

## Verification
- `uv run pytest -q tests/test_smoke.py -k 'route_after_prefilter_runs_ai_moderation_when_available or route_after_prefilter_goes_directly_to_select_candidate_without_batch_moderation or candidate_policy_guard_releases_unused_claimed_candidates_after_early_stop or route_after_selection_goes_directly_to_candidate_policy_guard or runtime_select_candidate_uses_single_candidate_moderation_before_policy_guard or runtime_select_candidate_stops_after_first_allowed_candidate or runtime_select_candidate_checks_next_candidate_after_rejection or runtime_reuses_cached_candidate_moderation_without_ai_call'` -> `8 passed`
- `uv run pytest -q tests/test_smoke.py -k 'candidate_cache or route_after or feed_engage or moderation or select_candidate or candidate_policy_guard or draft or runtime_'` -> `33 passed`
- `uv run pytest -q` -> `80 passed in 0.92s`
- `python3 -m compileall src tests` -> success

## Simplifications Made
- The active runtime path is now `prefilter -> select_candidate -> candidate_policy_guard`, without a separate shortlist node.
- Claim release happens once the run is committed to the selected candidate, rather than during quantity trimming.
- Existing standalone unit coverage for `selected_candidate_review` remains available even though the active graph path no longer uses that node.

## Failures
- Initial RED failures were expected:
  - prefilter still routed through the deleted shortlist node
  - claim release was still tied to shortlist trimming
- Both issues were resolved in focused verification.

## Remaining Risks
- Historical `progress.md` entries below still describe earlier shortlist-based stages; they are retained as work log, not as current behavior.

## Task Update 2026-04-24 — Sequential Early-Stop Candidate Selection

## Task
Change the runtime path from batch shortlist hydration/comparison to ordered sequential candidate evaluation so the run can stop after the first acceptable candidate.

## Milestones
1. Add regression coverage for sequential early-stop behavior
2. Switch runtime flow to shortlist -> direct sequential candidate selection
3. Merge selected-candidate review into sequential selection flow
4. Update docs/progress and run verification

## Current Milestone
Completed

## Done
- Added RED runtime tests proving:
  - the first acceptable candidate stops hydration/moderation early
  - a rejected first candidate causes the second candidate to be checked next
- Changed `_build_runtime_graph(...)` so the runtime path no longer enters the batch `moderate_candidates` node after shortlist preparation.
- Reworked the runtime `select_candidate(snapshot)` adapter into ordered sequential evaluation:
  - hydrate one candidate at a time
  - apply reply-policy checks immediately
  - run single-candidate AI moderation only when needed
  - stop at the first acceptable candidate
- Merged the runtime responsibilities of `selected_candidate_review` into the sequential `select_candidate(snapshot)` path, so selection now performs the final candidate validation inline.
- Preserved retry semantics by keeping the selected candidate plus remaining unchecked candidates in `snapshot.candidates`, so downstream policy retries still have a pool to continue with.

## Verification
- `uv run pytest -q tests/test_smoke.py -k 'runtime_select_candidate_stops_after_first_allowed_candidate or runtime_select_candidate_checks_next_candidate_after_rejection or runtime_select_candidate_uses_single_candidate_moderation_before_policy_guard or runtime_reuses_cached_candidate_moderation_without_ai_call'` -> `4 passed`
- `uv run pytest -q tests/test_smoke.py -k 'shortlist_prepare or candidate_cache or route_after_prefilter or feed_engage or moderation or select_candidate or draft or runtime_'` -> `31 passed`
- `uv run pytest -q` -> `80 passed in 1.00s`
- `python3 -m compileall src tests` -> success

## Simplifications Made
- Removed runtime dependence on multi-candidate AI selection; selection is now newest-first ordered validation.
- Kept the generic graph node surfaces intact, so existing unit tests around standalone `AutomationGraph` nodes still work.
- Reused the existing moderation cache and retry semantics instead of introducing a new candidate-state machine.

## Failures
- Initial RED failures were expected:
  - runtime still hydrated the whole shortlist before selecting
  - runtime still depended on the batch moderation path
- Both targeted failures were resolved in focused verification.

## Remaining Risks
- Candidate quality is now driven by shortlist order plus per-candidate validation, not by AI comparing multiple candidates against each other.

## Task Update 2026-04-24 — Release Trimmed Claimed Candidates

## Task
Release cache-claimed candidates back to `pending` immediately when they are dropped only because the shortlist size cap trimmed them.

## Milestones
1. Add regression coverage for claim release
2. Add storage-level claim-release API
3. Release trimmed claimed candidates during shortlist preparation
4. Run focused/full verification and report risks

## Current Milestone
Completed

## Done
- Added a RED storage test proving a specific claimed candidate can be released back to `pending` and reclaimed by a later run.
- Added a RED graph test proving `shortlist_prepare_candidates` releases only the candidates dropped because of quantity trimming.
- Added `AutomationStorage.release_claimed_candidate_cache(...)` to move still-claimed candidates back to `pending` for the current run only.
- Propagated `claim_run_id` into cached candidate metadata during `fetch_feed` so shortlist trimming can identify which dropped candidates are safe to release.
- Updated `shortlist_prepare_candidates` to release only quantity-trimmed, cache-claimed candidates and record a dedicated event when release happens.
- Kept moderation/policy rejections unchanged: they still flow through `reject_candidate_cache(...)` and are not returned to `pending`.

## Verification
- `uv run pytest -q tests/test_smoke.py -k 'shortlist_prepare_candidates_releases_trimmed_claimed_candidates or candidate_cache_release_claimed_entries_returns_to_pending'` -> `2 passed`
- `uv run pytest -q tests/test_smoke.py -k 'shortlist_prepare or candidate_cache or route_after_prefilter or feed_engage or moderation or select_candidate'` -> `26 passed`
- `uv run pytest -q` -> `77 passed in 1.20s`
- `python3 -m compileall src tests` -> success

## Simplifications Made
- Release is limited to shortlist-size trimming only; no other rejection path changed behavior.
- Reused existing cache status fields instead of adding a new table or state enum.
- Scoped release by `run_id` so one run cannot release another run's claims.

## Failures
- Initial RED failures were expected:
  - shortlist trimming did not call any release hook
  - storage had no targeted claim-release API
- Both failures were resolved in focused verification.

## Remaining Risks
- If a run repeatedly refreshes and reclaims the same pool, quantity-trimmed candidates may be seen again on a later fetch cycle; this is intentional and now cheaper than waiting for claim TTL expiry.

## Task Update 2026-04-24 — AI Node Latency Optimization

## Task
Reduce the runtime cost and observability blind spots around AI-heavy `feed-engage` graph nodes.

## Milestones
1. Add regression coverage for AI-node metrics and routing order
2. Emit per-call AI latency/input metrics
3. Shortlist before AI moderation
4. Reuse full-text moderation results
5. Run focused/full verification and report risks

## Current Milestone
Completed

## Done
- Added focused RED tests proving:
  - prefilter routes to shortlist before AI moderation
  - moderation only sees the configured shortlist size
  - AI runtime metrics are copied onto graph events
  - selected-candidate review reuses full-text moderation instead of calling AI twice
  - cached hydrated candidates with moderation metadata skip AI moderation
- Reordered the active graph path so `prefilter_candidates` routes through `shortlist_prepare_candidates` before `moderate_candidates`.
- Hydrated shortlisted candidates before runtime AI moderation, so moderation decisions are based on full tweet text.
- Stored moderation metadata on candidates using a text hash, and reused it only when the selected/cached candidate text still matches.
- Wrapped runtime AI moderation, selection, style classification, and draft calls with `asyncio.to_thread()` to avoid blocking the graph event loop.
- Added AI-specific observability fields:
  - `ai_moderation_duration_ms`, `ai_moderation_candidate_count`, `ai_moderation_cache_hits`, `ai_moderation_input_bytes`
  - `ai_selection_duration_ms`, `ai_selection_candidate_count`, `ai_selection_input_bytes`
  - `ai_style_classification_duration_ms`
  - `ai_draft_duration_ms`, `ai_draft_input_bytes`

## Verification
- `uv run pytest -q tests/test_smoke.py -k 'shortlist_prepare_candidates_limits_ai_moderation_input or route_after_prefilter_shortlists_before_ai_moderation_when_available or prefilter_candidates_remove_already_engaged_before_ai_moderation_runs or moderation_event_includes_ai_runtime_metrics or runtime_reuses_full_text_moderation_for_selected_review'` -> `5 passed`
- `uv run pytest -q tests/test_smoke.py -k 'feed_engage or moderation or select_candidate or draft or candidate_cache or runtime_observability or route_after'` -> `27 passed`
- `uv run pytest -q tests/test_smoke.py -k 'runtime_reuses_cached_candidate_moderation_without_ai_call or runtime_reuses_full_text_moderation_for_selected_review'` -> `2 passed`
- `uv run pytest -q` -> `75 passed in 0.90s`
- `python3 -m compileall src tests` -> success

## Simplifications Made
- Kept the public AI provider interface unchanged; optimization is contained inside runtime graph adapters.
- Avoided new dependencies by using existing stdlib timing, hashing, JSON sizing, and `asyncio.to_thread()`.
- Reused candidate metadata/cache instead of adding a new moderation cache table.

## Failures
- Initial RED failures were expected:
  - prefilter still routed directly to `moderate_candidates`
  - selected-candidate review called moderation a second time
  - moderation events did not include AI-specific runtime metrics
- All targeted failures are resolved in focused verification.

## Remaining Risks
- AI moderation metadata is intentionally reused only when the text hash matches; changed tweet text will trigger a fresh moderation call.
- Prompt-level call merging was not introduced; this pass reduces duplicate/scope cost without changing provider response contracts.

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
