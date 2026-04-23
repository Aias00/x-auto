# Remove Request Candidate Injection Plan

**Goal:** Remove `request.candidate` / `request.candidates` from the active `feed-engage` request surface so the runtime is fully scheduler-native.

## Milestones

1. Lock removal behavior with failing tests
2. Remove request-level candidate injection from state and graph
3. Align request binding and tests with the reduced request surface
4. Run focused and full verification, then update progress

## Scope

- Modify: `src/x_atuo/automation/state.py`
  - Remove `candidate` / `candidates` from `AutomationRequest`
  - Remove `for_feed_engage()` support for injected candidates
- Modify: `src/x_atuo/automation/graph.py`
  - Remove `prepare()` preload behavior based on `request.candidates`
  - Remove direct candidate selection path based on `request.candidate`
  - Remove `build_request_binding()` support for `candidate` / `candidates` payload keys
- Modify: `tests/test_smoke.py`
  - Replace injection-focused tests with scheduler-native request-surface assertions
- Modify: `tests/test_langfuse_observability.py`
  - Remove candidate injection assertions from request-binding coverage
- Modify: `progress.md`
  - Track milestone status, failures, verification, and final report

## Notes

- This is a cleanup-only change; it should not widen workflow behavior.
- Candidate pool handling still exists in runtime snapshot state and storage/cache layers; only request-surface injection is being removed.
- Follow TDD strictly: fail first, then minimum implementation, then focused verification.
