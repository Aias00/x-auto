# Progress

## Current Status

- Mainline is now the scheduled runtime flow only:
  - `fetch_feed -> prefilter_candidates -> select_candidate -> candidate_policy_guard -> draft_text -> policy_guard -> execute`
- Candidate evaluation is sequential and stops at the first acceptable candidate.
- Candidate cache persists business-relevant fields only; runtime-only `_x_atuo_*` metadata is stripped before SQLite writes.
- Moderation cache keys and moderation prompt payloads now share the same normalized input shape.
- Dead runtime surfaces for shortlist, selected-candidate review, AI selection, and batch moderation have been removed.
- Added a read-only `GET /analytics/account` surface backed by cookie-authenticated X Web analytics queries from `x.com/i/account_analytics`, with service-side normalization into account summary + post totals.
- Extended analytics support with `GET /analytics/account/content`, including content-page style filters for `type`, `sort`, `dir`, `from`, `to`, and `limit`.
- Added a read-only `GET /notifications` surface backed by cookie-authenticated X Web `NotificationsTimeline`, including `All` / `Mentions`-style timeline selection and cursor support.
- Author-alpha sync now reconstructs the original target tweet/author from reply detail before scoring daily reply analytics, so nested reply chains score the real target author instead of the immediate parent account.
- Author-alpha sync now prefers a fast direct-parent attribution path: `reply_to_id + first @handle` from the analytics payload, and only falls back to `twitter tweet --json` detail lookups when that fast path cannot recover the target author.
- Author-alpha now exposes `POST /author-alpha/sync/stop` for cooperative sync cancellation and `POST /author-alpha/reset` for clearing local author-alpha state when no sync is active.
- Author-alpha now exposes `POST /author-alpha/execute` as a scheduler-independent single-run trigger, so the lane can be validated manually while automatic scheduling stays disabled.
- Verified the manual HTTP execute path with real provider config end-to-end, including successful dry-run and real execution against the live author-alpha lane.
- Manual author-alpha execution now creates `manual:author-alpha-engage` runs in the author-alpha SQLite and has been validated end-to-end; the remaining failure mode is still upstream `twitter user-posts` 429s during candidate fetch.
- Author-alpha score recomputation now uses the risk-adjusted model documented in the engineering spec: winsorized reply impressions + empirical-Bayes shrinkage + small-sample penalty, replacing raw `impressions_total_7d` ordering.
- Author-alpha score smoothing weight `k` and penalty constant `c` are now configurable through `X_ATUO_AUTHOR_ALPHA__SCORE_PRIOR_WEIGHT` and `X_ATUO_AUTHOR_ALPHA__SCORE_PENALTY_CONSTANT`.
- Added a fully separate `author-alpha-engage` lane with its own SQLite, scheduler registration, sync control APIs, execution graph, and run lookup surface.
- Author-alpha sync now supports resumable day-by-day bootstrap, explicit error persistence on failed runs, same-day replacement of reply facts/author rollups, and zero-score retention for authors that drop out of the current 7-day window.
- Author-alpha execution now enforces ranked-author sourcing, same-target reply bursts, lifetime target caps, daily/global send ceilings, lane-local day accounting, and persistence of execution runs in the author-alpha SQLite.
- Author-alpha candidate sourcing now uses the notifications `device_follow` feed instead of per-author `twitter user-posts` reads, so execution no longer depends on walking authors one by one to discover fresh posts.
- Author-alpha now reads up to `50` device-follow posts per run, re-ranks them by local `author_score`, and skips targets whose last successful processing is still inside a `1h` revisit cooldown unless they age out and remain under the lifetime reply cap.
- Author-alpha execution now limits each run to at most `5` target posts and at most `2` replies per target burst, tightening manual and scheduled verification runs.
- Author-alpha now retries bounded transient network failures only for candidate-source reads and AI drafting, applies retry backoff before subsequent attempts, and degrades exhausted candidate-level failures into soft skips so later candidates can still run.
- Reply writes are intentionally single-attempt again to avoid duplicate comments after ambiguous network disconnects.
- Real-provider HTTP execution can still fail transiently at the X reply step (`Remote end closed connection without response`); one retry succeeded for the same `1 * 1` request shape.
- Author-alpha engagement records now explicitly tag same-target burst membership via `burst_id`, `burst_index`, and `burst_size`.
- Vendored the upstream `twitter_cli` package into `src/x_atuo/vendor/twitter_cli`, added its runtime dependencies to this project, introduced `twitter_runtime.py` as the shared credentials/proxy loader, and switched the current `TwitterClient` wrapper off subprocess/CLI execution onto the internal native HTTP client for the currently used operations.
- Extended the local `TwitterClient` core wrapper so the native client also backs `search`, `bookmarks` / bookmark folders, `user likes`, `followers` / `following`, and `retweet` / `unretweet`, with normalized x-atuo-facing return shapes.
- Extended the local `TwitterClient` core wrapper again so `like` / `unlike`, `bookmark` / `unbookmark`, `delete_tweet`, and `fetch_article` are also backed by the native client, with article metadata now normalized onto `TweetRecord`.
- Exposed the first batch of the newly wrapped Twitter read capabilities through FastAPI: `search`, `bookmarks`, bookmark folders, user likes, followers/following, and article fetch.
- Introduced a shared `x_web_transport.py` and migrated both `x_web_analytics.py` and `x_web_notifications.py` onto the same credentials/proxy/header/request stack, so the bottom-layer X read path is no longer duplicated across those readers.
- Introduced shared `x_graphql.py` operation specs and `x_web_normalization.py` helpers, then migrated analytics, notifications, and tweet-detail URL building onto those shared query-id / feature / normalization primitives.
- Introduced shared `x_timeline.py` helpers for timeline cursor extraction, notification entry normalization, and device-follow post normalization, and migrated `x_web_notifications.py` onto those shared parser/timeline primitives.
- Introduced shared `twitter_native_adapters.py` and moved `Tweet` / `UserProfile` -> x-atuo model adaptation out of `TwitterClient` into that dedicated boundary layer.
- Introduced shared `x_parser.py` and moved the parser’s core parsing logic there, making x-atuo the source of truth for timeline and tweet parsing.
- Introduced shared `x_native_client.py` and moved the native transport/mutation implementation there, making x-atuo the source of truth for low-level X reads and writes.
- Completed the final de-vendoring step: migrated the remaining native constants/exceptions/models into `core`, removed the `src/x_atuo/vendor/twitter_cli` package entirely, and dropped obsolete CLI-only Python dependencies (`click`, `rich`, `browser-cookie3`) from this project.
- Added a scheduler-independent `POST /feed-engage/execute` route so the original feed-engage lane can now be triggered manually the same way author-alpha can.
- Verified the new manual feed-engage route with one real execution: run `34e585ef-2082-4c5f-92bb-57992b35e36e` replied to `@sama` on tweet `2049493609028923826` with reply tweet `2049496352460337467`.
- Cleaned the final migration/documentation residue: `TwitterClient` write methods are now guarded against duplicate definitions by regression tests, README reflects both manual execute routes and current author-alpha pacing, and this progress file no longer describes a vendored compatibility layer that has already been removed.
- Added a shared cross-workflow engagement ledger in the main automation DB so `feed-engage` and `author-alpha` now record touched target tweets into the same table; `author-alpha` still keeps `alpha_engagements` for burst/lifetime caps, but it now skips targets that were already touched by other workflows and mirrors its successful replies into the shared ledger.

## Verification

- `uv run pytest -q` -> `78 passed in 0.88s`
- `python3 -m compileall src tests` -> success
- `uv run pytest -q tests/test_account_analytics.py` -> `3 passed`
- `uv run pytest -q tests/test_account_analytics.py` -> `5 passed`
- `uv run pytest -q tests/test_account_analytics.py` -> `8 passed`
- `uv run pytest -q tests/test_author_alpha_storage.py tests/test_author_alpha_sync.py tests/test_author_alpha_execution.py tests/test_smoke.py -k "author_alpha or scheduler_registers_author_alpha_job or scheduler_dispatch_creates_author_alpha_execution_run"` -> `36 passed`
- `uv run pytest -q` -> `122 passed`
- `python3 -m compileall src tests` -> success
- `uv run pytest -q tests/test_author_alpha_storage.py tests/test_author_alpha_sync.py` -> `14 passed`
- `uv run pytest -q tests/test_author_alpha_storage.py tests/test_author_alpha_sync.py tests/test_author_alpha_execution.py` -> `28 passed`
- `uv run pytest -q` -> `127 passed`
- `uv run pytest -q tests/test_smoke.py tests/test_author_alpha_execution.py tests/test_author_alpha_sync.py` -> `100 passed`
- `uv run pytest -q` -> `139 passed`
- `python3 -m compileall src tests` -> success
- `uv run pytest -q tests/test_twitter_native_migration.py` -> `4 passed`
- `uv run pytest -q tests/test_twitter_native_migration.py` -> `11 passed`
- `uv run pytest -q tests/test_twitter_native_migration.py` -> `17 passed`
- `uv run pytest -q tests/test_smoke.py tests/test_twitter_native_migration.py` -> `94 passed`
- `uv run pytest -q tests/test_author_alpha_execution.py tests/test_account_analytics.py` -> `26 passed`
- `uv run pytest -q` -> `147 passed`
- `python3 -m compileall src tests` -> success
- `uv run pytest -q` -> `154 passed`
- `python3 -m compileall src tests` -> success
- `uv run pytest -q` -> `160 passed`
- `python3 -m compileall src tests` -> success
- `uv run pytest -q` -> `161 passed`
- `python3 -m compileall src tests` -> success
- `uv run pytest -q tests/test_x_web_transport.py tests/test_account_analytics.py` -> `13 passed`
- `uv run pytest -q tests/test_x_web_shared.py tests/test_x_web_transport.py tests/test_account_analytics.py tests/test_author_alpha_sync.py -k "x_web or fetch_tweet_enriches"` -> `7 passed`
- `uv run pytest -q` -> `163 passed`
- `python3 -m compileall src tests` -> success
- `uv run pytest -q` -> `166 passed`
- `python3 -m compileall src tests` -> success
- `uv run pytest -q tests/test_x_timeline.py tests/test_x_web_shared.py tests/test_account_analytics.py` -> `20 passed`
- `uv run pytest -q` -> `172 passed`
- `python3 -m compileall src tests` -> success
- `uv run pytest -q tests/test_twitter_native_adapters.py tests/test_twitter_native_migration.py` -> `19 passed`
- `uv run pytest -q` -> `174 passed`
- `python3 -m compileall src tests` -> success
- `uv run pytest -q tests/test_x_parser.py tests/test_twitter_native_adapters.py tests/test_twitter_native_migration.py` -> `22 passed`
- `uv run pytest -q` -> `177 passed`
- `python3 -m compileall src tests` -> success
- `uv run pytest -q tests/test_x_native_client.py tests/test_twitter_native_adapters.py tests/test_twitter_native_migration.py` -> `20 passed`
- `uv run pytest -q` -> `180 passed`
- `python3 -m compileall src tests` -> success
- `uv run pytest -q tests/test_vendored_surface.py tests/test_twitter_native_migration.py tests/test_x_parser.py` -> `22 passed`
- `uv run pytest -q` -> `179 passed`
- `python3 -m compileall src tests` -> success
- `uv run pytest -q tests/test_no_vendored_runtime.py tests/test_vendored_surface.py tests/test_twitter_native_adapters.py tests/test_twitter_native_migration.py tests/test_x_parser.py tests/test_x_native_client.py` -> `27 passed`
- `uv run pytest -q tests/test_no_vendored_runtime.py tests/test_vendored_surface.py` -> `4 passed`
- `uv run pytest -q` -> `182 passed`
- `python3 -m compileall src tests` -> success
- `uv run pytest -q tests/test_smoke.py -k "feed_engage_manual_execute_route_runs_once_without_scheduler or author_alpha_manual_execute_route_runs_once_without_scheduler"` -> `2 passed`
- `uv run pytest -q` -> `183 passed`
- `python3 -m compileall src tests` -> success
- `uv run pytest -q tests/test_project_docs.py` -> `2 passed`
- `uv run pytest -q` -> `185 passed`
- `python3 -m compileall src tests` -> success
- `uv run pytest -q tests/test_author_alpha_execution.py -k "shared_engagement or already_triggered_by_feed_engage"` -> `2 passed`
- `uv run pytest -q tests/test_smoke.py -k "run_author_alpha_request_passes_real_asyncio_sleep or author_alpha_manual_execute_route_runs_once_without_scheduler or scheduler_dispatch_creates_author_alpha_execution_run"` -> `3 passed`
- `uv run pytest -q` -> `187 passed`
- `python3 -m compileall src tests` -> success

## Remaining Risks

- `progress.md` is now a current-state document, not a step-by-step work log.
- X Web analytics is login-cookie based and private/unstable; query ids and payload shapes may change independently of the public X API.
- Content page filtering and sorting is inferred from the current X Web bundle and mirrored server-side; if X changes client logic, backend parity may drift.
- Notifications parsing is also based on X Web private timeline payloads and may drift when X changes timeline templates or rich-message structure.
- The X read/write stack is now unified through x-atuo-owned runtime/transport/spec/normalization/timeline/parser/native-client helpers; the vendored compatibility package has been removed entirely.
- Author-alpha original-target recovery depends on X detail payloads continuing to expose either the conversation root tweet author or the direct parent author.
- Author-alpha burst drafting currently reuses the generic AI draft interface with burst context rather than a dedicated burst-plan model surface; the lane behavior is correct and tested, but the planning semantics remain thinner than the full design aspiration.
- The shared engagement ledger currently dedupes at the target-tweet level across workflows; if future requirements need cross-workflow coexistence on the same target, the shared ledger policy will need to become workflow-aware rather than binary.
- If deeper historical context is needed, use the archive below.

## Archive

- Historical task-by-task progress has moved to `docs/superpowers/history/2026-04-24-progress-archive.md`

## Final Report

- Completed the end-to-end migration away from the external `twitter-cli` runtime dependency.
- Consolidated the X capability stack into x-atuo-owned layers:
  - `twitter_runtime`
  - `x_web_transport`
  - `x_graphql`
  - `x_web_normalization`
  - `x_timeline`
  - `x_parser`
  - `twitter_native_adapters`
  - `x_native_client`
- Rewired analytics, notifications, author-alpha, and the shared `TwitterClient` wrapper onto those layers.
- Exposed the first batch of X read capabilities through FastAPI while keeping author-alpha execution stable.
- Removed the vendored `twitter_cli` package and dropped obsolete CLI-only dependencies.

Main achievements:
- The service no longer depends on the external `twitter` executable or the external `/Users/aias/Work/github/twitter-cli` repo at runtime.
- Current runtime behavior is covered by `187` passing tests.
- Local service has been restarted successfully on the final code.

Future recommendations:
- Add first-class write HTTP routes for `like / bookmark / retweet / delete` if you want these capabilities callable externally, not just from core.
- Consider adding more robust write-side idempotency and duplicate-detection around reply/post mutations.
- If X payload drift becomes frequent, centralize additional response-shape probes and diagnostics around the shared `x_graphql` layer.
