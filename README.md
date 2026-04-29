# x-atuo

Twitter automation service for scheduler-driven reply workflows, with independent `feed-engage` and `author-alpha-engage` lanes plus cookie-authenticated X Web analytics and notifications readers.

## Scope

- FastAPI read-only host for lifecycle, health, and run lookup
- LangGraph workflow orchestration
- SQLite-backed run and dedupe state
- Twitter core runner and engagement service
- APScheduler-backed `feed-engage` and `author-alpha-engage` execution

## Implemented Endpoints

- `GET /healthz`
- `GET /runs/{run_id}`
- `GET /analytics/account`
- `GET /analytics/account/content`
- `GET /notifications`
- `GET /twitter/search`
- `GET /twitter/bookmarks`
- `GET /twitter/bookmarks/folders`
- `GET /twitter/bookmarks/folders/{folder_id}`
- `GET /twitter/users/{screen_name}/likes`
- `GET /twitter/users/{screen_name}/followers`
- `GET /twitter/users/{screen_name}/following`
- `GET /twitter/articles/{tweet_id}`
- `POST /feed-engage/execute`
- `POST /author-alpha/sync/bootstrap`
- `POST /author-alpha/sync/reconcile`
- `POST /author-alpha/sync/stop`
- `GET /author-alpha/sync/status`
- `GET /author-alpha/sync/history`
- `GET /author-alpha/runs/{run_id}`
- `POST /author-alpha/reset`
- `POST /author-alpha/execute`

## Runtime Notes

- Twitter execution now uses the in-repo native X client rather than an external `twitter` binary
- Runtime credentials still read from `~/.agent-reach/config.yaml`
- If `twitter_auth_token` / `twitter_ct0` are absent there, it falls back to `TWITTER_AUTH_TOKEN` and `TWITTER_CT0`
- Proxy can be passed per request, or defaulted in automation config
- `GET /analytics/account` uses the same X login cookie auth as the rest of the runtime
  - It reads `twitter_auth_token` / `twitter_ct0` from `~/.agent-reach/config.yaml`
  - If those are absent there, it falls back to `TWITTER_AUTH_TOKEN` / `TWITTER_CT0`
  - The endpoint reads X Web GraphQL queries behind `x.com/i/account_analytics`, not the official public X REST API
  - Account summary comes from `accountOverviewQuery`; post list and per-post totals come from `contentPostListQuery`
  - `GET /analytics/account/content` supports the content-page style filters:
    - `type=all|posts|replies|community`
    - `sort=date|impressions|likes|replies|reposts`
    - `dir=asc|desc`
    - `from=YYYY-MM-DD`
    - `to=YYYY-MM-DD`
    - `limit=1..500`
- `GET /notifications` uses the same X login cookie auth
  - It reads X Web `NotificationsTimeline`
  - Supported `timeline_type`: `All`, `Mentions`, `Priority`, `Verified`, `SuperFollowers`
  - Supported params: `count`, `cursor`
- Langfuse tracing is optional
  - set `LANGFUSE_PUBLIC_KEY` and `LANGFUSE_SECRET_KEY` to enable it
  - `LANGFUSE_BASE_URL` and `LANGFUSE_TRACING_ENVIRONMENT` are optional Langfuse client settings
  - when the required keys are absent, the runtime falls back to a no-op observability path
- Execution entrypoints now include both scheduler-driven and manual single-run surfaces
  - scheduled workflows remain `feed-engage` and `author-alpha-engage`
  - manual single-run validation is available through `POST /feed-engage/execute` and `POST /author-alpha/execute`
  - FastAPI no longer exposes write routes under `/hooks/twitter/*`
- `feed-engage` requires an AI provider for moderation and drafting; politics, crime, violence, fraud, drugs, war, and law-enforcement / case-news content is filtered out before execution
- `feed-engage` keeps the feed candidates in newest-first order, then walks that candidate pool one-by-one: hydrate one candidate, validate it, and stop at the first acceptable candidate before candidate-policy checks and drafting
- hydrated candidate-pool entries are cached in SQLite and reused across runs before refetching feed; cached candidates are leased to a single run at a time and expire automatically
- `author-alpha-engage` keeps a separate SQLite file and execution history
  - author ordering is recomputed from account-wide reply analytics over the last 7 days
  - candidates are sourced from the X notifications `device_follow` feed and then re-ranked locally by author score
  - same target post may burst up to `2` direct replies in one run, with `5` second same-target spacing and `10` second cross-target spacing
  - the same target post is capped at `4` successful replies across this lane's lifetime
  - each successful burst reply is tagged in `alpha_engagements` with `burst_id`, `burst_index`, and `burst_size`
  - default lane ceilings are `700` successful replies per day and `50` successful replies per rolling 15 minutes
- author-alpha historical sync is a separate control surface
  - `POST /author-alpha/sync/bootstrap` backfills a date range day by day
  - `POST /author-alpha/sync/reconcile` refreshes one day, defaulting to yesterday in the configured lane timezone
  - `POST /author-alpha/sync/stop` requests cooperative cancellation of the currently active bootstrap/reconcile run
  - `GET /author-alpha/sync/status` reports active/latest sync state and checkpoints
  - `GET /author-alpha/sync/history` lists recent sync runs
  - `GET /author-alpha/runs/{run_id}` looks up author-alpha execution runs first, then sync runs
  - `POST /author-alpha/reset` clears author-alpha sync, score, engagement, and execution-run data when no sync is active
- `POST /author-alpha/execute` runs one author-alpha execution immediately without scheduler registration and works even when `X_ATUO_SCHEDULER__ENABLED=false`
  - it reuses the same execution logic as the scheduled lane, so manual validation also exercises the real candidate fetch / draft / execute path
- `POST /feed-engage/execute` runs one feed-engage execution immediately without scheduler registration and works even when `X_ATUO_SCHEDULER__ENABLED=false`
  - it reuses the same execution logic as the scheduled lane, so manual validation also exercises the real candidate selection / draft / execute path
- current defaults cap one run to at most `5` target posts, with at most `2` direct replies per target burst
- the external `/Users/aias/Work/github/twitter-cli` repository is no longer required at runtime
- AI node events include per-call latency and input-size metrics for moderation, style classification, and drafting
- scheduled `feed-engage` runs are queued behind a single in-process worker with a bounded backlog; when the backlog is full, new scheduled requests are recorded as blocked runs instead of silently disappearing
- scheduled `author-alpha-engage` runs use the same in-process scheduler queue, but dropped executions are persisted in the author-alpha SQLite instead of the main workflow DB
- AI drafting/selection is required for the active runtime path
  - `X_ATUO_AI__PROVIDER=mock` enables local fake AI for testing
  - `X_ATUO_AI__PROVIDER=openai_compatible` enables HTTP chat-completions calls
  - set `X_ATUO_AI__MODEL`, `X_ATUO_AI__API_KEY`, and optionally `X_ATUO_AI__BASE_URL`
- the runtime no longer supports a deterministic no-AI execution mode for `feed-engage`

## Development

Preferred with `uv`:

```bash
cd /Users/aias/workspace/x-atuo
uv sync --extra dev
uv run pytest -q
PYTHONPATH=src uv run uvicorn x_atuo.automation.api:app --host 0.0.0.0 --port 18000 --reload
```

Traditional `venv + pip`:

```bash
cd /Users/aias/workspace/x-atuo
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
python -m pytest -q
PYTHONPATH=src python -m uvicorn x_atuo.automation.api:app --host 0.0.0.0 --port 18000 --reload
```

Notes:

- `uv` is the shortest path because it creates and uses the project virtualenv automatically.
- If you use the `venv` flow, create the environment with `python3 -m venv .venv` first. Outside an activated virtualenv, bare `python` may point to the macOS system stub.
- Scheduler control lives under `X_ATUO_SCHEDULER__*`:
  - `ENABLED=true` turns scheduler support on
  - `AUTOSTART=true` starts the scheduler automatically when the service boots
  - `FEED_ENGAGE_ENABLED=true` enables the scheduled `feed-engage` job
  - `FEED_ENGAGE_TRIGGER`, `FEED_ENGAGE_SECONDS`, `FEED_ENGAGE_JITTER_SECONDS`, `FEED_ENGAGE_MINUTE`, `FEED_ENGAGE_HOUR`, `FEED_ENGAGE_DAY`, `FEED_ENGAGE_DAY_OF_WEEK` shape the schedule
- Author-alpha control lives under `X_ATUO_AUTHOR_ALPHA__*`:
  - `ENABLED=true` enables the scheduled `author-alpha-engage` job
  - `DB_PATH` selects the separate author-alpha SQLite path
  - `TIMEZONE` controls day partitioning for sync and daily caps
  - `EXCLUDED_AUTHORS` removes blocked or unwanted authors from author-alpha scoring and execution ordering
  - `TRIGGER`, `SECONDS`, `JITTER_SECONDS`, `MINUTE`, `HOUR` shape the schedule
  - `SCORE_LOOKBACK_DAYS` defaults to `7`
  - `SCORE_MIN_DAILY_REPLIES` defaults to `400` and filters the score model to high-volume days only
  - `SCORE_PRIOR_WEIGHT` defaults to `7.0`
  - `SCORE_PENALTY_CONSTANT` defaults to `200.0`
  - `DEVICE_FOLLOW_FEED_COUNT` defaults to `50`
  - `DAILY_EXECUTION_LIMIT` defaults to `700`
  - `GLOBAL_SEND_LIMIT_15M` defaults to `50`
  - `PER_AUTHOR_DAILY_SUCCESS_LIMIT` defaults to `100`
  - `PER_TARGET_TWEET_SUCCESS_LIMIT` defaults to `4`
  - `TARGET_REVISIT_COOLDOWN_SECONDS` defaults to `3600`
  - `MAX_TARGETS_PER_RUN` defaults to `5`
  - `PER_RUN_SAME_TARGET_BURST_LIMIT` defaults to `2`
  - `POSTS_PER_AUTHOR` defaults to `1`
  - `AUTHOR_COOLDOWN_SECONDS` defaults to `3`
  - `INTER_REPLY_DELAY_SECONDS` defaults to `5`
  - `TARGET_SWITCH_DELAY_SECONDS` defaults to `10`
- Candidate-pool tuning lives under `X_ATUO_POLICIES__*`:
  - `CANDIDATE_REFRESH_ROUNDS`: how many times `feed-engage` may refetch when the pool empties
  - `CANDIDATE_CACHE_PENDING_TTL_MINUTES`: how long pending hydrated candidate-pool entries stay reusable
  - `CANDIDATE_CACHE_REJECTED_TTL_MINUTES`: how long rejected shortlist entries are retained for diagnostics
  - `CANDIDATE_CACHE_CLAIM_TTL_MINUTES`: how long a run leases claimed shortlist entries before they return to `pending`

## Example Calls

Health:

```bash
curl -s http://127.0.0.1:18000/healthz | jq
```

Lookup a run:

```bash
curl -s http://127.0.0.1:18000/runs/<run_id> | jq
```

Fetch account analytics:

```bash
curl -s "http://127.0.0.1:18000/analytics/account?days=28&post_limit=10&granularity=total" | jq
```

Fetch content analytics with page-style filters:

```bash
curl -s "http://127.0.0.1:18000/analytics/account/content?type=replies&sort=impressions&dir=desc&from=2026-04-25&to=2026-04-25&limit=20" | jq
```

Fetch notifications:

```bash
curl -s "http://127.0.0.1:18000/notifications?timeline_type=All&count=20" | jq
```

Search tweets:

```bash
curl -s "http://127.0.0.1:18000/twitter/search?q=openai&limit=10&product=Latest" | jq
```

Fetch bookmarks and bookmark folders:

```bash
curl -s "http://127.0.0.1:18000/twitter/bookmarks?limit=20" | jq
curl -s "http://127.0.0.1:18000/twitter/bookmarks/folders" | jq
curl -s "http://127.0.0.1:18000/twitter/bookmarks/folders/<folder_id>?limit=20" | jq
```

Fetch user likes and social graph:

```bash
curl -s "http://127.0.0.1:18000/twitter/users/<screen_name>/likes?limit=20" | jq
curl -s "http://127.0.0.1:18000/twitter/users/<screen_name>/followers?limit=20" | jq
curl -s "http://127.0.0.1:18000/twitter/users/<screen_name>/following?limit=20" | jq
```

Fetch an article tweet:

```bash
curl -s "http://127.0.0.1:18000/twitter/articles/<tweet_id>" | jq
```

Run one feed-engage execution immediately:

```bash
curl -s -X POST http://127.0.0.1:18000/feed-engage/execute \
  -H 'content-type: application/json' \
  -d '{"dry_run":true}' | jq
```

Bootstrap author-alpha historical sync:

```bash
curl -s -X POST http://127.0.0.1:18000/author-alpha/sync/bootstrap \
  -H 'content-type: application/json' \
  -d '{"from_date":"2026-04-01","to_date":"2026-04-07","resume":true,"max_days":3}' | jq
```

Reconcile one author-alpha day:

```bash
curl -s -X POST http://127.0.0.1:18000/author-alpha/sync/reconcile \
  -H 'content-type: application/json' \
  -d '{"target_date":"2026-04-26"}' | jq
```

Stop the active author-alpha sync run:

```bash
curl -s -X POST http://127.0.0.1:18000/author-alpha/sync/stop | jq
```

Inspect author-alpha sync status and run history:

```bash
curl -s http://127.0.0.1:18000/author-alpha/sync/status | jq
curl -s "http://127.0.0.1:18000/author-alpha/sync/history?limit=10" | jq
curl -s http://127.0.0.1:18000/author-alpha/runs/<run_id> | jq
```

Clear all local author-alpha data:

```bash
curl -s -X POST http://127.0.0.1:18000/author-alpha/reset | jq
```

Run one manual author-alpha execution:

```bash
curl -s -X POST http://127.0.0.1:18000/author-alpha/execute \
  -H 'content-type: application/json' \
  -d '{"dry_run": true}' | jq
```
