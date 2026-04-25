# x-atuo

Twitter automation service for scheduler-driven `feed-engage` workflows, with LangGraph orchestration and AI-required execution.

## Scope

- FastAPI read-only host for lifecycle, health, and run lookup
- LangGraph workflow orchestration
- SQLite-backed run and dedupe state
- Twitter core runner and engagement service
- APScheduler-backed `feed-engage` execution

## Implemented Endpoints

- `GET /healthz`
- `GET /runs/{run_id}`

## Runtime Notes

- Twitter execution reads credentials from `~/.agent-reach/config.yaml`
- If `twitter_auth_token` / `twitter_ct0` are absent there, it falls back to `TWITTER_AUTH_TOKEN` and `TWITTER_CT0`
- Proxy can be passed per request, or defaulted in automation config
- Langfuse tracing is optional
  - set `LANGFUSE_PUBLIC_KEY` and `LANGFUSE_SECRET_KEY` to enable it
  - `LANGFUSE_BASE_URL` and `LANGFUSE_TRACING_ENVIRONMENT` are optional Langfuse client settings
  - when the required keys are absent, the runtime falls back to a no-op observability path
- Scheduler is the only execution entrypoint
  - `feed-engage` is the only remaining executable workflow
  - FastAPI no longer exposes write routes under `/hooks/twitter/*`
- `feed-engage` requires an AI provider for moderation and drafting; politics, crime, violence, fraud, drugs, war, and law-enforcement / case-news content is filtered out before execution
- `feed-engage` keeps the feed candidates in newest-first order, then walks that candidate pool one-by-one: hydrate one candidate, validate it, and stop at the first acceptable candidate before candidate-policy checks and drafting
- hydrated candidate-pool entries are cached in SQLite and reused across runs before refetching feed; cached candidates are leased to a single run at a time and expire automatically
- AI node events include per-call latency and input-size metrics for moderation, style classification, and drafting
- scheduled `feed-engage` runs are queued behind a single in-process worker with a bounded backlog; when the backlog is full, new scheduled requests are recorded as blocked runs instead of silently disappearing
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
