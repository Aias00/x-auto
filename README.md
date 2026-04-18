# x-atuo

Twitter automation service for webhook- and schedule-driven workflows, with LangGraph orchestration and deterministic execution runners.

## Scope

- FastAPI webhook service
- LangGraph workflow orchestration
- SQLite-backed run and dedupe state
- Twitter core runner and engagement service
- Dry-run friendly execution surface

## Implemented Endpoints

- `GET /healthz`
- `GET /runs/{run_id}`
- `POST /hooks/twitter/feed-engage`
- `POST /hooks/twitter/explicit-engage`
- `POST /hooks/twitter/repo-post`
- `POST /hooks/twitter/direct-post`

## Runtime Notes

- Twitter execution reads credentials from `~/.agent-reach/config.yaml`
- If `twitter_auth_token` / `twitter_ct0` are absent there, it falls back to `TWITTER_AUTH_TOKEN` and `TWITTER_CT0`
- Proxy can be passed per request, or defaulted in automation config
- `repo-post` currently uses deterministic GitHub fetching and deterministic text rendering, not LLM drafting yet
- AI drafting/selection is optional
  - `X_ATUO_AI__PROVIDER=mock` enables deterministic fake AI for testing
  - `X_ATUO_AI__PROVIDER=openai_compatible` enables HTTP chat-completions calls
  - set `X_ATUO_AI__MODEL`, `X_ATUO_AI__API_KEY`, and optionally `X_ATUO_AI__BASE_URL`

## AI Modes

- `deterministic`
  - no AI selection or drafting
- `ai_auto`
  - AI may select candidates and draft text when provider is configured
  - non-dry-run runs execute immediately after policy checks

## Development

```bash
cd /Users/aias/Work/github/x-atuo
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
uvicorn x_atuo.automation.api:app --host 0.0.0.0 --port 18000 --reload
```

## Example Calls

Health:

```bash
curl -s http://127.0.0.1:18000/healthz | jq
```

Feed engage dry-run:

```bash
curl -s -X POST http://127.0.0.1:18000/hooks/twitter/feed-engage \
  -H 'Content-Type: application/json' \
  -d '{"dry_run": true}' | jq
```

Feed engage real run with built-in defaults:

```bash
curl -s -X POST http://127.0.0.1:18000/hooks/twitter/feed-engage \
  -H 'Content-Type: application/json' \
  -d '{}' | jq
```

Explicit engage dry-run:

```bash
curl -s -X POST http://127.0.0.1:18000/hooks/twitter/explicit-engage \
  -H 'Content-Type: application/json' \
  -d '{
    "tweet_id": "2044477537200550383",
    "screen_name": "ShopifyEng",
    "reply_text": "Interesting benchmark breakdown.",
    "dry_run": true
  }' | jq
```

Repo post dry-run:

```bash
curl -s -X POST http://127.0.0.1:18000/hooks/twitter/repo-post \
  -H 'Content-Type: application/json' \
  -d '{
    "repo_url": "https://github.com/google/magika",
    "dry_run": true
  }' | jq
```

Direct post dry-run:

```bash
curl -s -X POST http://127.0.0.1:18000/hooks/twitter/direct-post \
  -H 'Content-Type: application/json' \
  -d '{
    "text": "hello from x-atuo",
    "dry_run": true
  }' | jq
```

Lookup a run:

```bash
curl -s http://127.0.0.1:18000/runs/<run_id> | jq
```
