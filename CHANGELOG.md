# Changelog

## 0.1.0 - 2026-04-18

### Features
- Add FastAPI webhook service with `feed-engage`, `repo-post`, and `direct-post` workflows.
- Add LangGraph-driven automation runs backed by SQLite state, audit events, and scheduler support.
- Add AI-assisted candidate moderation, candidate selection, and reply drafting for automation flows.

### Reliability
- Serialize scheduled work through a bounded in-process queue with dropped-run auditing.
- Retry `feed-engage` across later candidates when earlier ones are already engaged or reply-restricted.
- Reuse enriched tweet payloads during execution to reduce duplicate fetches.

### Safety
- Filter political, crime, violence, fraud, drug, war, and law-enforcement/case-news content before engagement.
- Remove the `explicit-engage` workflow to narrow the exposed execution surface.
