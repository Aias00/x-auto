# Changelog

## 0.2.0 - 2026-04-20

### Features
- Harden `feed-engage` with candidate refresh loops, newest-first feed ordering, and shortlist caching with lease-based reuse.
- Add lightweight node observability for fetch, moderation, selection, review, drafting, and execution timings.
- Add structured tweet timestamps and richer candidate handling for feed selection and reuse.

### Reliability
- Prevent duplicate replies by executing only the selected candidate and re-entering graph selection on fallback paths.
- Reuse hydrated shortlist candidates without redundant tweet fetches and clear stale running jobs on service startup.
- Clamp fallback replies to policy limits and improve scheduler/cache recovery behavior.

### Safety
- Expand AI moderation coverage to adult, hate, self-harm, gambling, extremism, crypto shills, and high-risk medical/legal content.
- Apply a final full-text moderation review to the selected candidate before drafting and execution.

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
