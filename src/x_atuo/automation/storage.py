from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterator


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _serialize_json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _deserialize_json(value: str | None) -> Any:
    if value is None:
        return None
    return json.loads(value)


def _strip_internal_metadata(value: dict[str, Any]) -> dict[str, Any]:
    return {
        key: item
        for key, item in value.items()
        if not str(key).startswith("_x_atuo_")
    }


class AutomationStorage:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path).expanduser().resolve()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path, timeout=10.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    config_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    job_type TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    status TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    response_json TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    FOREIGN KEY(job_id) REFERENCES jobs(id)
                );

                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    level TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    node TEXT,
                    payload_json TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES runs(id)
                );

                CREATE INDEX IF NOT EXISTS idx_runs_job_id ON runs(job_id);
                CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
                CREATE INDEX IF NOT EXISTS idx_audit_events_run_id ON audit_events(run_id);

                CREATE TABLE IF NOT EXISTS dedupe_keys (
                    dedupe_key TEXT PRIMARY KEY,
                    scope TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT
                );

                CREATE TABLE IF NOT EXISTS engagements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    target_tweet_id TEXT,
                    target_author TEXT,
                    target_tweet_url TEXT,
                    reply_tweet_id TEXT,
                    reply_url TEXT,
                    followed INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES runs(id)
                );

                CREATE TABLE IF NOT EXISTS shared_engagements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    workflow TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    target_tweet_id TEXT,
                    target_author TEXT,
                    target_tweet_url TEXT,
                    reply_tweet_id TEXT,
                    reply_url TEXT,
                    followed INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS candidate_cache (
                    workflow TEXT NOT NULL,
                    tweet_id TEXT NOT NULL,
                    screen_name TEXT,
                    created_at TEXT,
                    text TEXT,
                    metadata_json TEXT,
                    status TEXT NOT NULL,
                    reason TEXT,
                    source_run_id TEXT,
                    claim_run_id TEXT,
                    claim_expires_at TEXT,
                    hydrated_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    created_ts TEXT NOT NULL,
                    updated_ts TEXT NOT NULL,
                    PRIMARY KEY (workflow, tweet_id)
                );

                """
            )
            columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(engagements)").fetchall()
            }
            if "target_tweet_url" not in columns:
                connection.execute("ALTER TABLE engagements ADD COLUMN target_tweet_url TEXT")
            if "reply_url" not in columns:
                connection.execute("ALTER TABLE engagements ADD COLUMN reply_url TEXT")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_shared_engagements_target_tweet_id ON shared_engagements(target_tweet_id)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_shared_engagements_workflow_target ON shared_engagements(workflow, target_tweet_id)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_shared_engagements_author_created ON shared_engagements(target_author, created_at)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_candidate_cache_workflow_status_expires ON candidate_cache(workflow, status, expires_at)"
            )
            cache_columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(candidate_cache)").fetchall()
            }
            if "claim_run_id" not in cache_columns:
                connection.execute("ALTER TABLE candidate_cache ADD COLUMN claim_run_id TEXT")
            if "claim_expires_at" not in cache_columns:
                connection.execute("ALTER TABLE candidate_cache ADD COLUMN claim_expires_at TEXT")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_candidate_cache_source_run_id ON candidate_cache(source_run_id)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_candidate_cache_claim_run_id ON candidate_cache(claim_run_id)"
            )

    @staticmethod
    def _release_expired_candidate_claims(connection: sqlite3.Connection, *, now: str) -> int:
        cursor = connection.execute(
            """
            UPDATE candidate_cache
            SET status = 'pending', claim_run_id = NULL, claim_expires_at = NULL, updated_ts = ?
            WHERE status = 'claimed' AND claim_expires_at IS NOT NULL AND claim_expires_at <= ? AND expires_at > ?
            """,
            (now, now, now),
        )
        return int(cursor.rowcount or 0)

    def healthcheck(self) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute("SELECT 1 AS ok").fetchone()
        return {
            "status": "ok" if row and row["ok"] == 1 else "error",
            "db_path": str(self.db_path),
            "checked_at": datetime.now(timezone.utc),
        }

    def upsert_job(self, job_id: str, job_type: str, config: dict[str, Any] | None = None) -> None:
        now = utcnow()
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO jobs (id, job_type, config_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    job_type = excluded.job_type,
                    config_json = excluded.config_json,
                    updated_at = excluded.updated_at
                """,
                (job_id, job_type, _serialize_json(config), now, now),
            )

    def create_run(
        self,
        *,
        run_id: str,
        job_id: str,
        job_type: str,
        endpoint: str,
        request_payload: dict[str, Any],
        status: str = "queued",
    ) -> None:
        now = utcnow()
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO runs (
                    id, job_id, job_type, endpoint, status, request_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    job_id,
                    job_type,
                    endpoint,
                    status,
                    _serialize_json(request_payload) or "{}",
                    now,
                    now,
                ),
            )

    def update_run(
        self,
        run_id: str,
        *,
        status: str | None = None,
        response_payload: Any = None,
        error: str | None = None,
        started_at: str | None = None,
        finished_at: str | None = None,
    ) -> None:
        assignments: list[str] = ["updated_at = ?"]
        parameters: list[Any] = [utcnow()]
        if status is not None:
            assignments.append("status = ?")
            parameters.append(status)
        if response_payload is not None:
            assignments.append("response_json = ?")
            parameters.append(_serialize_json(response_payload))
        if error is not None:
            assignments.append("error = ?")
            parameters.append(error)
        if started_at is not None:
            assignments.append("started_at = ?")
            parameters.append(started_at)
        if finished_at is not None:
            assignments.append("finished_at = ?")
            parameters.append(finished_at)
        parameters.append(run_id)
        with self.connect() as connection:
            connection.execute(
                f"UPDATE runs SET {', '.join(assignments)} WHERE id = ?",
                parameters,
            )

    def add_audit_event(
        self,
        *,
        run_id: str,
        event_type: str,
        payload: Any = None,
        level: str = "info",
        node: str | None = None,
    ) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO audit_events (run_id, level, event_type, node, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (run_id, level, event_type, node, _serialize_json(payload), utcnow()),
            )
            return int(cursor.lastrowid)

    def clear_stale_running_runs(self, *, reason: str) -> list[str]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT id
                FROM runs
                WHERE status = 'running'
                ORDER BY created_at ASC
                """
            ).fetchall()

        cleared: list[str] = []
        for row in rows:
            run_id = str(row["id"])
            self.update_run(
                run_id,
                status="failed",
                error=reason,
                finished_at=utcnow(),
            )
            self.add_audit_event(
                run_id=run_id,
                event_type="stale_running_cleared",
                node="service",
                payload={"reason": reason},
            )
            cleared.append(run_id)
        return cleared

    def cleanup_candidate_cache(self) -> int:
        now = utcnow()
        with self.connect() as connection:
            released = self._release_expired_candidate_claims(connection, now=now)
            cursor = connection.execute(
                """
                DELETE FROM candidate_cache
                WHERE expires_at <= ?
                """,
                (now,),
            )
            return released + int(cursor.rowcount or 0)

    def upsert_candidate_cache_entries(
        self,
        *,
        workflow: str,
        source_run_id: str,
        candidates: list[dict[str, Any]],
        expires_at: str,
    ) -> None:
        now = utcnow()
        with self.connect() as connection:
            for candidate in candidates:
                metadata = _strip_internal_metadata(dict(candidate.get("metadata") or {}))
                connection.execute(
                    """
                    INSERT INTO candidate_cache (
                        workflow, tweet_id, screen_name, created_at, text, metadata_json,
                        status, reason, source_run_id, claim_run_id, claim_expires_at,
                        hydrated_at, expires_at, created_ts, updated_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, 'pending', NULL, ?, NULL, NULL, ?, ?, ?, ?)
                    ON CONFLICT(workflow, tweet_id) DO UPDATE SET
                        screen_name = excluded.screen_name,
                        created_at = excluded.created_at,
                        text = excluded.text,
                        metadata_json = excluded.metadata_json,
                        status = 'pending',
                        reason = NULL,
                        source_run_id = excluded.source_run_id,
                        claim_run_id = NULL,
                        claim_expires_at = NULL,
                        hydrated_at = excluded.hydrated_at,
                        expires_at = excluded.expires_at,
                        updated_ts = excluded.updated_ts
                    """,
                    (
                        workflow,
                        str(candidate.get("tweet_id") or ""),
                        candidate.get("screen_name"),
                        candidate.get("created_at"),
                        candidate.get("text"),
                        _serialize_json(metadata),
                        source_run_id,
                        now,
                        expires_at,
                        now,
                        now,
                    ),
                )

    def claim_pending_candidate_cache(
        self,
        *,
        workflow: str,
        limit: int,
        run_id: str,
        lease_expires_at: str,
    ) -> list[dict[str, Any]]:
        now = utcnow()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._release_expired_candidate_claims(connection, now=now)
            rows = connection.execute(
                """
                SELECT tweet_id
                FROM candidate_cache
                WHERE workflow = ? AND status = 'pending' AND expires_at > ?
                ORDER BY COALESCE(created_at, created_ts) DESC, updated_ts ASC
                LIMIT ?
                """,
                (workflow, now, limit),
            ).fetchall()
            tweet_ids = [str(row["tweet_id"]) for row in rows]
            if not tweet_ids:
                return []
            placeholders = ", ".join("?" for _ in tweet_ids)
            connection.execute(
                f"""
                UPDATE candidate_cache
                SET status = 'claimed', claim_run_id = ?, claim_expires_at = ?, updated_ts = ?
                WHERE workflow = ? AND tweet_id IN ({placeholders}) AND status = 'pending'
                """,
                (run_id, lease_expires_at, now, workflow, *tweet_ids),
            )
            claimed_rows = connection.execute(
                f"""
                SELECT workflow, tweet_id, screen_name, created_at, text, metadata_json,
                       status, reason, source_run_id, claim_run_id, claim_expires_at,
                       hydrated_at, expires_at, created_ts, updated_ts
                FROM candidate_cache
                WHERE workflow = ? AND claim_run_id = ? AND tweet_id IN ({placeholders})
                ORDER BY COALESCE(created_at, created_ts) DESC, updated_ts ASC
                """,
                (workflow, run_id, *tweet_ids),
            ).fetchall()
        return [
            {
                "workflow": row["workflow"],
                "tweet_id": row["tweet_id"],
                "screen_name": row["screen_name"],
                "created_at": row["created_at"],
                "text": row["text"],
                "metadata": _deserialize_json(row["metadata_json"]) or {},
                "status": row["status"],
                "reason": row["reason"],
                "source_run_id": row["source_run_id"],
                "claim_run_id": row["claim_run_id"],
                "claim_expires_at": row["claim_expires_at"],
                "hydrated_at": row["hydrated_at"],
                "expires_at": row["expires_at"],
            }
            for row in claimed_rows
        ]

    def list_pending_candidate_cache(self, *, workflow: str, limit: int) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT workflow, tweet_id, screen_name, created_at, text, metadata_json,
                       status, reason, source_run_id, claim_run_id, claim_expires_at,
                       hydrated_at, expires_at, created_ts, updated_ts
                FROM candidate_cache
                WHERE workflow = ? AND status = 'pending' AND expires_at > ?
                ORDER BY COALESCE(created_at, created_ts) DESC, updated_ts ASC
                LIMIT ?
                """,
                (workflow, utcnow(), limit),
            ).fetchall()
        return [
            {
                "workflow": row["workflow"],
                "tweet_id": row["tweet_id"],
                "screen_name": row["screen_name"],
                "created_at": row["created_at"],
                "text": row["text"],
                "metadata": _deserialize_json(row["metadata_json"]) or {},
                "status": row["status"],
                "reason": row["reason"],
                "source_run_id": row["source_run_id"],
                "claim_run_id": row["claim_run_id"],
                "claim_expires_at": row["claim_expires_at"],
                "hydrated_at": row["hydrated_at"],
                "expires_at": row["expires_at"],
            }
            for row in rows
        ]

    def release_claimed_candidate_cache(
        self,
        *,
        workflow: str,
        run_id: str,
        tweet_ids: list[str],
    ) -> int:
        if not tweet_ids:
            return 0
        now = utcnow()
        placeholders = ", ".join("?" for _ in tweet_ids)
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                f"""
                UPDATE candidate_cache
                SET status = 'pending', claim_run_id = NULL, claim_expires_at = NULL, updated_ts = ?
                WHERE workflow = ? AND claim_run_id = ? AND status = 'claimed' AND tweet_id IN ({placeholders})
                """,
                (now, workflow, run_id, *tweet_ids),
            )
            return int(cursor.rowcount or 0)

    def reject_candidate_cache(self, *, workflow: str, tweet_id: str, reason: str, expires_at: str) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE candidate_cache
                SET status = 'rejected', reason = ?, expires_at = ?, claim_run_id = NULL, claim_expires_at = NULL, updated_ts = ?
                WHERE workflow = ? AND tweet_id = ?
                """,
                (reason, expires_at, utcnow(), workflow, tweet_id),
            )

    def consume_candidate_cache(self, *, workflow: str, tweet_id: str) -> None:
        with self.connect() as connection:
            connection.execute(
                "DELETE FROM candidate_cache WHERE workflow = ? AND tweet_id = ?",
                (workflow, tweet_id),
            )

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            run_row = connection.execute(
                """
                SELECT id, job_id, job_type, endpoint, status, request_json, response_json,
                       error, created_at, updated_at, started_at, finished_at
                FROM runs
                WHERE id = ?
                """,
                (run_id,),
            ).fetchone()
            if run_row is None:
                return None
            audit_rows = connection.execute(
                """
                SELECT id, run_id, level, event_type, node, payload_json, created_at
                FROM audit_events
                WHERE run_id = ?
                ORDER BY id ASC
                """,
                (run_id,),
            ).fetchall()

        return {
            "run": {
                "id": run_row["id"],
                "job_id": run_row["job_id"],
                "job_type": run_row["job_type"],
                "endpoint": run_row["endpoint"],
                "status": run_row["status"],
                "request_payload": _deserialize_json(run_row["request_json"]) or {},
                "response_payload": _deserialize_json(run_row["response_json"]),
                "error": run_row["error"],
                "created_at": run_row["created_at"],
                "updated_at": run_row["updated_at"],
                "started_at": run_row["started_at"],
                "finished_at": run_row["finished_at"],
            },
            "audit_events": [
                {
                    "id": row["id"],
                    "run_id": row["run_id"],
                    "level": row["level"],
                    "event_type": row["event_type"],
                    "node": row["node"],
                    "payload": _deserialize_json(row["payload_json"]),
                    "created_at": row["created_at"],
                }
                for row in audit_rows
            ],
        }

    def has_dedupe_key(self, dedupe_key: str) -> bool:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT dedupe_key FROM dedupe_keys WHERE dedupe_key = ?",
                (dedupe_key,),
            ).fetchone()
        return row is not None

    def store_dedupe_key(self, dedupe_key: str, scope: str, expires_at: str | None = None) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO dedupe_keys (dedupe_key, scope, created_at, expires_at)
                VALUES (?, ?, ?, ?)
                """,
                (dedupe_key, scope, utcnow(), expires_at),
            )

    def record_engagement(
        self,
        *,
        run_id: str,
        target_tweet_id: str | None,
        target_author: str | None,
        target_tweet_url: str | None,
        reply_tweet_id: str | None,
        reply_url: str | None,
        followed: bool,
    ) -> None:
        created_at = utcnow()
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO engagements (run_id, target_tweet_id, target_author, target_tweet_url, reply_tweet_id, reply_url, followed, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, target_tweet_id, target_author, target_tweet_url, reply_tweet_id, reply_url, int(followed), created_at),
            )
        self.record_shared_engagement(
            workflow="feed_engage",
            run_id=run_id,
            target_tweet_id=target_tweet_id,
            target_author=target_author,
            target_tweet_url=target_tweet_url,
            reply_tweet_id=reply_tweet_id,
            reply_url=reply_url,
            followed=followed,
            created_at=created_at,
        )

    def record_shared_engagement(
        self,
        *,
        workflow: str,
        run_id: str,
        target_tweet_id: str | None,
        target_author: str | None,
        target_tweet_url: str | None,
        reply_tweet_id: str | None,
        reply_url: str | None,
        followed: bool,
        created_at: str | None = None,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO shared_engagements (
                    workflow,
                    run_id,
                    target_tweet_id,
                    target_author,
                    target_tweet_url,
                    reply_tweet_id,
                    reply_url,
                    followed,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    workflow,
                    run_id,
                    target_tweet_id,
                    target_author,
                    target_tweet_url,
                    reply_tweet_id,
                    reply_url,
                    int(followed),
                    created_at or utcnow(),
                ),
            )

    def has_target_tweet_id(self, target_tweet_id: str, *, exclude_workflows: tuple[str, ...] | None = None) -> bool:
        query = """
                SELECT target_tweet_id
                FROM shared_engagements
                WHERE target_tweet_id = ?
        """
        parameters: list[Any] = [target_tweet_id]
        if exclude_workflows:
            placeholders = ", ".join("?" for _ in exclude_workflows)
            query += f" AND workflow NOT IN ({placeholders})"
            parameters.extend(exclude_workflows)
        query += " LIMIT 1"
        with self.connect() as connection:
            row = connection.execute(query, tuple(parameters)).fetchone()
        return row is not None

    def get_daily_execution_count(self, workflow: str, day: date) -> int:
        normalized = workflow.value if hasattr(workflow, "value") else str(workflow)
        normalized = normalized.replace("-", "_")
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM runs
                WHERE job_type = ? AND date(created_at) = ?
                """,
                (normalized, day.isoformat()),
            ).fetchone()
        return int(row["count"]) if row else 0

    def get_last_author_engagement(self, screen_name: str) -> datetime | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT created_at
                FROM engagements
                WHERE target_author = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (screen_name,),
            ).fetchone()
        if row is None or not row["created_at"]:
            return None
        return datetime.fromisoformat(row["created_at"])
