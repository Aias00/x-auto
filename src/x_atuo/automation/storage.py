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
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO engagements (run_id, target_tweet_id, target_author, target_tweet_url, reply_tweet_id, reply_url, followed, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, target_tweet_id, target_author, target_tweet_url, reply_tweet_id, reply_url, int(followed), utcnow()),
            )

    def has_target_tweet_id(self, target_tweet_id: str) -> bool:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT target_tweet_id
                FROM engagements
                WHERE target_tweet_id = ?
                LIMIT 1
                """,
                (target_tweet_id,),
            ).fetchone()
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
