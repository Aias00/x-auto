from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

from x_atuo.automation.storage import utcnow

_UNSET = object()


class AuthorAlphaStorage:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path).expanduser().resolve()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path, timeout=10.0)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS alpha_authors (
                    screen_name TEXT PRIMARY KEY,
                    author_name TEXT,
                    rest_id TEXT,
                    author_score REAL NOT NULL DEFAULT 0,
                    reply_count_7d INTEGER NOT NULL DEFAULT 0,
                    impressions_total_7d INTEGER NOT NULL DEFAULT 0,
                    avg_impressions_7d REAL NOT NULL DEFAULT 0,
                    max_impressions_7d INTEGER NOT NULL DEFAULT 0,
                    last_replied_at TEXT,
                    last_post_seen_at TEXT,
                    last_scored_at TEXT,
                    source TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS alpha_reply_daily_metrics (
                    metric_date TEXT NOT NULL,
                    reply_tweet_id TEXT NOT NULL,
                    target_tweet_id TEXT,
                    target_author TEXT,
                    impressions INTEGER NOT NULL DEFAULT 0,
                    likes INTEGER NOT NULL DEFAULT 0,
                    replies INTEGER NOT NULL DEFAULT 0,
                    reposts INTEGER NOT NULL DEFAULT 0,
                    sampled_at TEXT NOT NULL,
                    PRIMARY KEY (metric_date, reply_tweet_id)
                );

                CREATE TABLE IF NOT EXISTS alpha_author_daily_rollups (
                    metric_date TEXT NOT NULL,
                    target_author TEXT NOT NULL,
                    reply_count INTEGER NOT NULL DEFAULT 0,
                    impressions_total INTEGER NOT NULL DEFAULT 0,
                    likes_total INTEGER NOT NULL DEFAULT 0,
                    replies_total INTEGER NOT NULL DEFAULT 0,
                    reposts_total INTEGER NOT NULL DEFAULT 0,
                    avg_impressions REAL NOT NULL DEFAULT 0,
                    max_impressions INTEGER NOT NULL DEFAULT 0,
                    computed_at TEXT NOT NULL,
                    PRIMARY KEY (metric_date, target_author)
                );

                CREATE TABLE IF NOT EXISTS alpha_sync_runs (
                    run_id TEXT PRIMARY KEY,
                    run_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    from_date TEXT,
                    to_date TEXT,
                    current_date TEXT,
                    days_completed INTEGER NOT NULL DEFAULT 0,
                    days_total INTEGER NOT NULL DEFAULT 0,
                    resume_from_date TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT
                );

                CREATE TABLE IF NOT EXISTS alpha_sync_checkpoints (
                    sync_scope TEXT PRIMARY KEY,
                    last_completed_date TEXT,
                    next_pending_date TEXT,
                    last_run_id TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS alpha_engagements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    target_author TEXT NOT NULL,
                    target_tweet_id TEXT NOT NULL,
                    target_tweet_url TEXT,
                    reply_tweet_id TEXT NOT NULL,
                    reply_url TEXT,
                    burst_id TEXT,
                    burst_index INTEGER,
                    burst_size INTEGER,
                    metric_date TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS alpha_runs (
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
                    finished_at TEXT
                );

                CREATE TABLE IF NOT EXISTS alpha_run_audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    level TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    node TEXT,
                    payload_json TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_alpha_authors_score
                    ON alpha_authors(author_score DESC, avg_impressions_7d DESC, screen_name ASC);
                CREATE INDEX IF NOT EXISTS idx_alpha_reply_daily_metrics_author
                    ON alpha_reply_daily_metrics(metric_date, target_author);
                CREATE INDEX IF NOT EXISTS idx_alpha_author_daily_rollups_author
                    ON alpha_author_daily_rollups(metric_date, target_author);
                CREATE INDEX IF NOT EXISTS idx_alpha_engagements_target_tweet
                    ON alpha_engagements(target_tweet_id);
                CREATE INDEX IF NOT EXISTS idx_alpha_engagements_author_created
                    ON alpha_engagements(target_author, created_at);
                CREATE INDEX IF NOT EXISTS idx_alpha_engagements_created
                    ON alpha_engagements(created_at);
                CREATE INDEX IF NOT EXISTS idx_alpha_runs_created
                    ON alpha_runs(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_alpha_run_audit_events_run
                    ON alpha_run_audit_events(run_id, created_at ASC);
                """
            )
            engagement_columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(alpha_engagements)").fetchall()
            }
            if "metric_date" not in engagement_columns:
                connection.execute("ALTER TABLE alpha_engagements ADD COLUMN metric_date TEXT")
                connection.execute(
                    """
                    UPDATE alpha_engagements
                    SET metric_date = substr(created_at, 1, 10)
                    WHERE metric_date IS NULL OR metric_date = ''
                    """
                )
            if "burst_id" not in engagement_columns:
                connection.execute("ALTER TABLE alpha_engagements ADD COLUMN burst_id TEXT")
            if "burst_index" not in engagement_columns:
                connection.execute("ALTER TABLE alpha_engagements ADD COLUMN burst_index INTEGER")
            if "burst_size" not in engagement_columns:
                connection.execute("ALTER TABLE alpha_engagements ADD COLUMN burst_size INTEGER")
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_alpha_engagements_metric_date
                ON alpha_engagements(metric_date, target_author, created_at)
                """
            )

    @staticmethod
    def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {key: row[key] for key in row.keys()}

    def has_table(self, table_name: str) -> bool:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table' AND name = ?
                """,
                (table_name,),
            ).fetchone()
        return row is not None

    def upsert_author(
        self,
        *,
        screen_name: str,
        author_name: str | None,
        rest_id: str | None,
        author_score: float,
        reply_count_7d: int,
        impressions_total_7d: int,
        avg_impressions_7d: float,
        max_impressions_7d: int,
        last_replied_at: str | None,
        last_post_seen_at: str | None,
        last_scored_at: str | None,
        source: str | None,
    ) -> None:
        now = utcnow()
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO alpha_authors (
                    screen_name,
                    author_name,
                    rest_id,
                    author_score,
                    reply_count_7d,
                    impressions_total_7d,
                    avg_impressions_7d,
                    max_impressions_7d,
                    last_replied_at,
                    last_post_seen_at,
                    last_scored_at,
                    source,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(screen_name) DO UPDATE SET
                    author_name = excluded.author_name,
                    rest_id = excluded.rest_id,
                    author_score = excluded.author_score,
                    reply_count_7d = excluded.reply_count_7d,
                    impressions_total_7d = excluded.impressions_total_7d,
                    avg_impressions_7d = excluded.avg_impressions_7d,
                    max_impressions_7d = excluded.max_impressions_7d,
                    last_replied_at = excluded.last_replied_at,
                    last_post_seen_at = excluded.last_post_seen_at,
                    last_scored_at = excluded.last_scored_at,
                    source = excluded.source,
                    updated_at = excluded.updated_at
                """,
                (
                    screen_name,
                    author_name,
                    rest_id,
                    author_score,
                    reply_count_7d,
                    impressions_total_7d,
                    avg_impressions_7d,
                    max_impressions_7d,
                    last_replied_at,
                    last_post_seen_at,
                    last_scored_at,
                    source,
                    now,
                    now,
                ),
            )

    def list_authors_ordered_by_score(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        query = """
            SELECT *
            FROM alpha_authors
            ORDER BY author_score DESC, avg_impressions_7d DESC, screen_name ASC
        """
        parameters: tuple[Any, ...] = ()
        if limit is not None:
            query += " LIMIT ?"
            parameters = (limit,)
        with self.connect() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [dict(row) for row in rows]

    def count_authors(self) -> int:
        with self.connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS count FROM alpha_authors").fetchone()
        return int(row["count"]) if row else 0

    def export_score_snapshot(self) -> dict[str, Any]:
        authors = self.list_authors_ordered_by_score()
        reply_daily_metrics = self.list_reply_daily_metrics("0001-01-01", "9999-12-31")
        author_daily_rollups = self.list_author_daily_rollups("0001-01-01", "9999-12-31")
        sync_runs = self.list_sync_runs(limit=1_000_000)
        sync_checkpoints = self.list_sync_checkpoints()
        engagements = self.list_engagements()
        execution_runs = self.list_execution_runs()
        execution_audit_events = self.list_execution_audit_events()
        latest_scored_at = max(
            (str(author["last_scored_at"]) for author in authors if author.get("last_scored_at")),
            default=None,
        )
        return {
            "schema_version": 1,
            "exported_at": utcnow(),
            "author_count": len(authors),
            "reply_metric_count": len(reply_daily_metrics),
            "rollup_count": len(author_daily_rollups),
            "sync_run_count": len(sync_runs),
            "sync_checkpoint_count": len(sync_checkpoints),
            "engagement_count": len(engagements),
            "execution_run_count": len(execution_runs),
            "execution_audit_event_count": len(execution_audit_events),
            "latest_scored_at": latest_scored_at,
            "authors": authors,
            "reply_daily_metrics": reply_daily_metrics,
            "author_daily_rollups": author_daily_rollups,
            "sync_runs": sync_runs,
            "sync_checkpoints": sync_checkpoints,
            "engagements": engagements,
            "execution_runs": execution_runs,
            "execution_audit_events": execution_audit_events,
        }

    def import_score_snapshot(
        self,
        snapshot: dict[str, Any],
        *,
        replace_existing: bool = False,
    ) -> dict[str, Any]:
        schema_version = int(snapshot.get("schema_version") or 0)
        if schema_version != 1:
            raise ValueError(f"unsupported author-alpha score snapshot schema version: {schema_version}")
        authors = snapshot.get("authors")
        if not isinstance(authors, list):
            raise ValueError("author-alpha score snapshot must include an authors list")
        reply_daily_metrics = snapshot.get("reply_daily_metrics")
        if not isinstance(reply_daily_metrics, list):
            raise ValueError("author-alpha score snapshot must include a reply_daily_metrics list")
        author_daily_rollups = snapshot.get("author_daily_rollups")
        if not isinstance(author_daily_rollups, list):
            raise ValueError("author-alpha score snapshot must include an author_daily_rollups list")
        sync_runs = snapshot.get("sync_runs")
        if not isinstance(sync_runs, list):
            raise ValueError("author-alpha score snapshot must include a sync_runs list")
        sync_checkpoints = snapshot.get("sync_checkpoints")
        if not isinstance(sync_checkpoints, list):
            raise ValueError("author-alpha score snapshot must include a sync_checkpoints list")
        engagements = snapshot.get("engagements")
        if not isinstance(engagements, list):
            raise ValueError("author-alpha score snapshot must include an engagements list")
        execution_runs = snapshot.get("execution_runs")
        if not isinstance(execution_runs, list):
            raise ValueError("author-alpha score snapshot must include an execution_runs list")
        execution_audit_events = snapshot.get("execution_audit_events")
        if not isinstance(execution_audit_events, list):
            raise ValueError("author-alpha score snapshot must include an execution_audit_events list")
        now = utcnow()
        imported_count = 0
        imported_reply_metric_count = 0
        imported_rollup_count = 0
        imported_sync_run_count = 0
        imported_sync_checkpoint_count = 0
        imported_engagement_count = 0
        imported_execution_run_count = 0
        imported_execution_audit_event_count = 0
        with self.connect() as connection:
            if replace_existing:
                connection.execute("DELETE FROM alpha_authors")
                connection.execute("DELETE FROM alpha_reply_daily_metrics")
                connection.execute("DELETE FROM alpha_author_daily_rollups")
                connection.execute("DELETE FROM alpha_sync_runs")
                connection.execute("DELETE FROM alpha_sync_checkpoints")
                connection.execute("DELETE FROM alpha_engagements")
                connection.execute("DELETE FROM alpha_run_audit_events")
                connection.execute("DELETE FROM alpha_runs")
            for author in authors:
                if not isinstance(author, dict):
                    raise ValueError("author-alpha score snapshot authors entries must be objects")
                screen_name = str(author.get("screen_name") or "").strip()
                if not screen_name:
                    raise ValueError("author-alpha score snapshot author is missing screen_name")
                created_at = str(author.get("created_at") or now)
                updated_at = str(author.get("updated_at") or now)
                if not replace_existing:
                    existing = connection.execute(
                        "SELECT 1 FROM alpha_authors WHERE screen_name = ? LIMIT 1",
                        (screen_name,),
                    ).fetchone()
                    if existing is not None:
                        continue
                connection.execute(
                    """
                    INSERT INTO alpha_authors (
                        screen_name,
                        author_name,
                        rest_id,
                        author_score,
                        reply_count_7d,
                        impressions_total_7d,
                        avg_impressions_7d,
                        max_impressions_7d,
                        last_replied_at,
                        last_post_seen_at,
                        last_scored_at,
                        source,
                        created_at,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(screen_name) DO UPDATE SET
                        author_name = excluded.author_name,
                        rest_id = excluded.rest_id,
                        author_score = excluded.author_score,
                        reply_count_7d = excluded.reply_count_7d,
                        impressions_total_7d = excluded.impressions_total_7d,
                        avg_impressions_7d = excluded.avg_impressions_7d,
                        max_impressions_7d = excluded.max_impressions_7d,
                        last_replied_at = excluded.last_replied_at,
                        last_post_seen_at = excluded.last_post_seen_at,
                        last_scored_at = excluded.last_scored_at,
                        source = excluded.source,
                        created_at = excluded.created_at,
                        updated_at = excluded.updated_at
                    """,
                    (
                        screen_name,
                        author.get("author_name"),
                        author.get("rest_id"),
                        float(author.get("author_score") or 0),
                        int(author.get("reply_count_7d") or 0),
                        int(author.get("impressions_total_7d") or 0),
                        float(author.get("avg_impressions_7d") or 0),
                        int(author.get("max_impressions_7d") or 0),
                        author.get("last_replied_at"),
                        author.get("last_post_seen_at"),
                        author.get("last_scored_at"),
                        author.get("source"),
                        created_at,
                        updated_at,
                    ),
                )
                imported_count += 1
            for row in reply_daily_metrics:
                if not isinstance(row, dict):
                    raise ValueError("author-alpha score snapshot reply_daily_metrics entries must be objects")
                metric_date = str(row.get("metric_date") or "").strip()
                reply_tweet_id = str(row.get("reply_tweet_id") or "").strip()
                sampled_at = str(row.get("sampled_at") or now)
                if not metric_date or not reply_tweet_id:
                    raise ValueError("reply_daily_metrics entry must include metric_date and reply_tweet_id")
                if not replace_existing:
                    existing = connection.execute(
                        """
                        SELECT 1 FROM alpha_reply_daily_metrics
                        WHERE metric_date = ? AND reply_tweet_id = ?
                        LIMIT 1
                        """,
                        (metric_date, reply_tweet_id),
                    ).fetchone()
                    if existing is not None:
                        continue
                connection.execute(
                    """
                    INSERT INTO alpha_reply_daily_metrics (
                        metric_date,
                        reply_tweet_id,
                        target_tweet_id,
                        target_author,
                        impressions,
                        likes,
                        replies,
                        reposts,
                        sampled_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(metric_date, reply_tweet_id) DO UPDATE SET
                        target_tweet_id = excluded.target_tweet_id,
                        target_author = excluded.target_author,
                        impressions = excluded.impressions,
                        likes = excluded.likes,
                        replies = excluded.replies,
                        reposts = excluded.reposts,
                        sampled_at = excluded.sampled_at
                    """,
                    (
                        metric_date,
                        reply_tweet_id,
                        row.get("target_tweet_id"),
                        row.get("target_author"),
                        int(row.get("impressions") or 0),
                        int(row.get("likes") or 0),
                        int(row.get("replies") or 0),
                        int(row.get("reposts") or 0),
                        sampled_at,
                    ),
                )
                imported_reply_metric_count += 1
            for row in author_daily_rollups:
                if not isinstance(row, dict):
                    raise ValueError("author-alpha score snapshot author_daily_rollups entries must be objects")
                metric_date = str(row.get("metric_date") or "").strip()
                target_author = str(row.get("target_author") or "").strip()
                computed_at = str(row.get("computed_at") or now)
                if not metric_date or not target_author:
                    raise ValueError("author_daily_rollups entry must include metric_date and target_author")
                if not replace_existing:
                    existing = connection.execute(
                        """
                        SELECT 1 FROM alpha_author_daily_rollups
                        WHERE metric_date = ? AND target_author = ?
                        LIMIT 1
                        """,
                        (metric_date, target_author),
                    ).fetchone()
                    if existing is not None:
                        continue
                connection.execute(
                    """
                    INSERT INTO alpha_author_daily_rollups (
                        metric_date,
                        target_author,
                        reply_count,
                        impressions_total,
                        likes_total,
                        replies_total,
                        reposts_total,
                        avg_impressions,
                        max_impressions,
                        computed_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(metric_date, target_author) DO UPDATE SET
                        reply_count = excluded.reply_count,
                        impressions_total = excluded.impressions_total,
                        likes_total = excluded.likes_total,
                        replies_total = excluded.replies_total,
                        reposts_total = excluded.reposts_total,
                        avg_impressions = excluded.avg_impressions,
                        max_impressions = excluded.max_impressions,
                        computed_at = excluded.computed_at
                    """,
                    (
                        metric_date,
                        target_author,
                        int(row.get("reply_count") or 0),
                        int(row.get("impressions_total") or 0),
                        int(row.get("likes_total") or 0),
                        int(row.get("replies_total") or 0),
                        int(row.get("reposts_total") or 0),
                        float(row.get("avg_impressions") or 0),
                        int(row.get("max_impressions") or 0),
                        computed_at,
                    ),
                )
                imported_rollup_count += 1
            for row in sync_runs:
                if not isinstance(row, dict):
                    raise ValueError("author-alpha score snapshot sync_runs entries must be objects")
                run_id = str(row.get("run_id") or "").strip()
                run_type = str(row.get("run_type") or "").strip()
                status = str(row.get("status") or "").strip()
                if not run_id or not run_type or not status:
                    raise ValueError("sync_runs entry must include run_id, run_type, and status")
                if not replace_existing:
                    existing = connection.execute(
                        "SELECT 1 FROM alpha_sync_runs WHERE run_id = ? LIMIT 1",
                        (run_id,),
                    ).fetchone()
                    if existing is not None:
                        continue
                connection.execute(
                    """
                    INSERT INTO alpha_sync_runs (
                        run_id, run_type, status, from_date, to_date, current_date,
                        days_completed, days_total, resume_from_date, error,
                        created_at, started_at, finished_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id) DO UPDATE SET
                        run_type = excluded.run_type,
                        status = excluded.status,
                        from_date = excluded.from_date,
                        to_date = excluded.to_date,
                        current_date = excluded.current_date,
                        days_completed = excluded.days_completed,
                        days_total = excluded.days_total,
                        resume_from_date = excluded.resume_from_date,
                        error = excluded.error,
                        created_at = excluded.created_at,
                        started_at = excluded.started_at,
                        finished_at = excluded.finished_at
                    """,
                    (
                        run_id,
                        run_type,
                        status,
                        row.get("from_date"),
                        row.get("to_date"),
                        row.get("current_date"),
                        int(row.get("days_completed") or 0),
                        int(row.get("days_total") or 0),
                        row.get("resume_from_date"),
                        row.get("error"),
                        row.get("created_at") or now,
                        row.get("started_at"),
                        row.get("finished_at"),
                    ),
                )
                imported_sync_run_count += 1
            for row in sync_checkpoints:
                if not isinstance(row, dict):
                    raise ValueError("author-alpha score snapshot sync_checkpoints entries must be objects")
                sync_scope = str(row.get("sync_scope") or "").strip()
                if not sync_scope:
                    raise ValueError("sync_checkpoints entry must include sync_scope")
                if not replace_existing:
                    existing = connection.execute(
                        "SELECT 1 FROM alpha_sync_checkpoints WHERE sync_scope = ? LIMIT 1",
                        (sync_scope,),
                    ).fetchone()
                    if existing is not None:
                        continue
                connection.execute(
                    """
                    INSERT INTO alpha_sync_checkpoints (
                        sync_scope, last_completed_date, next_pending_date, last_run_id, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(sync_scope) DO UPDATE SET
                        last_completed_date = excluded.last_completed_date,
                        next_pending_date = excluded.next_pending_date,
                        last_run_id = excluded.last_run_id,
                        updated_at = excluded.updated_at
                    """,
                    (
                        sync_scope,
                        row.get("last_completed_date"),
                        row.get("next_pending_date"),
                        row.get("last_run_id"),
                        row.get("updated_at") or now,
                    ),
                )
                imported_sync_checkpoint_count += 1
            for row in engagements:
                if not isinstance(row, dict):
                    raise ValueError("author-alpha score snapshot engagements entries must be objects")
                run_id = str(row.get("run_id") or "").strip()
                target_author = str(row.get("target_author") or "").strip()
                target_tweet_id = str(row.get("target_tweet_id") or "").strip()
                reply_tweet_id = str(row.get("reply_tweet_id") or "").strip()
                if not run_id or not target_author or not target_tweet_id or not reply_tweet_id:
                    raise ValueError("engagements entry must include run_id, target_author, target_tweet_id, and reply_tweet_id")
                if not replace_existing:
                    existing = connection.execute(
                        """
                        SELECT 1
                        FROM alpha_engagements
                        WHERE reply_tweet_id = ?
                        LIMIT 1
                        """,
                        (reply_tweet_id,),
                    ).fetchone()
                    if existing is not None:
                        continue
                connection.execute(
                    """
                    INSERT INTO alpha_engagements (
                        run_id, target_author, target_tweet_id, target_tweet_url, reply_tweet_id, reply_url,
                        burst_id, burst_index, burst_size, metric_date, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        target_author,
                        target_tweet_id,
                        row.get("target_tweet_url"),
                        reply_tweet_id,
                        row.get("reply_url"),
                        row.get("burst_id"),
                        row.get("burst_index"),
                        row.get("burst_size"),
                        row.get("metric_date") or str(row.get("created_at") or "")[:10],
                        row.get("created_at") or now,
                    ),
                )
                imported_engagement_count += 1
            for row in execution_runs:
                if not isinstance(row, dict):
                    raise ValueError("author-alpha score snapshot execution_runs entries must be objects")
                run_id = str(row.get("id") or "").strip()
                job_id = str(row.get("job_id") or "").strip()
                job_type = str(row.get("job_type") or "").strip()
                endpoint = str(row.get("endpoint") or "").strip()
                status = str(row.get("status") or "").strip()
                if not run_id or not job_id or not job_type or not endpoint or not status:
                    raise ValueError("execution_runs entry must include id, job_id, job_type, endpoint, and status")
                if not replace_existing:
                    existing = connection.execute(
                        "SELECT 1 FROM alpha_runs WHERE id = ? LIMIT 1",
                        (run_id,),
                    ).fetchone()
                    if existing is not None:
                        continue
                connection.execute(
                    """
                    INSERT INTO alpha_runs (
                        id, job_id, job_type, endpoint, status, request_json, response_json, error,
                        created_at, updated_at, started_at, finished_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        job_id = excluded.job_id,
                        job_type = excluded.job_type,
                        endpoint = excluded.endpoint,
                        status = excluded.status,
                        request_json = excluded.request_json,
                        response_json = excluded.response_json,
                        error = excluded.error,
                        created_at = excluded.created_at,
                        updated_at = excluded.updated_at,
                        started_at = excluded.started_at,
                        finished_at = excluded.finished_at
                    """,
                    (
                        run_id,
                        job_id,
                        job_type,
                        endpoint,
                        status,
                        _serialize_json(row.get("request_payload")) or "{}",
                        _serialize_json(row.get("response_payload")),
                        row.get("error"),
                        row.get("created_at") or now,
                        row.get("updated_at") or now,
                        row.get("started_at"),
                        row.get("finished_at"),
                    ),
                )
                imported_execution_run_count += 1
            for row in execution_audit_events:
                if not isinstance(row, dict):
                    raise ValueError("author-alpha score snapshot execution_audit_events entries must be objects")
                run_id = str(row.get("run_id") or "").strip()
                level = str(row.get("level") or "").strip()
                event_type = str(row.get("event_type") or "").strip()
                if not run_id or not level or not event_type:
                    raise ValueError("execution_audit_events entry must include run_id, level, and event_type")
                payload_json = _serialize_json(row.get("payload"))
                created_at = row.get("created_at") or now
                if not replace_existing:
                    existing = connection.execute(
                        """
                        SELECT 1
                        FROM alpha_run_audit_events
                        WHERE run_id = ?
                          AND level = ?
                          AND event_type = ?
                          AND ifnull(node, '') = ifnull(?, '')
                          AND ifnull(payload_json, '') = ifnull(?, '')
                          AND created_at = ?
                        LIMIT 1
                        """,
                        (run_id, level, event_type, row.get("node"), payload_json, created_at),
                    ).fetchone()
                    if existing is not None:
                        continue
                connection.execute(
                    """
                    INSERT INTO alpha_run_audit_events (
                        run_id, level, event_type, node, payload_json, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        level,
                        event_type,
                        row.get("node"),
                        payload_json,
                        created_at,
                    ),
                )
                imported_execution_audit_event_count += 1
        return {
            "status": "imported",
            "schema_version": schema_version,
            "replace_existing": bool(replace_existing),
            "imported_count": imported_count,
            "imported_reply_metric_count": imported_reply_metric_count,
            "imported_rollup_count": imported_rollup_count,
            "imported_sync_run_count": imported_sync_run_count,
            "imported_sync_checkpoint_count": imported_sync_checkpoint_count,
            "imported_engagement_count": imported_engagement_count,
            "imported_execution_run_count": imported_execution_run_count,
            "imported_execution_audit_event_count": imported_execution_audit_event_count,
            "latest_scored_at": snapshot.get("latest_scored_at"),
        }

    def list_sync_checkpoints(self) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM alpha_sync_checkpoints
                ORDER BY sync_scope ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def list_engagements(self) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM alpha_engagements
                ORDER BY created_at ASC, id ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def list_execution_runs(self) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT id, job_id, job_type, endpoint, status, request_json, response_json, error,
                       created_at, updated_at, started_at, finished_at
                FROM alpha_runs
                ORDER BY created_at ASC, id ASC
                """
            ).fetchall()
        return [
            {
                "id": str(row["id"]),
                "job_id": str(row["job_id"]),
                "job_type": str(row["job_type"]),
                "endpoint": str(row["endpoint"]),
                "status": str(row["status"]),
                "request_payload": _deserialize_json(row["request_json"]) or {},
                "response_payload": _deserialize_json(row["response_json"]),
                "error": row["error"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
            }
            for row in rows
        ]

    def list_execution_audit_events(self) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT id, run_id, level, event_type, node, payload_json, created_at
                FROM alpha_run_audit_events
                ORDER BY created_at ASC, id ASC
                """
            ).fetchall()
        return [
            {
                "id": int(row["id"]),
                "run_id": str(row["run_id"]),
                "level": str(row["level"]),
                "event_type": str(row["event_type"]),
                "node": row["node"],
                "payload": _deserialize_json(row["payload_json"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def upsert_reply_daily_metrics(
        self,
        *,
        metric_date: str,
        reply_tweet_id: str,
        target_tweet_id: str | None,
        target_author: str | None,
        impressions: int,
        likes: int,
        replies: int,
        reposts: int,
        sampled_at: str,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO alpha_reply_daily_metrics (
                    metric_date,
                    reply_tweet_id,
                    target_tweet_id,
                    target_author,
                    impressions,
                    likes,
                    replies,
                    reposts,
                    sampled_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(metric_date, reply_tweet_id) DO UPDATE SET
                    target_tweet_id = excluded.target_tweet_id,
                    target_author = excluded.target_author,
                    impressions = excluded.impressions,
                    likes = excluded.likes,
                    replies = excluded.replies,
                    reposts = excluded.reposts,
                    sampled_at = excluded.sampled_at
                """,
                (
                    metric_date,
                    reply_tweet_id,
                    target_tweet_id,
                    target_author,
                    impressions,
                    likes,
                    replies,
                    reposts,
                    sampled_at,
                ),
            )

    def upsert_author_daily_rollup(
        self,
        *,
        metric_date: str,
        target_author: str,
        reply_count: int,
        impressions_total: int,
        likes_total: int,
        replies_total: int,
        reposts_total: int,
        avg_impressions: float,
        max_impressions: int,
        computed_at: str,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO alpha_author_daily_rollups (
                    metric_date,
                    target_author,
                    reply_count,
                    impressions_total,
                    likes_total,
                    replies_total,
                    reposts_total,
                    avg_impressions,
                    max_impressions,
                    computed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(metric_date, target_author) DO UPDATE SET
                    reply_count = excluded.reply_count,
                    impressions_total = excluded.impressions_total,
                    likes_total = excluded.likes_total,
                    replies_total = excluded.replies_total,
                    reposts_total = excluded.reposts_total,
                    avg_impressions = excluded.avg_impressions,
                    max_impressions = excluded.max_impressions,
                    computed_at = excluded.computed_at
                """,
                (
                    metric_date,
                    target_author,
                    reply_count,
                    impressions_total,
                    likes_total,
                    replies_total,
                    reposts_total,
                    avg_impressions,
                    max_impressions,
                    computed_at,
                ),
            )

    def replace_day_sync_snapshot(
        self,
        *,
        metric_date: str,
        reply_metrics: list[dict[str, Any]],
        author_rollups: list[dict[str, Any]],
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                "DELETE FROM alpha_reply_daily_metrics WHERE metric_date = ?",
                (metric_date,),
            )
            connection.execute(
                "DELETE FROM alpha_author_daily_rollups WHERE metric_date = ?",
                (metric_date,),
            )
            connection.executemany(
                """
                INSERT INTO alpha_reply_daily_metrics (
                    metric_date,
                    reply_tweet_id,
                    target_tweet_id,
                    target_author,
                    impressions,
                    likes,
                    replies,
                    reposts,
                    sampled_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        metric_date,
                        str(row["reply_tweet_id"]),
                        row.get("target_tweet_id"),
                        row.get("target_author"),
                        int(row.get("impressions", 0)),
                        int(row.get("likes", 0)),
                        int(row.get("replies", 0)),
                        int(row.get("reposts", 0)),
                        str(row["sampled_at"]),
                    )
                    for row in reply_metrics
                ],
            )
            connection.executemany(
                """
                INSERT INTO alpha_author_daily_rollups (
                    metric_date,
                    target_author,
                    reply_count,
                    impressions_total,
                    likes_total,
                    replies_total,
                    reposts_total,
                    avg_impressions,
                    max_impressions,
                    computed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        metric_date,
                        str(row["target_author"]),
                        int(row.get("reply_count", 0)),
                        int(row.get("impressions_total", 0)),
                        int(row.get("likes_total", 0)),
                        int(row.get("replies_total", 0)),
                        int(row.get("reposts_total", 0)),
                        float(row.get("avg_impressions", 0.0)),
                        int(row.get("max_impressions", 0)),
                        str(row["computed_at"]),
                    )
                    for row in author_rollups
                ],
            )

    def record_sync_run(
        self,
        *,
        run_id: str,
        run_type: str,
        status: str,
        from_date: str | None,
        to_date: str | None,
        current_date: str | None,
        days_completed: int,
        days_total: int,
        resume_from_date: str | None,
        error: str | None = None,
        created_at: str | None = None,
        started_at: str | None = None,
        finished_at: str | None = None,
    ) -> None:
        recorded_at = created_at or utcnow()
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO alpha_sync_runs (
                    run_id,
                    run_type,
                    status,
                    from_date,
                    to_date,
                    current_date,
                    days_completed,
                    days_total,
                    resume_from_date,
                    error,
                    created_at,
                    started_at,
                    finished_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    run_type,
                    status,
                    from_date,
                    to_date,
                    current_date,
                    days_completed,
                    days_total,
                    resume_from_date,
                    error,
                    recorded_at,
                    started_at or recorded_at,
                    finished_at,
                ),
            )

    def update_sync_run(
        self,
        run_id: str,
        *,
        status: str | object = _UNSET,
        current_date: str | object = _UNSET,
        days_completed: int | object = _UNSET,
        days_total: int | object = _UNSET,
        resume_from_date: str | None | object = _UNSET,
        error: str | None | object = _UNSET,
        started_at: str | object = _UNSET,
        finished_at: str | object = _UNSET,
    ) -> None:
        assignments: list[str] = []
        parameters: list[Any] = []
        if status is not _UNSET:
            assignments.append("status = ?")
            parameters.append(status)
        if current_date is not _UNSET:
            assignments.append("current_date = ?")
            parameters.append(current_date)
        if days_completed is not _UNSET:
            assignments.append("days_completed = ?")
            parameters.append(days_completed)
        if days_total is not _UNSET:
            assignments.append("days_total = ?")
            parameters.append(days_total)
        if resume_from_date is not _UNSET:
            assignments.append("resume_from_date = ?")
            parameters.append(resume_from_date)
        if error is not _UNSET:
            assignments.append("error = ?")
            parameters.append(error)
        if started_at is not _UNSET:
            assignments.append("started_at = ?")
            parameters.append(started_at)
        if finished_at is not _UNSET:
            assignments.append("finished_at = ?")
            parameters.append(finished_at)
        if not assignments:
            return
        parameters.append(run_id)
        with self.connect() as connection:
            connection.execute(
                f"UPDATE alpha_sync_runs SET {', '.join(assignments)} WHERE run_id = ?",
                parameters,
            )

    def create_execution_run(
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
                INSERT INTO alpha_runs (
                    id,
                    job_id,
                    job_type,
                    endpoint,
                    status,
                    request_json,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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

    def update_execution_run(
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
                f"UPDATE alpha_runs SET {', '.join(assignments)} WHERE id = ?",
                parameters,
            )

    def add_execution_audit_event(
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
                INSERT INTO alpha_run_audit_events (run_id, level, event_type, node, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (run_id, level, event_type, node, _serialize_json(payload), utcnow()),
            )
            return int(cursor.lastrowid)

    def read_checkpoint(self, sync_scope: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM alpha_sync_checkpoints
                WHERE sync_scope = ?
                """,
                (sync_scope,),
            ).fetchone()
        return self._row_to_dict(row)

    def write_checkpoint(
        self,
        *,
        sync_scope: str,
        last_completed_date: str | None,
        next_pending_date: str | None,
        last_run_id: str | None,
        updated_at: str | None = None,
    ) -> None:
        checkpoint_time = updated_at or utcnow()
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO alpha_sync_checkpoints (
                    sync_scope,
                    last_completed_date,
                    next_pending_date,
                    last_run_id,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(sync_scope) DO UPDATE SET
                    last_completed_date = excluded.last_completed_date,
                    next_pending_date = excluded.next_pending_date,
                    last_run_id = excluded.last_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    sync_scope,
                    last_completed_date,
                    next_pending_date,
                    last_run_id,
                    checkpoint_time,
                ),
            )

    def record_engagement(
        self,
        *,
        run_id: str,
        target_author: str,
        target_tweet_id: str,
        target_tweet_url: str | None,
        reply_tweet_id: str,
        reply_url: str | None,
        burst_id: str | None = None,
        burst_index: int | None = None,
        burst_size: int | None = None,
        metric_date: str | None = None,
        created_at: str | None = None,
    ) -> None:
        normalized_target_author = target_author.strip()
        normalized_target_tweet_id = target_tweet_id.strip()
        normalized_reply_tweet_id = reply_tweet_id.strip()
        if not normalized_target_author or not normalized_target_tweet_id or not normalized_reply_tweet_id:
            raise ValueError("target_author, target_tweet_id, and reply_tweet_id are required")
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO alpha_engagements (
                    run_id,
                    target_author,
                    target_tweet_id,
                    target_tweet_url,
                    reply_tweet_id,
                    reply_url,
                    burst_id,
                    burst_index,
                    burst_size,
                    metric_date,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    normalized_target_author,
                    normalized_target_tweet_id,
                    target_tweet_url,
                    normalized_reply_tweet_id,
                    reply_url,
                    burst_id.strip() if isinstance(burst_id, str) and burst_id.strip() else None,
                    burst_index,
                    burst_size,
                    metric_date or _parse_timestamp(created_at or utcnow()).date().isoformat(),
                    _normalize_timestamp(created_at or utcnow()),
                ),
            )

    def update_burst_size(self, *, burst_id: str, burst_size: int) -> None:
        normalized_burst_id = burst_id.strip()
        if not normalized_burst_id:
            raise ValueError("burst_id is required")
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE alpha_engagements
                SET burst_size = ?
                WHERE burst_id = ?
                """,
                (burst_size, normalized_burst_id),
            )

    def get_target_success_count(self, target_tweet_id: str) -> int:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM alpha_engagements
                WHERE target_tweet_id = ?
                """,
                (target_tweet_id,),
            ).fetchone()
        return int(row["count"]) if row else 0

    def get_target_last_success_at(self, target_tweet_id: str) -> str | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT created_at
                FROM alpha_engagements
                WHERE target_tweet_id = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (target_tweet_id,),
            ).fetchone()
        if row is None:
            return None
        value = row["created_at"]
        return str(value) if value is not None else None

    def get_author_daily_success_count(self, target_author: str, *, metric_date: str) -> int:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM alpha_engagements
                WHERE target_author = ? AND metric_date = ?
                """,
                (target_author, metric_date),
            ).fetchone()
        return int(row["count"]) if row else 0

    def get_daily_success_count(self, *, metric_date: str) -> int:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM alpha_engagements
                WHERE metric_date = ?
                """,
                (metric_date,),
            ).fetchone()
        return int(row["count"]) if row else 0

    def get_recent_success_count_15m(self, as_of: str | None = None) -> int:
        anchor = _parse_timestamp(as_of or utcnow())
        cutoff = anchor - timedelta(minutes=15)
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM alpha_engagements
                WHERE unixepoch(created_at) >= unixepoch(?) AND unixepoch(created_at) <= unixepoch(?)
                """,
                (_normalize_timestamp(cutoff.isoformat()), _normalize_timestamp(anchor.isoformat())),
            ).fetchone()
        return int(row["count"]) if row else 0

    def zero_out_stale_authors(self, scored_screen_names: set[str], *, scored_at: str) -> int:
        parameters: list[Any] = [0.0, 0, 0, 0.0, 0, scored_at, utcnow()]
        where_clause = ""
        if scored_screen_names:
            placeholders = ", ".join("?" for _ in scored_screen_names)
            where_clause = f"WHERE screen_name NOT IN ({placeholders})"
            parameters.extend(sorted(scored_screen_names))
        with self.connect() as connection:
            cursor = connection.execute(
                f"""
                UPDATE alpha_authors
                SET
                    author_score = ?,
                    reply_count_7d = ?,
                    impressions_total_7d = ?,
                    avg_impressions_7d = ?,
                    max_impressions_7d = ?,
                    last_scored_at = ?,
                    updated_at = ?
                {where_clause}
                """,
                parameters,
            )
        return int(cursor.rowcount or 0)

    def get_reply_daily_metric(
        self, metric_date: str, reply_tweet_id: str
    ) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM alpha_reply_daily_metrics
                WHERE metric_date = ? AND reply_tweet_id = ?
                """,
                (metric_date, reply_tweet_id),
            ).fetchone()
        return self._row_to_dict(row)

    def get_author_daily_rollup(
        self, metric_date: str, target_author: str
    ) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM alpha_author_daily_rollups
                WHERE metric_date = ? AND target_author = ?
                """,
                (metric_date, target_author),
            ).fetchone()
        return self._row_to_dict(row)

    def list_author_daily_rollups(self, start_date: str, end_date: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM alpha_author_daily_rollups
                WHERE metric_date >= ? AND metric_date <= ?
                ORDER BY metric_date ASC, target_author ASC
                """,
                (start_date, end_date),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_reply_daily_metrics(self, start_date: str, end_date: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM alpha_reply_daily_metrics
                WHERE metric_date >= ? AND metric_date <= ?
                ORDER BY metric_date ASC, reply_tweet_id ASC
                """,
                (start_date, end_date),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_sync_run(self, run_id: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM alpha_sync_runs
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
        return self._row_to_dict(row)

    def get_execution_run(self, run_id: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            run_row = connection.execute(
                """
                SELECT
                    id,
                    job_id,
                    job_type,
                    endpoint,
                    status,
                    request_json,
                    response_json,
                    error,
                    created_at,
                    updated_at,
                    started_at,
                    finished_at
                FROM alpha_runs
                WHERE id = ?
                """,
                (run_id,),
            ).fetchone()
            if run_row is None:
                return None
            audit_rows = connection.execute(
                """
                SELECT id, run_id, level, event_type, node, payload_json, created_at
                FROM alpha_run_audit_events
                WHERE run_id = ?
                ORDER BY created_at ASC, id ASC
                """,
                (run_id,),
            ).fetchall()
        run = {
            "id": str(run_row["id"]),
            "job_id": str(run_row["job_id"]),
            "job_type": str(run_row["job_type"]),
            "endpoint": str(run_row["endpoint"]),
            "status": str(run_row["status"]),
            "request_payload": _deserialize_json(run_row["request_json"]) or {},
            "response_payload": _deserialize_json(run_row["response_json"]),
            "error": run_row["error"],
            "created_at": run_row["created_at"],
            "updated_at": run_row["updated_at"],
            "started_at": run_row["started_at"],
            "finished_at": run_row["finished_at"],
        }
        audit_events = [
            {
                "id": int(row["id"]),
                "run_id": str(row["run_id"]),
                "level": str(row["level"]),
                "event_type": str(row["event_type"]),
                "node": row["node"],
                "payload": _deserialize_json(row["payload_json"]),
                "created_at": row["created_at"],
            }
            for row in audit_rows
        ]
        return {
            "run": run,
            "audit_events": audit_events,
        }

    def list_sync_runs(self, *, limit: int = 20) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM alpha_sync_runs
                ORDER BY created_at DESC, run_id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_active_sync_run(self) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM alpha_sync_runs
                WHERE status = 'running'
                ORDER BY created_at DESC, run_id DESC
                LIMIT 1
                """
            ).fetchone()
        return self._row_to_dict(row)

    def count_reply_daily_metrics(self) -> int:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM alpha_reply_daily_metrics"
            ).fetchone()
        return int(row["count"]) if row else 0

    def count_author_daily_rollups(self) -> int:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM alpha_author_daily_rollups"
            ).fetchone()
        return int(row["count"]) if row else 0

    def reset_all(self) -> None:
        with self.connect() as connection:
            connection.execute("DELETE FROM alpha_run_audit_events")
            connection.execute("DELETE FROM alpha_runs")
            connection.execute("DELETE FROM alpha_engagements")
            connection.execute("DELETE FROM alpha_sync_checkpoints")
            connection.execute("DELETE FROM alpha_sync_runs")
            connection.execute("DELETE FROM alpha_author_daily_rollups")
            connection.execute("DELETE FROM alpha_reply_daily_metrics")
            connection.execute("DELETE FROM alpha_authors")


def _parse_timestamp(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _normalize_timestamp(value: str) -> str:
    return _parse_timestamp(value).isoformat()


def _serialize_json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _deserialize_json(value: str | None) -> Any:
    if value is None:
        return None
    return json.loads(value)
