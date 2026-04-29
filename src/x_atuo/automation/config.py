"""Configuration models for the automation orchestration layer."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict


class TwitterRuntimeConfig(BaseModel):
    """Settings used by Twitter-facing automation runners."""

    cli_bin: str = "twitter"
    proxy_url: str | None = "http://127.0.0.1:7890"
    auth_token_env: str = "TWITTER_AUTH_TOKEN"
    ct0_env: str = "TWITTER_CT0"
    default_feed_type: Literal["following", "for-you"] = "for-you"
    default_feed_count: int = 10


class AISettings(BaseModel):
    """Config for optional AI-backed moderation and drafting."""

    provider: Literal["none", "mock", "openai_compatible"] = "none"
    model: str | None = None
    api_key: str | None = None
    base_url: str = "https://api.openai.com/v1"
    timeout_seconds: int = 30


class PolicyConfig(BaseModel):
    """Automation safeguards."""

    max_post_length: int = 280
    max_reply_length: int = 280
    candidate_refresh_rounds: int = 2
    candidate_cache_pending_ttl_minutes: int = 60
    candidate_cache_rejected_ttl_minutes: int = 1440
    candidate_cache_claim_ttl_minutes: int = 10
    enforce_dedupe: bool = True
    daily_execution_limit: int | None = None
    per_author_cooldown_minutes: int | None = None


class SchedulerSettings(BaseModel):
    """Scheduler feature flags and defaults."""

    enabled: bool = False
    autostart: bool = False
    timezone: str = "UTC"
    coalesce: bool = True
    misfire_grace_time: int = 300
    feed_engage_enabled: bool = False
    feed_engage_trigger: Literal["interval", "cron"] = "interval"
    feed_engage_seconds: int = 3600
    feed_engage_jitter_seconds: int = 600
    feed_engage_minute: str | None = None
    feed_engage_hour: str | None = None
    feed_engage_day: str | None = None
    feed_engage_day_of_week: str | None = None


class AuthorAlphaSettings(BaseModel):
    """Independent settings for the author-alpha workflow lane."""

    enabled: bool = False
    autostart: bool = False
    db_path: str = "data/author_alpha.sqlite3"
    timezone: str = "Asia/Shanghai"
    excluded_authors: list[str] = Field(default_factory=list)
    trigger: Literal["interval", "cron"] = "interval"
    seconds: int = 3600
    jitter_seconds: int = 300
    minute: str | None = None
    hour: str | None = None
    score_lookback_days: int = 7
    score_min_daily_replies: int = 400
    score_prior_weight: float = 7.0
    score_penalty_constant: float = 200.0
    device_follow_feed_count: int = 50
    daily_execution_limit: int = 700
    global_send_limit_15m: int = 50
    per_author_daily_success_limit: int = 100
    per_target_tweet_success_limit: int = 4
    target_revisit_cooldown_seconds: int = 3600
    max_targets_per_run: int = 5
    per_run_same_target_burst_limit: int = 2
    posts_per_author: int = 1
    author_cooldown_seconds: int = 3
    inter_reply_delay_seconds: int = 5
    target_switch_delay_seconds: int = 10


class JobConfig(BaseModel):
    """Persisted job defaults for webhook- or schedule-triggered workflows."""

    name: str
    workflow: str
    enabled: bool = True
    schedule: str | None = None
    timezone: str | None = None
    dry_run: bool | None = None
    approval_mode: str = "ai_auto"
    payload_overrides: dict[str, Any] = Field(default_factory=dict)


class AutomationConfig(BaseSettings):
    """App-level settings for the automation service."""

    model_config = SettingsConfigDict(
        env_prefix="X_ATUO_",
        env_nested_delimiter="__",
        env_file=".env",
        extra="ignore",
    )

    environment: str = "development"
    data_dir: Path = Path(".x_atuo")
    webhook_secret: str | None = None
    twitter: TwitterRuntimeConfig = Field(default_factory=TwitterRuntimeConfig)
    ai: AISettings = Field(default_factory=AISettings)
    policies: PolicyConfig = Field(default_factory=PolicyConfig)
    scheduler: SchedulerSettings = Field(default_factory=SchedulerSettings)
    author_alpha: AuthorAlphaSettings = Field(default_factory=AuthorAlphaSettings)
    jobs: dict[str, JobConfig] = Field(default_factory=dict)

    @property
    def agent_reach_config_path(self) -> Path:
        return Path.home() / ".agent-reach" / "config.yaml"

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # Local project .env should override ambient shell environment.
        return (
            init_settings,
            dotenv_settings,
            env_settings,
            file_secret_settings,
        )

    @classmethod
    def from_yaml(cls, path: str | Path | None, **overrides: Any) -> "AutomationConfig":
        """Load config from a YAML file and merge explicit overrides."""

        payload: dict[str, Any] = {}
        if path:
            with Path(path).expanduser().open("r", encoding="utf-8") as handle:
                payload = yaml.safe_load(handle) or {}
        payload.update(overrides)
        return cls.model_validate(payload)

    def resolve_job(self, job_name: str | None) -> JobConfig | None:
        """Return a configured job definition by name, if present."""

        if not job_name:
            return None
        return self.jobs.get(job_name)

    def apply_job_defaults(self, request_payload: dict[str, Any], *, job_name: str | None) -> dict[str, Any]:
        """Overlay configured job defaults onto an incoming request payload."""

        merged = dict(request_payload)
        job = self.resolve_job(job_name)
        if job is None:
            return merged
        if job.dry_run is not None and "dry_run" not in merged:
            merged["dry_run"] = job.dry_run
        if "approval_mode" not in merged and job.approval_mode:
            merged["approval_mode"] = job.approval_mode
        for key, value in job.payload_overrides.items():
            merged.setdefault(key, value)
        return merged
