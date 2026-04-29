"""Shared runtime loading for Twitter/X auth cookies and proxy settings."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml


@dataclass(slots=True, frozen=True)
class TwitterRuntime:
    auth_token: str
    ct0: str
    proxy: str | None = None
    cookie_string: str | None = None
    source_config: dict[str, Any] | None = None

    @property
    def ok(self) -> bool:
        return bool(self.auth_token and self.ct0)


def load_twitter_runtime(
    config_path: str | Path,
    *,
    proxy: str | None = None,
    base_env: Mapping[str, str] | None = None,
) -> TwitterRuntime:
    env = dict(base_env) if base_env is not None else dict(os.environ)
    config_file = Path(config_path)
    data: dict[str, Any] = {}
    if config_file.exists():
        loaded = yaml.safe_load(config_file.read_text(encoding="utf-8")) or {}
        if not isinstance(loaded, dict):
            raise ValueError(f"Unexpected config shape in {config_file}")
        data = loaded

    auth_token = str(data.get("twitter_auth_token") or env.get("TWITTER_AUTH_TOKEN") or "")
    ct0 = str(data.get("twitter_ct0") or env.get("TWITTER_CT0") or "")
    cookie_string = data.get("twitter_cookie_string") or env.get("TWITTER_COOKIE_STRING")
    cookie_string_str = str(cookie_string).strip() if isinstance(cookie_string, str) else None
    return TwitterRuntime(
        auth_token=auth_token,
        ct0=ct0,
        proxy=proxy or env.get("TWITTER_PROXY") or env.get("HTTPS_PROXY") or env.get("HTTP_PROXY"),
        cookie_string=cookie_string_str or None,
        source_config=data or None,
    )
