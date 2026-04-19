"""Low-level twitter-cli wrapper for deterministic service use."""

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from x_atuo.core.twitter_models import PostResult, TweetRecord, TwitterCommandResult


class TwitterClientError(RuntimeError):
    """Raised when twitter-cli fails or returns malformed payloads."""


def _format_cli_error(cmd: Sequence[str], stdout: str, stderr: str) -> str:
    return (
        f"twitter-cli failed: {' '.join(cmd)}\n"
        f"stdout:\n{stdout}\n"
        f"stderr:\n{stderr}"
    )


@dataclass(slots=True, frozen=True)
class TwitterCredentials:
    auth_token: str = ""
    ct0: str = ""

    @property
    def ok(self) -> bool:
        return bool(self.auth_token and self.ct0)


@dataclass(slots=True)
class _CommandExecution:
    command: tuple[str, ...]
    returncode: int
    payload: dict[str, Any]
    stdout: str
    stderr: str


@dataclass(slots=True)
class TwitterClient:
    twitter_bin: str = "twitter"
    credentials: TwitterCredentials = field(default_factory=TwitterCredentials)
    proxy: str | None = None
    timeout: int = 120
    base_env: dict[str, str] = field(default_factory=lambda: os.environ.copy())
    config: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_config(
        cls,
        config_path: str | Path,
        *,
        proxy: str | None = None,
        twitter_bin: str = "twitter",
        timeout: int = 120,
        base_env: Mapping[str, str] | None = None,
    ) -> "TwitterClient":
        env = dict(base_env) if base_env is not None else os.environ.copy()
        config_file = Path(config_path)
        data: dict[str, Any] = {}
        if config_file.exists():
            loaded = yaml.safe_load(config_file.read_text(encoding="utf-8")) or {}
            if not isinstance(loaded, dict):
                raise ValueError(f"Unexpected config shape in {config_file}")
            data = loaded
        return cls(
            twitter_bin=twitter_bin,
            credentials=TwitterCredentials(
                auth_token=str(data.get("twitter_auth_token") or env.get("TWITTER_AUTH_TOKEN") or ""),
                ct0=str(data.get("twitter_ct0") or env.get("TWITTER_CT0") or ""),
            ),
            proxy=proxy or env.get("HTTPS_PROXY") or env.get("HTTP_PROXY"),
            timeout=timeout,
            base_env=env,
            config=data,
        )

    def with_runtime(
        self,
        *,
        base_env: Mapping[str, str] | None = None,
        proxy: str | None = None,
        config: Mapping[str, Any] | None = None,
        auth_token: str | None = None,
        ct0: str | None = None,
    ) -> "TwitterClient":
        merged_env = dict(base_env) if base_env is not None else dict(self.base_env)
        merged_config = dict(self.config)
        if config:
            merged_config.update(config)
        return TwitterClient(
            twitter_bin=self.twitter_bin,
            credentials=TwitterCredentials(
                auth_token=auth_token
                or self.credentials.auth_token
                or str(merged_config.get("twitter_auth_token") or merged_env.get("TWITTER_AUTH_TOKEN") or ""),
                ct0=ct0
                or self.credentials.ct0
                or str(merged_config.get("twitter_ct0") or merged_env.get("TWITTER_CT0") or ""),
            ),
            proxy=proxy if proxy is not None else self.proxy,
            timeout=self.timeout,
            base_env=merged_env,
            config=merged_config,
        )

    def build_env(self) -> dict[str, str]:
        env = dict(self.base_env)
        env["TWITTER_AUTH_TOKEN"] = self.credentials.auth_token
        env["TWITTER_CT0"] = self.credentials.ct0
        if self.proxy:
            env["HTTP_PROXY"] = self.proxy
            env["HTTPS_PROXY"] = self.proxy
        return env

    def fetch_feed(self, *, max_items: int = 5, feed_type: str | None = None) -> list[TweetRecord]:
        if max_items < 1:
            raise ValueError("max_items must be >= 1")
        args = ["feed", "-n", str(max_items)]
        if feed_type:
            args.extend(["-t", feed_type])
        payload = self._run_json(args).payload
        data = payload.get("data")
        if not isinstance(data, list) or not data:
            raise TwitterClientError("No feed data returned")
        items = [TweetRecord.from_payload(item) for item in data if isinstance(item, dict)]
        items = [item for item in items if item.tweet_id and item.screen_name]
        if not items:
            raise TwitterClientError("No valid feed items returned")
        return items

    def fetch_tweet(self, tweet_id: str) -> TweetRecord:
        payload = self._run_json(["tweet", tweet_id]).payload
        tweets = self._parse_tweets(payload)
        if not tweets:
            raise TwitterClientError(f"No tweet data returned for {tweet_id}")
        tweet = tweets[0]
        if not tweet.tweet_id:
            raise TwitterClientError(f"Invalid tweet payload returned for {tweet_id}")
        return tweet

    def fetch_tweet_thread(self, tweet_id: str, *, max_replies: int = 5) -> tuple[TweetRecord, list[TweetRecord]]:
        if max_replies < 0:
            raise ValueError("max_replies must be >= 0")
        payload = self._run_json(["tweet", tweet_id, "-n", str(max_replies)]).payload
        tweets = self._parse_tweets(payload)
        if not tweets:
            raise TwitterClientError(f"No tweet thread returned for {tweet_id}")
        return tweets[0], tweets[1:]

    def fetch_user_profile(self, screen_name: str) -> dict[str, object]:
        payload = self._run_json(["user", screen_name]).payload
        data = payload.get("data")
        if isinstance(data, list):
            record = next((item for item in data if isinstance(item, dict)), None)
        elif isinstance(data, dict):
            record = data
        else:
            record = None
        if record is None:
            raise TwitterClientError(f"No user profile returned for {screen_name}")
        raw = dict(record)
        return {
            "screen_name": str(raw.get("screenName") or raw.get("screen_name") or screen_name),
            "name": str(raw.get("name")) if raw.get("name") else None,
            "verified": bool(raw.get("verified")),
            "description": str(raw.get("description") or raw.get("bio") or "") or None,
            "followers_count": raw.get("followersCount") or raw.get("followers_count"),
            "following_count": raw.get("followingCount") or raw.get("following_count"),
            "raw": raw,
        }

    def fetch_user_posts(self, screen_name: str, *, max_items: int = 5) -> list[TweetRecord]:
        if max_items < 1:
            raise ValueError("max_items must be >= 1")
        payload = self._run_json(["user-posts", screen_name, "-n", str(max_items), "--full-text"]).payload
        tweets = self._parse_tweets(payload)
        if not tweets:
            raise TwitterClientError(f"No user posts returned for {screen_name}")
        return tweets

    def reply(self, tweet_id: str, text: str) -> TwitterCommandResult:
        execution = self._run_json(["reply", tweet_id, text], allow_error_payload=True)
        payload = execution.payload
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        return TwitterCommandResult(
            action="reply",
            ok=bool(data.get("success")),
            target_tweet_id=tweet_id,
            tweet_id=self._extract_tweet_id(data),
            text=text,
            payload=payload,
            error_code=self._extract_error_code(payload),
            error_message=self._extract_error_message(payload),
        )

    def follow(self, screen_name: str) -> TwitterCommandResult:
        execution = self._run_json(["follow", screen_name], allow_error_payload=True)
        payload = execution.payload
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        return TwitterCommandResult(
            action="follow",
            ok=bool(data.get("success", execution.returncode == 0)),
            screen_name=screen_name,
            payload=payload,
            error_code=self._extract_error_code(payload),
            error_message=self._extract_error_message(payload),
        )

    def post(
        self,
        text: str,
        *,
        reply_to: str | None = None,
        images: Sequence[str] | None = None,
    ) -> PostResult:
        args = ["post", text]
        media = tuple(str(image) for image in (images or ()))
        if reply_to:
            args.extend(["-r", reply_to])
        for image in media:
            args.extend(["-i", image])
        execution = self._run_json(args, allow_error_payload=True)
        payload = execution.payload
        return PostResult(
            ok=execution.returncode == 0 and not self._extract_error_message(payload),
            action="post",
            text=text,
            tweet_id=self._extract_tweet_id(payload.get("data")),
            target_tweet_id=reply_to,
            media_paths=media,
            payload=payload,
            error_code=self._extract_error_code(payload),
            error_message=self._extract_error_message(payload),
        )

    def quote(
        self,
        tweet_id: str,
        text: str,
        *,
        images: Sequence[str] | None = None,
    ) -> PostResult:
        args = ["quote", tweet_id, text]
        media = tuple(str(image) for image in (images or ()))
        for image in media:
            args.extend(["-i", image])
        execution = self._run_json(args, allow_error_payload=True)
        payload = execution.payload
        return PostResult(
            ok=execution.returncode == 0 and not self._extract_error_message(payload),
            action="quote",
            text=text,
            tweet_id=self._extract_tweet_id(payload.get("data")),
            target_tweet_id=tweet_id,
            media_paths=media,
            payload=payload,
            error_code=self._extract_error_code(payload),
            error_message=self._extract_error_message(payload),
        )

    def _parse_tweets(self, payload: dict[str, object]) -> list[TweetRecord]:
        data = payload.get("data")
        if not isinstance(data, list):
            return []
        tweets = [TweetRecord.from_payload(item) for item in data if isinstance(item, dict)]
        return [tweet for tweet in tweets if tweet.tweet_id]

    def _run_json(
        self,
        args: Sequence[str],
        *,
        allow_error_payload: bool = False,
    ) -> _CommandExecution:
        cmd = [self.twitter_bin, *args, "--json"]
        try:
            result = subprocess.run(
                cmd,
                env=self.build_env(),
                capture_output=True,
                text=True,
                timeout=self.timeout,
                check=False,
            )
        except OSError as exc:
            raise TwitterClientError(f"Failed to execute {' '.join(cmd)}: {exc}") from exc
        except subprocess.TimeoutExpired as exc:
            raise TwitterClientError(f"twitter-cli timed out: {' '.join(cmd)}") from exc

        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise TwitterClientError(_format_cli_error(cmd, result.stdout, result.stderr)) from exc
        if not isinstance(payload, dict):
            raise TwitterClientError(f"Unexpected twitter-cli payload type: {type(payload)!r}")
        execution = _CommandExecution(
            command=tuple(cmd),
            returncode=result.returncode,
            payload=payload,
            stdout=result.stdout,
            stderr=result.stderr,
        )
        if execution.returncode != 0 and not allow_error_payload:
            raise TwitterClientError(_format_cli_error(cmd, result.stdout, result.stderr))
        return execution

    @staticmethod
    def _extract_error_code(payload: dict[str, Any]) -> str | None:
        error = payload.get("error")
        if not isinstance(error, dict):
            return None
        code = error.get("code")
        return str(code) if code not in (None, "") else None

    @staticmethod
    def _extract_error_message(payload: dict[str, Any]) -> str | None:
        error = payload.get("error")
        if not isinstance(error, dict):
            return None
        message = error.get("message")
        return str(message) if message not in (None, "") else None

    @staticmethod
    def _extract_tweet_id(data: Any) -> str | None:
        candidates: list[Any] = []
        if isinstance(data, dict):
            candidates.extend(
                [
                    data.get("tweet_id"),
                    data.get("tweetId"),
                    data.get("id"),
                    data.get("rest_id"),
                ]
            )
            nested = data.get("tweet")
            if isinstance(nested, dict):
                candidates.extend(
                    [
                        nested.get("tweet_id"),
                        nested.get("tweetId"),
                        nested.get("id"),
                        nested.get("rest_id"),
                    ]
                )
        for candidate in candidates:
            if candidate not in (None, ""):
                return str(candidate)
        return None
