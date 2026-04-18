"""Higher-level Twitter engagement service built on top of TwitterClient."""

from __future__ import annotations

from collections.abc import Sequence

from x_atuo.core.twitter_client import TwitterClient, TwitterClientError
from x_atuo.core.twitter_models import (
    Candidate,
    CandidateAttempt,
    EngageResult,
    PostResult,
    TwitterCommandResult,
)


def is_reply_restricted(result: TwitterCommandResult) -> bool:
    message = (result.error_message or "").lower()
    return "restricted who can reply" in message or "(433)" in message


class TwitterEngageService:
    """Deterministic service for feed-driven and explicit engagement flows."""

    def __init__(self, client: TwitterClient):
        self.client = client

    def engage_from_feed(
        self,
        *,
        reply_text: str,
        feed_count: int = 5,
        feed_type: str | None = None,
        dry_run: bool = False,
    ) -> EngageResult:
        tweets = self.client.fetch_feed(max_items=feed_count, feed_type=feed_type)
        candidates = [
            Candidate(
                tweet_id=tweet.tweet_id,
                screen_name=tweet.author.screen_name,
                reply_text=reply_text,
            )
            for tweet in tweets
            if tweet.tweet_id and tweet.author.screen_name
        ]
        if not candidates:
            return EngageResult(
                ok=False,
                status="failed",
                error="No valid feed candidates returned",
                feed_items=tuple(tweets),
            )
        result = self.engage_candidates(candidates, dry_run=dry_run)
        return EngageResult(
            ok=result.ok,
            status=result.status,
            attempts=result.attempts,
            selected_candidate=result.selected_candidate,
            reply_result=result.reply_result,
            follow_result=result.follow_result,
            feed_items=tuple(tweets),
            error=result.error,
        )

    def engage_candidates(
        self,
        candidates: Sequence[Candidate],
        *,
        dry_run: bool = False,
    ) -> EngageResult:
        attempts: list[CandidateAttempt] = []
        if not self.client.credentials.ok:
            return EngageResult(ok=False, status="failed", error="Missing Twitter credentials")

        for candidate in candidates:
            try:
                main_tweet = self.client.fetch_tweet(candidate.tweet_id)
            except TwitterClientError as exc:
                attempts.append(
                    CandidateAttempt(
                        candidate=candidate,
                        tweet_id=candidate.tweet_id,
                        screen_name=candidate.screen_name,
                        outcome="tweet_fetch_failed",
                        detail=str(exc),
                    )
                )
                return EngageResult(
                    ok=False,
                    status="failed",
                    attempts=tuple(attempts),
                    selected_candidate=candidate,
                    error=str(exc),
                )

            actual_screen_name = main_tweet.author.screen_name or candidate.screen_name
            resolved_candidate = Candidate(
                tweet_id=candidate.tweet_id,
                screen_name=actual_screen_name,
                reply_text=candidate.reply_text,
            )

            if not main_tweet.author.verified:
                attempts.append(
                    CandidateAttempt(
                        candidate=resolved_candidate,
                        tweet_id=candidate.tweet_id,
                        screen_name=actual_screen_name,
                        tweet=main_tweet,
                        outcome="author_not_verified",
                    )
                )
                continue

            if dry_run:
                reply_result = TwitterCommandResult(
                    action="reply",
                    ok=True,
                    dry_run=True,
                    target_tweet_id=candidate.tweet_id,
                    screen_name=actual_screen_name,
                    text=candidate.reply_text,
                )
                follow_result = TwitterCommandResult(
                    action="follow",
                    ok=True,
                    dry_run=True,
                    screen_name=actual_screen_name,
                )
                attempts.append(
                    CandidateAttempt(
                        candidate=resolved_candidate,
                        tweet_id=candidate.tweet_id,
                        screen_name=actual_screen_name,
                        tweet=main_tweet,
                        outcome="would_reply",
                        reply_result=reply_result,
                        follow_result=follow_result,
                    )
                )
                return EngageResult(
                    ok=True,
                    status="dry_run",
                    attempts=tuple(attempts),
                    selected_candidate=resolved_candidate,
                    reply_result=reply_result,
                    follow_result=follow_result,
                )

            reply_result = self.client.reply(candidate.tweet_id, candidate.reply_text)
            if not reply_result.ok and is_reply_restricted(reply_result):
                attempts.append(
                    CandidateAttempt(
                        candidate=resolved_candidate,
                        tweet_id=candidate.tweet_id,
                        screen_name=actual_screen_name,
                        tweet=main_tweet,
                        outcome="reply_restricted",
                        reply_result=reply_result,
                    )
                )
                continue

            if not reply_result.ok:
                attempts.append(
                    CandidateAttempt(
                        candidate=resolved_candidate,
                        tweet_id=candidate.tweet_id,
                        screen_name=actual_screen_name,
                        tweet=main_tweet,
                        outcome="reply_failed",
                        detail=reply_result.error_message or str(reply_result.payload),
                        reply_result=reply_result,
                    )
                )
                return EngageResult(
                    ok=False,
                    status="failed",
                    attempts=tuple(attempts),
                    selected_candidate=resolved_candidate,
                    reply_result=reply_result,
                    error=reply_result.error_message or str(reply_result.payload),
                )

            follow_result = self.client.follow(actual_screen_name)
            attempts.append(
                CandidateAttempt(
                    candidate=resolved_candidate,
                    tweet_id=candidate.tweet_id,
                    screen_name=actual_screen_name,
                    tweet=main_tweet,
                    outcome="replied" if follow_result.ok else "follow_failed",
                    detail=follow_result.error_message,
                    reply_result=reply_result,
                    follow_result=follow_result,
                )
            )
            return EngageResult(
                ok=True,
                status="executed",
                attempts=tuple(attempts),
                selected_candidate=resolved_candidate,
                reply_result=reply_result,
                follow_result=follow_result,
                error=follow_result.error_message if not follow_result.ok else None,
            )

        return EngageResult(
            ok=False,
            status="skipped",
            attempts=tuple(attempts),
            error="No candidate succeeded",
        )

    def post_tweet(
        self,
        *,
        text: str,
        reply_to: str | None = None,
        images: list[str] | None = None,
        dry_run: bool = False,
    ) -> PostResult:
        if dry_run:
            return PostResult(
                ok=True,
                action="post",
                text=text,
                dry_run=True,
                target_tweet_id=reply_to,
                media_paths=tuple(images or ()),
                payload={"text": text, "reply_to": reply_to, "images": images or []},
            )
        return self.client.post(text, reply_to=reply_to, images=images)

    def quote_tweet(
        self,
        *,
        tweet_id: str,
        text: str,
        images: list[str] | None = None,
        dry_run: bool = False,
    ) -> PostResult:
        if dry_run:
            return PostResult(
                ok=True,
                action="quote",
                text=text,
                dry_run=True,
                target_tweet_id=tweet_id,
                media_paths=tuple(images or ()),
                payload={"tweet_id": tweet_id, "text": text, "images": images or []},
            )
        return self.client.quote(tweet_id, text, images=images)
