# app/scraper.py
from __future__ import annotations

import asyncio
import random
import re
from datetime import datetime
from typing import List, Optional, Dict, Any

import asyncpraw
from asyncpraw.exceptions import RedditAPIException
from asyncprawcore import exceptions as prawcore_exc

from .config import (
    REDDIT_CLIENT_ID,
    REDDIT_CLIENT_SECRET,
    REDDIT_USER_AGENT,
    RATELIMIT_SECONDS,
    MAX_RETRIES,
    MAX_BACKOFF_SECONDS,
)


def _parse_wait_seconds_from_msg(msg: str) -> Optional[int]:
    """
    Best-effort parse of messages like:
      "You're doing that too much. Try again in 3 minutes."
      "try again in 57 seconds"
    """
    m = re.search(r"(\d+)\s*(second|seconds|minute|minutes|hour|hours)", msg, re.I)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2).lower()
    if unit.startswith("second"):
        return n
    if unit.startswith("minute"):
        return n * 60
    if unit.startswith("hour"):
        return n * 3600
    return None


async def _sleep_with_status(state_proxy, seconds: float, reason: str):
    secs = max(0.0, min(seconds, MAX_BACKOFF_SECONDS))
    if secs > 0:
        try:
            state_proxy.message = f"{reason}: sleeping ~{int(secs)}s"
            state_proxy.touch()
        except Exception:
            pass
        await asyncio.sleep(secs)


async def _search_with_backoff(
    reddit: asyncpraw.Reddit,
    subreddit_name: str,
    query: str,
    limit: Optional[int],
    time_filter: str,
    sort: str,
    state_proxy,
):
    """
    Yield submissions from a subreddit.search(query, ...) with robust retry/backoff
    on 429/5xx and ratelimit exceptions.
    """
    attempt = 0
    while True:
        try:
            subreddit = await reddit.subreddit(subreddit_name)
            # This async generator will make multiple HTTP calls under the hood.
            async for submission in subreddit.search(
                query, limit=limit, time_filter=time_filter, sort=sort
            ):
                yield submission
            return  # finished without errors
        except prawcore_exc.TooManyRequests as e:
            # Prefer Retry-After if present; otherwise exponential backoff + jitter
            retry_after = None
            try:
                retry_after = int(float(e.response.headers.get("retry-after", "")))
            except Exception:
                pass
            if retry_after is None:
                retry_after = min(MAX_BACKOFF_SECONDS, (2**attempt) + random.random())
            await _sleep_with_status(
                state_proxy, retry_after + 1, "429 Too Many Requests"
            )
        except prawcore_exc.ServerError:
            # 5xx — transient
            backoff = min(MAX_BACKOFF_SECONDS, (2**attempt) + random.random())
            await _sleep_with_status(state_proxy, backoff, "Server error")
        except RedditAPIException as e:
            # Async PRAW already slept up to RATELIMIT_SECONDS; if it still raises,
            # try to honor the requested time if we can parse it and it's reasonable.
            wait = _parse_wait_seconds_from_msg(str(e)) or 0
            if wait and wait <= MAX_BACKOFF_SECONDS:
                await _sleep_with_status(state_proxy, wait + 1, "API ratelimit")
            else:
                # Unknown/very long ratelimit or other API error; surface it.
                raise
        except (prawcore_exc.ResponseException, prawcore_exc.RequestException):
            # Network-y issues; backoff
            backoff = min(MAX_BACKOFF_SECONDS, (2**attempt) + random.random())
            await _sleep_with_status(state_proxy, backoff, "Network error")
        attempt += 1
        if attempt >= MAX_RETRIES:
            raise


async def scrape_players_async(
    players: List[str],
    subreddits: List[str],
    search_limit: Optional[int],
    time_filter: str,
    sort: str,
    state_proxy,
) -> None:
    """
    Scrape submissions + comments for every (player, subreddit) pair and push rows via
    state_proxy.write_row(player, row).
    """
    if not (REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET and REDDIT_USER_AGENT):
        raise RuntimeError(
            "Missing Reddit credentials (REDDIT_CLIENT_ID/SECRET/USER_AGENT)."
        )

    reddit = asyncpraw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
        ratelimit_seconds=RATELIMIT_SECONDS,
    )

    try:
        # progress counts player × subreddit units
        state_proxy.set_total(len(players) * len(subreddits))

        for pi, player in enumerate(players):
            for sub_name in subreddits:
                # allow pause/cancel between units
                if state_proxy.wait_if_paused_or_cancelled():
                    return

                state_proxy.current_player_index = pi
                state_proxy.message = f"Searching '{player}' in r/{sub_name}"
                state_proxy.touch()

                async for submission in _search_with_backoff(
                    reddit=reddit,
                    subreddit_name=sub_name,
                    query=player,
                    limit=search_limit,
                    time_filter=time_filter,
                    sort=sort,
                    state_proxy=state_proxy,
                ):
                    if state_proxy.wait_if_paused_or_cancelled():
                        return

                    await submission.load()
                    await submission.comments.replace_more(limit=0)
                    all_comments = submission.comments.list()

                    row: Dict[str, Any] = {
                        "subreddit": getattr(
                            submission.subreddit, "display_name", sub_name
                        ),
                        "submission_id": submission.id,
                        "title": submission.title or "",
                        "submission_url": submission.url,
                        "submission_text": submission.selftext or "",
                        "score": submission.score,
                        "upvote_ratio": getattr(submission, "upvote_ratio", None),
                        "num_comments": len(all_comments),
                        "created_utc": datetime.utcfromtimestamp(
                            submission.created_utc
                        ).isoformat(),
                        "comments": [
                            getattr(c, "body", "")
                            for c in all_comments
                            if getattr(c, "body", None)
                        ],
                        "search_player": player,
                    }
                    state_proxy.write_row(player, row)

                # finished this (player, subreddit) unit
                state_proxy.increment_progress()

        state_proxy.mark_finished()
    finally:
        await reddit.close()
