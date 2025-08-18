# app/scraper.py
from __future__ import annotations

import asyncio
import random
import re
from datetime import datetime
from typing import List, Optional, Dict, Any, Callable

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
    Yield submissions from subreddit.search(query, ...) with retry/backoff.
    - 403/404 (private/missing): skip this unit immediately.
    - 429/5xx: retry with backoff.
    - RedditAPIException with ratelimit message: sleep (bounded) then retry.
    """
    attempt = 0
    while True:
        try:
            subreddit = await reddit.subreddit(subreddit_name)
            async for submission in subreddit.search(
                query, limit=limit, time_filter=time_filter, sort=sort
            ):
                yield submission
            return  # finished without errors
        except prawcore_exc.TooManyRequests as e:
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
        except prawcore_exc.ResponseException as e:
            # If we have a response and it's a client error (403/404/451), skip this unit.
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status in (403, 404, 451):
                try:
                    state_proxy.message = f"Skipping r/{subreddit_name} (HTTP {status})"
                    state_proxy.touch()
                except Exception:
                    pass
                return  # skip this unit
            # Otherwise treat as transient and retry with backoff.
            backoff = min(MAX_BACKOFF_SECONDS, (2**attempt) + random.random())
            await _sleep_with_status(state_proxy, backoff, f"HTTP {status or 'error'}")
        except prawcore_exc.RequestException:
            # Network-ish errors; backoff
            backoff = min(MAX_BACKOFF_SECONDS, (2**attempt) + random.random())
            await _sleep_with_status(state_proxy, backoff, "Network error")
        except RedditAPIException as e:
            # Async PRAW sleeps up to RATELIMIT_SECONDS; if still raised, try a bounded sleep.
            wait = _parse_wait_seconds_from_msg(str(e)) or 0
            if wait and wait <= MAX_BACKOFF_SECONDS:
                await _sleep_with_status(state_proxy, wait + 1, "API ratelimit")
            else:
                raise
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
    *,
    resume_cursor: Optional[Dict[str, int]] = None,
    update_resume_cursor: Optional[Callable[[int, int], None]] = None,
) -> None:
    """
    Scrape submissions + comments for every (player, subreddit) pair and push rows via
    state_proxy.write_row(player, row).

    Resume support:
      - Pass resume_cursor={"player_index": int, "subreddit_index": int}
      - We'll skip directly to those indices.
      - We'll call update_resume_cursor(pi, si) right before starting each unit,
        so a crash resumes at the beginning of that unit.
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

    # Determine where to start from a checkpoint
    start_pi = 0
    start_si = 0
    if resume_cursor:
        start_pi = int(resume_cursor.get("player_index", 0))
        start_si = int(resume_cursor.get("subreddit_index", 0))

    try:
        state_proxy.set_total(len(players) * len(subreddits))

        for pi in range(start_pi, len(players)):
            player = players[pi]
            first_si = start_si if pi == start_pi else 0

            for si in range(first_si, len(subreddits)):
                sub_name = subreddits[si]

                if state_proxy.wait_if_paused_or_cancelled():
                    return

                state_proxy.current_player_index = pi
                state_proxy.message = f"Searching '{player}' in r/{sub_name}"
                state_proxy.touch()

                # Persist forward progress before starting this unit
                if update_resume_cursor:
                    update_resume_cursor(pi, si)

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
