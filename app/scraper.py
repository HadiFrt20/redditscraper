# app/scraper.py
from datetime import datetime
from typing import List, Optional, Dict, Any

import asyncpraw

from .config import (
    REDDIT_CLIENT_ID,
    REDDIT_CLIENT_SECRET,
    REDDIT_USER_AGENT,
)

async def scrape_players_async(
    players: List[str],
    subreddit_name: str,
    search_limit: Optional[int],
    time_filter: str,
    sort: str,
    state_proxy,  # your ScrapeManager instance
) -> None:
    """
    Scrapes submissions + comments for each player name and pushes rows into state_proxy.
    state_proxy must implement:
      - set_total(n: int)
      - wait_if_paused_or_cancelled() -> bool
      - add_row(row: Dict[str, Any])
      - increment_progress()
      - mark_finished()
      - touch() and fields current_player_index, message (optional but nice)
    """
    if not (REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET and REDDIT_USER_AGENT):
        raise RuntimeError("Missing Reddit credentials (REDDIT_CLIENT_ID/SECRET/USER_AGENT).")

    reddit = asyncpraw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
    )

    try:
        subreddit = await reddit.subreddit(subreddit_name)
        state_proxy.set_total(len(players))

        for i, player in enumerate(players):
            # allow pause/cancel between players
            if state_proxy.wait_if_paused_or_cancelled():
                return

            state_proxy.current_player_index = i
            state_proxy.message = f"Searching '{player}'"
            state_proxy.touch()

            async for submission in subreddit.search(
                player, limit=search_limit, time_filter=time_filter, sort=sort
            ):
                if state_proxy.wait_if_paused_or_cancelled():
                    return

                # load all fields and comments
                await submission.load()
                await submission.comments.replace_more(limit=0)
                all_comments = submission.comments.list()
                comments_text = [
                    getattr(c, "body", "")
                    for c in all_comments
                    if getattr(c, "body", None)
                ]

                row: Dict[str, Any] = {
                    "submission_id": submission.id,
                    "title": submission.title or "",
                    "submision_url": submission.url,
                    "submission_text": submission.selftext or "",
                    "score": submission.score,
                    "upvote_ratio": getattr(submission, "upvote_ratio", None),
                    "num_comments": len(all_comments),
                    "created_utc": datetime.utcfromtimestamp(submission.created_utc).isoformat(),
                    "comments": comments_text,
                    "search_player": player,
                }
                state_proxy.add_row(row)

            # finished this player
            state_proxy.increment_progress()

        state_proxy.mark_finished()
    finally:
        await reddit.close()
