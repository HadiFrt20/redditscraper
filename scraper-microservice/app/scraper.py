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
    subreddits: List[str],             # multiple subreddits
    search_limit: Optional[int],
    time_filter: str,
    sort: str,
    state_proxy,                       # ScrapeManager
) -> None:
    """
    Scrape submissions + comments for every (player, subreddit) pair and push rows via
    state_proxy.write_row(player, row).

    state_proxy must implement:
      - set_total(n: int)
      - wait_if_paused_or_cancelled() -> bool
      - write_row(player: str, row: Dict[str, Any])
      - increment_progress()
      - mark_finished()
      - touch(), and fields current_player_index, message (optional)
    """
    if not (REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET and REDDIT_USER_AGENT):
        raise RuntimeError("Missing Reddit credentials (REDDIT_CLIENT_ID/SECRET/USER_AGENT).")

    reddit = asyncpraw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
    )

    try:
        # progress counts player × subreddit units
        state_proxy.set_total(len(players) * len(subreddits))

        for pi, player in enumerate(players):
            for si, sub_name in enumerate(subreddits):
                # allow pause/cancel between units
                if state_proxy.wait_if_paused_or_cancelled():
                    return

                state_proxy.current_player_index = pi
                state_proxy.message = f"Searching '{player}' in r/{sub_name}"
                state_proxy.touch()

                subreddit = await reddit.subreddit(sub_name)

                async for submission in subreddit.search(
                    player,
                    limit=search_limit,
                    time_filter=time_filter,
                    sort=sort
                ):
                    if state_proxy.wait_if_paused_or_cancelled():
                        return

                    # ensure fields loaded & expand comments
                    await submission.load()
                    await submission.comments.replace_more(limit=0)
                    all_comments = submission.comments.list()

                    row: Dict[str, Any] = {
                        "subreddit": getattr(submission.subreddit, "display_name", sub_name),  # will be ignored if not in CSV_FIELDS
                        "submission_id": submission.id,
                        "title": submission.title or "",
                        "submission_url": submission.url,
                        "submission_text": submission.selftext or "",
                        "score": submission.score,
                        "upvote_ratio": getattr(submission, "upvote_ratio", None),
                        "num_comments": len(all_comments),
                        "created_utc": datetime.utcfromtimestamp(submission.created_utc).isoformat(),
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
