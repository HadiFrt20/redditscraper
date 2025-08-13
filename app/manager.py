# app/manager.py
import time
import threading
import asyncio
from typing import List, Dict, Any, Optional

class ScrapeManager:
    def __init__(self):
        self.lock = threading.RLock()
        self.thread: Optional[threading.Thread] = None

        # status
        self.status = "idle"  # idle|running|finished|error|paused|cancelling|cancelled
        self.message = ""
        self._cancel = False

        # progress
        self.total_units = 0
        self.completed_units = 0
        self.current_player_index = 0
        self.updated_at: Optional[float] = None

        # params
        self.players: List[str] = []
        self.subreddit = "nbadiscussion"
        self.search_limit: Optional[int] = 10
        self.time_filter = "year"
        self.sort = "new"

        # data
        self.rows: List[Dict[str, Any]] = []

    # ---------- lifecycle ----------
    def start(self, players: List[str], subreddit: str, search_limit: Optional[int], time_filter: str, sort: str):
        with self.lock:
            if self.is_running():
                raise RuntimeError("A job is already running.")
            # reset state
            self.status = "running"
            self.message = "Scrape started"
            self._cancel = False

            self.players = players
            self.subreddit = subreddit or "nbadiscussion"
            self.search_limit = search_limit
            self.time_filter = time_filter
            self.sort = sort

            self.total_units = len(players)
            self.completed_units = 0
            self.current_player_index = 0
            self.rows = []
            self.touch()

            self.thread = threading.Thread(target=self._worker, name="scraper-worker", daemon=True)
            self.thread.start()

    def is_running(self) -> bool:
        return self.status in ("running", "paused", "cancelling")

    # ---------- used by scraper ----------
    def set_total(self, n: int):
        with self.lock:
            self.total_units = n
            self.touch()

    def add_row(self, row: Dict[str, Any]):
        with self.lock:
            self.rows.append(row)
            self.touch()

    def increment_progress(self):
        with self.lock:
            self.completed_units += 1
            self.message = f"Completed {self.completed_units}/{self.total_units}"
            self.touch()

    def mark_finished(self):
        with self.lock:
            # don't override a cancellation/error
            if self.status not in ("cancelled", "error"):
                self.status = "finished"
                self.message = "Finished"
            self.touch()

    def wait_if_paused_or_cancelled(self) -> bool:
        """
        Returns True if the caller should stop (cancelled).
        Sleeps briefly while paused.
        """
        while True:
            with self.lock:
                if self._cancel:
                    self.status = "cancelled"
                    self.message = "Cancelled"
                    self.touch()
                    return True
                paused = (self.status == "paused")
            if not paused:
                return False
            time.sleep(0.2)

    def touch(self):
        self.updated_at = time.time()

    # ---------- background thread ----------
    def _worker(self):
        # Import here (on purpose) as you requested
        from .scraper import scrape_players_async

        try:
            asyncio.run(
                scrape_players_async(
                    players=self.players,
                    subreddit_name=self.subreddit,
                    search_limit=self.search_limit,
                    time_filter=self.time_filter,
                    sort=self.sort,
                    state_proxy=self,
                )
            )
        except Exception as e:
            with self.lock:
                self.status = "error"
                self.message = f"Error: {e}"
                self.touch()

    # ---------- optional controls (stub now; easy to extend) ----------
    def pause(self):
        with self.lock:
            if self.status != "running":
                raise RuntimeError("Job is not running.")
            self.status = "paused"
            self.message = "Paused"
            self.touch()

    def resume(self):
        with self.lock:
            if self.status != "paused":
                raise RuntimeError("Job is not paused.")
            self.status = "running"
            self.message = "Resumed"
            self.touch()

    def cancel(self):
        with self.lock:
            if self.status not in ("running", "paused"):
                raise RuntimeError("No running job to cancel.")
            self._cancel = True
            self.status = "cancelling"
            self.message = "Cancelling..."
            self.touch()

# global singleton for routes to use
MANAGER = ScrapeManager()
