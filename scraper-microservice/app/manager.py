# app/manager.py
import csv
import io
import json
import time
import threading
import asyncio
from typing import List, Dict, Any, Optional

from .config import GCP_BUCKET, RESULTS_PREFIX, CHUNK_ROWS
from .utils import slugify
from .gcs_io import (
    bucket as gcs_bucket,
    upload_text,
    exists as gcs_exists,
    compose_many,
)

# CSV schema (one row per submission)
CSV_FIELDS = [
    "subreddit",
    "submission_id",
    "title",
    "submission_url",
    "submission_text",
    "score",
    "upvote_ratio",
    "num_comments",
    "created_utc",
    "search_player",
    "comments_json",
]


def _now_job_id() -> str:
    return time.strftime("job-%Y-%m-%dT%H-%M-%S")


class ScrapeManager:
    def __init__(self):
        self.lock = threading.RLock()
        self.thread: Optional[threading.Thread] = None

        # status
        self.status = "idle"  # idle|running|paused|cancelling|cancelled|finished|error
        self.message = ""
        self._cancel = False

        # progress
        self.total_units = 0
        self.completed_units = 0
        self.current_player_index = 0
        self.updated_at: Optional[float] = None

        # params
        self.players: List[str] = []
        self.subreddits: List[str] = ["nbadiscussion"]
        self.search_limit: Optional[int] = 10
        self.time_filter = "year"
        self.sort = "new"

        # GCS context — LAZY (don’t touch GCS on import)
        self.bkt_name = GCP_BUCKET
        self.bkt = None  # will be created on first use

        # job info
        self.job_id: Optional[str] = None  # e.g. job-2025-08-14T12-34-56
        self.job_prefix: Optional[str] = None  # e.g. scrapes/job-.../
        self.slug_map: Dict[str, str] = {}  # player -> slug

        # per-player streaming buffers & part counts
        self.buffers: Dict[str, List[Dict[str, Any]]] = {}  # slug -> rows pending
        self.part_counts: Dict[str, int] = {}  # slug -> parts written

    # ---------- internals ----------
    def _ensure_bucket(self):
        if self.bkt is None:
            self.bkt = gcs_bucket(self.bkt_name)

    def is_running(self) -> bool:
        return self.status in ("running", "paused", "cancelling")

    def touch(self):
        self.updated_at = time.time()

    # ---------- lifecycle ----------
    def start(
        self,
        players: List[str],
        subreddits: List[str],
        search_limit: Optional[int],
        time_filter: str,
        sort: str,
    ):
        with self.lock:
            if self.is_running():
                raise RuntimeError("A job is already running.")

            self.status = "running"
            self.message = "Scrape started"
            self._cancel = False

            self.players = players

            # normalize subreddit list (dedupe, keep order)
            seen = set()
            self.subreddits = []
            for s in subreddits or ["nbadiscussion"]:
                if s not in seen:
                    self.subreddits.append(s)
                    seen.add(s)

            self.search_limit = search_limit
            self.time_filter = time_filter
            self.sort = sort

            # progress counts (player, subreddit) units
            self.total_units = len(self.players) * len(self.subreddits)
            self.completed_units = 0
            self.current_player_index = 0
            self.updated_at = time.time()

            # job layout in GCS
            self.job_id = _now_job_id()
            self.job_prefix = f"{RESULTS_PREFIX}/{self.job_id}/"  # trailing slash

            # map players -> unique slugs, init buffers
            used = set()
            self.slug_map.clear()
            self.buffers.clear()
            self.part_counts.clear()

            for p in players:
                base = slugify(p)
                slug = base
                i = 2
                while slug in used:
                    slug = f"{base}-{i}"
                    i += 1
                used.add(slug)
                self.slug_map[p] = slug
                self.buffers[slug] = []
                self.part_counts[slug] = 0

            # we’re about to write headers to GCS
            self._ensure_bucket()

            # write a header object per player (once)
            header_line = ",".join(CSV_FIELDS) + "\n"
            for slug in used:
                header_blob = f"{self.job_prefix}{slug}/header.csv"
                if not gcs_exists(self.bkt, header_blob):
                    upload_text(self.bkt, header_blob, header_line)

            # start background worker
            self.thread = threading.Thread(
                target=self._worker, name="scraper-worker", daemon=True
            )
            self.thread.start()

    # ---------- used by scraper ----------
    def set_total(self, n: int):
        with self.lock:
            self.total_units = n
            self.touch()

    def write_row(self, player: str, row: Dict[str, Any]):
        """
        Buffer a row for the player's CSV and flush to a 'part-xxxxx.csv' in GCS
        when the buffer reaches CHUNK_ROWS.
        """
        slug = self.slug_map[player]
        out = dict(row)
        # normalize key + encode comments as JSON (one CSV cell)
        out["submission_url"] = out.pop("submision_url", out.get("submission_url", ""))
        out["comments_json"] = json.dumps(out.pop("comments", []), ensure_ascii=False)

        buf = self.buffers[slug]
        buf.append(out)
        if len(buf) >= CHUNK_ROWS:
            self._flush_chunk(slug)

    def _flush_chunk(self, slug: str):
        buf = self.buffers[slug]
        if not buf:
            return

        # rows -> CSV (no header)
        sio = io.StringIO()
        writer = csv.DictWriter(sio, fieldnames=CSV_FIELDS, extrasaction="ignore")
        for r in buf:
            writer.writerow(r)
        payload = sio.getvalue()
        sio.close()

        # write to GCS as next part
        self._ensure_bucket()
        self.part_counts[slug] += 1
        part_no = self.part_counts[slug]
        blob_name = f"{self.job_prefix}{slug}/part-{part_no:05d}.csv"
        upload_text(self.bkt, blob_name, payload)

        # clear buffer
        self.buffers[slug] = []
        self.touch()

    def compose_final_if_needed(self, slug: str) -> str:
        """
        Compose header+parts into the final CSV object if it doesn't exist yet.
        Returns final blob name (e.g. scrapes/job-.../lebron-james.csv).
        """
        self._ensure_bucket()
        final_blob = f"{self.job_prefix}{slug}.csv"
        if gcs_exists(self.bkt, final_blob):
            return final_blob

        # flush remainder into a last part (if any)
        self._flush_chunk(slug)

        # build source list: header first, then parts in order
        sources = [f"{self.job_prefix}{slug}/header.csv"]
        n = self.part_counts.get(slug, 0)
        if n > 0:
            sources += [
                f"{self.job_prefix}{slug}/part-{i:05d}.csv" for i in range(1, n + 1)
            ]

        # compose (use positional args to avoid param-name mismatches)
        compose_many(
            self.bkt, sources, final_blob, f"{self.job_prefix}{slug}/_compose_tmp"
        )
        return final_blob

    def increment_progress(self):
        with self.lock:
            self.completed_units += 1
            self.message = f"Completed {self.completed_units}/{self.total_units}"
            self.touch()

    def mark_finished(self):
        with self.lock:
            if self.status not in ("cancelled", "error"):
                self.status = "finished"
                self.message = "Finished"
            self.touch()

    def wait_if_paused_or_cancelled(self) -> bool:
        while True:
            with self.lock:
                if self._cancel:
                    self.status = "cancelled"
                    self.message = "Cancelled"
                    self.touch()
                    return True
                paused = self.status == "paused"
            if not paused:
                return False
            time.sleep(0.2)

    # ---------- background thread ----------
    def _worker(self):
        from .scraper import scrape_players_async

        try:
            asyncio.run(
                scrape_players_async(
                    players=self.players,
                    subreddits=self.subreddits,
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

    # ---------- controls ----------
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

    # ---------- info for routes ----------
    def current_job_info(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "job_id": self.job_id,
                "job_prefix": self.job_prefix,
                "status": self.status,
                "message": self.message,
                "total_units": self.total_units,
                "completed_units": self.completed_units,
                "players": self.players,
                "subreddits": self.subreddits,
                "slugs": self.slug_map,
                "parts": self.part_counts,
                "chunk_rows": CHUNK_ROWS,
            }


_MANAGER: Optional[ScrapeManager] = None


def get_manager() -> ScrapeManager:
    global _MANAGER
    if _MANAGER is None:
        _MANAGER = ScrapeManager()
    return _MANAGER
