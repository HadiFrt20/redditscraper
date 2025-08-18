# tests/conftest.py
import time
import pytest

# Import your package and factory
import app as app_pkg
from app import create_app
import app.scraper as scraper_mod
from app.manager import get_manager


# --------------------------
# In-memory fake for GCS I/O
# --------------------------
class InMemoryGCS:
    """Very small in-memory blob store to stub GCS behavior."""

    def __init__(self):
        self.store = {}

    # bucket "handle"
    def bucket(self, name):
        return {"name": name}

    def upload_text(self, bkt, blob_name: str, text: str):
        # store exact text
        self.store[blob_name] = text

    def download_text(self, bkt, blob_name: str) -> str:
        return self.store.get(blob_name, "")

    def exists(self, bkt, blob_name: str) -> bool:
        return blob_name in self.store

    def compose_many(self, bkt, sources, dest, tmp_prefix):
        # concatenate sources in order
        payload = "".join(self.store.get(s, "") for s in sources)
        self.store[dest] = payload

    def signed_url(self, bkt, blob_name: str, ttl: int = 3600) -> str:
        return f"https://fake.local/{blob_name}?ttl={ttl}"


# --------------------------
# Pytest fixtures
# --------------------------
@pytest.fixture()
def fake_gcs(monkeypatch):
    """
    Patch GCS calls used by manager/routes with an in-memory fake.
    Returns the fake so tests can inspect uploaded content.
    """
    gcs = InMemoryGCS()

    # Patch the *symbols imported into app.manager*
    # manager did:
    #   from .gcs_io import bucket as gcs_bucket, upload_text,
    #                      exists as gcs_exists, compose_many, download_text
    monkeypatch.setattr(app_pkg.manager, "gcs_bucket", gcs.bucket, raising=True)
    monkeypatch.setattr(app_pkg.manager, "upload_text", gcs.upload_text, raising=True)
    monkeypatch.setattr(app_pkg.manager, "gcs_exists", gcs.exists, raising=True)
    monkeypatch.setattr(app_pkg.manager, "compose_many", gcs.compose_many, raising=True)
    monkeypatch.setattr(
        app_pkg.manager, "download_text", gcs.download_text, raising=True
    )

    # Some routes may import `signed_url` as `gcs_signed_url`
    # If present, patch it too (don't fail if not there).
    try:
        monkeypatch.setattr(
            app_pkg.routes, "gcs_signed_url", gcs.signed_url, raising=False
        )
    except Exception:
        pass

    return gcs


@pytest.fixture()
def players_csv(tmp_path, monkeypatch):
    """
    Create a temp players CSV and point routes to it so POST /scrape with no players uses this file.
    """
    path = tmp_path / "players.csv"
    path.write_text(
        "LeBron James\nNikola Jokic\nGiannis Antetokounmpo\n", encoding="utf-8"
    )

    # routes has a module-level constant PLAYERS_CSV_PATH
    monkeypatch.setattr(app_pkg.routes, "PLAYERS_CSV_PATH", str(path), raising=False)
    return path


@pytest.fixture()
def fast_chunks(monkeypatch):
    """
    Force CHUNK_ROWS=1 inside manager so every row flushes a part file.
    """
    monkeypatch.setattr(app_pkg.manager, "CHUNK_ROWS", 1, raising=False)


@pytest.fixture()
def fake_scraper(monkeypatch):
    """
    Replace the async scraper with a tiny, deterministic coroutine
    that writes one row per (player, subreddit) and finishes quickly.
    This runs under asyncio.run() inside the manager worker thread.
    """

    async def _fake_scrape(
        players,
        subreddits,
        search_limit,
        time_filter,
        sort,
        state_proxy,
        resume_cursor=None,  # accept new arg used by manager
        **kwargs,  # future-proof against more args
    ):
        total = len(players) * len(subreddits)
        state_proxy.set_total(total)

        for i, p in enumerate(players):
            state_proxy.current_player_index = i
            state_proxy.message = f"Searching '{p}'"
            state_proxy.touch()

            for sub in subreddits:
                if state_proxy.wait_if_paused_or_cancelled():
                    return
                row = {
                    "subreddit": sub,
                    "submission_id": f"{p}-demo-1",
                    "title": f"{p} in {sub}",
                    "submission_url": "https://example.local/post",
                    "submission_text": "",
                    "score": 1,
                    "upvote_ratio": 1.0,
                    "num_comments": 0,
                    "created_utc": "2020-01-01T00:00:00",
                    "comments": [],
                    "search_player": p,
                }
                state_proxy.write_row(p, row)

            state_proxy.increment_progress()

        state_proxy.mark_finished()

    monkeypatch.setattr(scraper_mod, "scrape_players_async", _fake_scrape, raising=True)


@pytest.fixture()
def app(fake_gcs, fast_chunks, players_csv):
    """
    Build a testing app. You can add any extra config overrides here.
    """
    flask_app = create_app()
    flask_app.config.update(TESTING=True)
    return flask_app


@pytest.fixture()
def client(app):
    return app.test_client()


@pytest.fixture()
def runner(app):
    return app.test_cli_runner()


@pytest.fixture(autouse=True)
def _stop_worker_after_test(fake_gcs):
    """
    Ensure the background worker is stopped after each test so patched GCS
    functions remain in place until the thread exits.
    """
    yield
    m = get_manager()
    if getattr(m, "thread", None) and m.thread.is_alive():
        try:
            m.cancel()
        except Exception:
            pass
        # wait briefly for the thread to exit
        deadline = time.time() + 2.0
        while m.thread.is_alive() and time.time() < deadline:
            time.sleep(0.05)
