from app.manager import get_manager


def test_health_ok(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json == {"status": "ok"}


def test_gae_health_ok(client):
    resp = client.get("/_ah/health")
    assert resp.status_code == 200
    assert resp.get_data(as_text=True).strip() == "ok"


def test_gae_start_ok(client):
    resp = client.get("/_ah/start")
    assert resp.status_code == 204
    assert resp.get_data() == b""


def test_home_status_message(client):
    resp = client.get("/")
    assert resp.status_code == 200
    text = resp.get_data(as_text=True)
    assert "NBA Scraper Service" in text
    assert "Service is running" in text


def test_start_scrape_with_subs_array_finishes(client, fake_scraper):
    payload = {
        "players": ["LeBron James", "Nikola Jokic"],
        "subreddits": ["nba", "nbadiscussion"],
        "search_limit": 1,
        "time_filter": "year",
        "sort": "new",
    }
    r = client.post("/scrape", json=payload)
    assert r.status_code == 202

    prog = client.get("/scrape/progress").get_json()
    assert {
        "status",
        "message",
        "total_units",
        "completed_units",
        "percent",
        "current_player_index",
    } <= set(prog)


def test_start_scrape_accepts_single_subreddit_string(client, fake_scraper):
    payload = {
        "players": ["LeBron James"],
        "subreddit": "nba",
        "search_limit": 1,
    }
    r = client.post("/scrape", json=payload)
    assert r.status_code == 202


def test_start_scrape_busy_returns_409(client, fake_scraper, monkeypatch):
    m = get_manager()
    with m.lock:
        m.status = "running"

    try:
        r = client.post("/scrape", json={"players": ["X"], "subreddits": ["nba"]})
        assert r.status_code == 409
        body = r.get_json()
        assert body["status"] == "busy"
    finally:
        with m.lock:
            m.status = "idle"
            m.message = ""


def test_scrape_progress_shape(client):
    r = client.get("/scrape/progress")
    assert r.status_code == 200
    body = r.get_json()
    assert {
        "status",
        "message",
        "total_units",
        "completed_units",
        "percent",
        "current_player_index",
    } <= set(body)


def test_pause_resume_cancel_flow(client, fake_scraper):
    # start a job
    client.post(
        "/scrape",
        json={"players": ["A", "B"], "subreddits": ["nba"], "search_limit": 1},
    )

    # pause
    r = client.post("/scrape/pause")
    assert r.status_code == 200
    assert r.get_json()["status"] == "paused"

    # resume
    r = client.post("/scrape/resume")
    assert r.status_code == 200
    assert r.get_json()["status"] == "running"

    # cancel
    r = client.post("/scrape/cancel")
    assert r.status_code == 200
    assert r.get_json()["status"] in ("cancelling", "cancelled")  # depending on timing


def test_pause_when_not_running_returns_400(client):
    r = client.post("/scrape/pause")
    assert r.status_code == 400


def test_resume_when_not_paused_returns_400(client):
    r = client.post("/scrape/resume")
    assert r.status_code == 400


def test_cancel_when_not_running_returns_400(client):
    r = client.post("/scrape/cancel")
    assert r.status_code == 400


def test_results_list_empty_when_no_job(client):
    r = client.get("/scrape/results")
    assert r.status_code == 200
    body = r.get_json()
    assert body["status"] in (
        "idle",
        "finished",
        "error",
        "cancelled",
        "running",
        "paused",
        "cancelling",
    )
    assert "files" in body


def test_results_list_has_entries_after_start(client, fake_scraper):
    players = ["LeBron James", "Nikola Jokic"]
    subs = ["nba"]
    client.post(
        "/scrape", json={"players": players, "subreddits": subs, "search_limit": 1}
    )

    r = client.get("/scrape/results")
    assert r.status_code == 200
    body = r.get_json()
    assert "job_id" in body and "job_prefix" in body
    assert isinstance(body["files"], list)


def test_resume_checkpoint_missing_job_id_returns_400(client):
    resp = client.post("/scrape/resume-checkpoint", json={})
    assert resp.status_code == 400
    body = resp.get_json()
    assert "error" in body


def test_checkpoints_list_empty(client, monkeypatch):
    class _FakeBlob:
        def __init__(self, name):
            self.name = name

    class _FakeBucket:
        def list_blobs(self, prefix=None):
            return []

    class _FakeClient:
        def bucket(self, name):
            return _FakeBucket()

    monkeypatch.setattr("google.cloud.storage.Client", lambda *a, **k: _FakeClient())

    r = client.get("/scrape/checkpoints")
    assert r.status_code == 200
    body = r.get_json()
    assert body == {"checkpoints": []}


def test_checkpoints_list_has_items(client, monkeypatch):
    class _FakeBlob:
        def __init__(self, name):
            self.name = name

    class _FakeBucket:
        def list_blobs(self, prefix=None):
            return [
                _FakeBlob("checkpointing/job-2025-08-16T13-31-41.json"),
                _FakeBlob("checkpointing/job-2025-08-16T14-02-10.json"),
                _FakeBlob("checkpointing/README.txt"),  # should be ignored
            ]

    class _FakeClient:
        def bucket(self, name):
            return _FakeBucket()

    monkeypatch.setattr("google.cloud.storage.Client", lambda *a, **k: _FakeClient())

    r = client.get("/scrape/checkpoints")
    assert r.status_code == 200
    body = r.get_json()
    assert body == {
        "checkpoints": [
            "job-2025-08-16T13-31-41.json",
            "job-2025-08-16T14-02-10.json",
        ]
    }
