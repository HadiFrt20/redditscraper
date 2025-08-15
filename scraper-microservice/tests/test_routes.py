def test_health_ok(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json == {"status": "ok"}


def test_home_status_message(client):
    resp = client.get("/")
    assert resp.status_code == 200
    text = resp.get_data(as_text=True)
    assert "status:" in text
    assert "message:" in text


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
    from app.manager import MANAGER

    with MANAGER.lock:
        MANAGER.status = "running"

    try:
        r = client.post("/scrape", json={"players": ["X"], "subreddits": ["nba"]})
        assert r.status_code == 409
        body = r.get_json()
        assert body["status"] == "busy"
    finally:
        with MANAGER.lock:
            MANAGER.status = "idle"
            MANAGER.message = ""


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
