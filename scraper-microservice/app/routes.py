# app/routes.py
from flask import Blueprint, jsonify, request, send_file, abort
import io

from .manager import get_manager

scrape_bp = Blueprint("scrape", __name__)


# ---- App Engine readiness endpoints ----
@scrape_bp.get("/_ah/health")
def gae_health():
    return "ok", 200


@scrape_bp.get("/_ah/start")
def gae_start():
    # Called on instance start; keep it fast and side-effect free.
    return "", 204


# ---- Basic health & home ----
@scrape_bp.get("/health")
def health():
    return jsonify({"status": "ok"})


@scrape_bp.get("/")
def home():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>NBA Scraper</title>
        <style>
            body { font-family: sans-serif; margin: 2em; background: #f9f9f9; }
            h1 { color: #0055a5; }
            code { background: #eee; padding: 2px 4px; border-radius: 4px; }
        </style>
    </head>
    <body>
        <h1>🏀 NBA Scraper Service</h1>
        <p>Service is running. Try <code>/health</code>, <code>POST /scrape</code>,
           or <code>GET /scrape/progress</code>.</p>
    </body>
    </html>
    """


# ---- Scrape controls ----
@scrape_bp.post("/scrape")
def start_scrape():
    # Local import keeps cold starts and readiness checks light.
    from .utils import players_from_csv

    m = get_manager()
    data = request.get_json(silent=True) or {}

    players_csv_path = "./players_names.csv"
    default_subreddit = "nbadiscussion"

    # players (CSV fallback)
    players = data.get("players")
    if not players:
        players = players_from_csv(players_csv_path)

    # subreddits: accept array or single string
    subs = data.get("subreddits")
    if not subs:
        one = data.get("subreddit", default_subreddit)
        subs = [one]
    elif isinstance(subs, str):
        subs = [subs]

    try:
        m.start(
            players=players,
            subreddits=subs,
            search_limit=data.get("search_limit", None),
            time_filter=data.get("time_filter", "all"),
            sort=data.get("sort", "new"),
            # optional: resume if a job_id is provided
            resume_job_id=data.get("resume_job_id"),
        )
        return jsonify({"status": "accepted", "message": "Job started"}), 202
    except RuntimeError as e:
        return jsonify({"status": "busy", "message": str(e)}), 409
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@scrape_bp.get("/scrape/progress")
def scrape_progress():
    m = get_manager()
    with m.lock:
        pct = (m.completed_units / m.total_units * 100.0) if m.total_units else 0.0
        return jsonify(
            {
                "status": m.status,
                "message": m.message,
                "total_units": m.total_units,
                "completed_units": m.completed_units,
                "percent": round(pct, 2),
                "current_player_index": m.current_player_index,
            }
        )


# ---- GCS-backed results ----
@scrape_bp.get("/scrape/results")
def list_results():
    m = get_manager()
    info = m.current_job_info()
    job_prefix = info.get("job_prefix")
    if not job_prefix:
        return jsonify({"status": info.get("status"), "files": []})

    files = []
    for player, slug in info.get("slugs", {}).items():
        files.append(
            {
                "player": player,
                "slug": slug,
                "final_blob": f"{job_prefix}{slug}.csv",
                "parts": info.get("parts", {}).get(slug, 0),
            }
        )

    return jsonify(
        {
            "status": info.get("status"),
            "message": info.get("message"),
            "job_id": info.get("job_id"),
            "job_prefix": job_prefix,
            "chunk_rows": info.get("chunk_rows"),
            "files": files,
        }
    )


@scrape_bp.get("/scrape/results/<slug>.csv")
def download_player_csv(slug: str):
    # Heavy deps imported lazily to keep readiness snappy.
    from google.cloud import storage
    from .config import GCP_BUCKET

    m = get_manager()
    info = m.current_job_info()
    job_prefix = info.get("job_prefix")
    if not job_prefix:
        abort(404, "No current job")

    final_blob_name = m.compose_final_if_needed(slug)

    client = storage.Client()
    bucket = client.bucket(GCP_BUCKET)
    blob = bucket.blob(final_blob_name)
    if not blob.exists():
        abort(404, f"Final CSV not found for slug '{slug}'")

    data = blob.download_as_bytes()
    return send_file(
        io.BytesIO(data),
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"{slug}.csv",
    )


@scrape_bp.get("/scrape/results/<slug>.url")
def signed_url_for_player_csv(slug: str):
    from google.cloud import storage
    from .config import GCP_BUCKET

    m = get_manager()
    info = m.current_job_info()
    job_prefix = info.get("job_prefix")
    if not job_prefix:
        abort(404, "No current job")

    final_blob_name = m.compose_final_if_needed(slug)

    client = storage.Client()
    bucket = client.bucket(GCP_BUCKET)
    blob = bucket.blob(final_blob_name)
    if not blob.exists():
        abort(404, f"Final CSV not found for slug '{slug}'")

    url = blob.generate_signed_url(version="v4", expiration=3600, method="GET")
    return jsonify({"url": url})


# ---- pause / resume / cancel ----
@scrape_bp.post("/scrape/pause")
def scrape_pause():
    m = get_manager()
    try:
        m.pause()
        return jsonify({"status": "paused", "message": "Job paused"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@scrape_bp.post("/scrape/resume")
def scrape_resume():
    m = get_manager()
    try:
        m.resume()
        return jsonify({"status": "running", "message": "Job resumed"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@scrape_bp.post("/scrape/cancel")
def scrape_cancel():
    m = get_manager()
    try:
        m.cancel()
        return (
            jsonify({"status": "cancelling", "message": "Cancellation requested"}),
            200,
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@scrape_bp.post("/scrape/resume-checkpoint")
def resume_from_checkpoint():
    """Body: { "job_id": "job-YYYY-MM-DDTHH-MM-SS" }"""
    m = get_manager()
    data = request.get_json(silent=True) or {}
    job_id = data.get("job_id")
    if not job_id:
        return jsonify({"error": "job_id is required"}), 400
    try:
        m.resume_from_checkpoint(job_id)
        return jsonify({"status": "accepted", "message": f"Resuming {job_id}"}), 202
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@scrape_bp.get("/scrape/checkpoints")
def list_checkpoints():
    """List checkpoint files under checkpointing/ (names only)."""
    from google.cloud import storage
    from .config import GCP_BUCKET

    client = storage.Client()
    bucket = client.bucket(GCP_BUCKET)
    prefix = "checkpointing/"
    blobs = bucket.list_blobs(prefix=prefix)
    names = [b.name[len(prefix) :] for b in blobs if b.name.endswith(".json")]
    return jsonify({"checkpoints": names})
