# app/routes.py
import io
from flask import Blueprint, jsonify, request, send_file, abort
from google.cloud import storage  # NEW: use GCS
from .manager import MANAGER
from .utils import players_from_csv

PLAYERS_CSV_PATH = "./players_names.csv"
DEFAULT_SUBREDDIT = "nbadiscussion"

scrape_bp = Blueprint("scrape", __name__)


@scrape_bp.get("/health")
def health():
    return jsonify({"status": "ok"})


@scrape_bp.get("/")
def home():
    return f"status: {MANAGER.status}\n message: {MANAGER.message}"


@scrape_bp.post("/scrape")
def start_scrape():
    data = request.get_json(silent=True) or {}

    # players (CSV fallback)
    players = data.get("players")
    if not players:
        players = players_from_csv(PLAYERS_CSV_PATH)[1:3]

    # subreddits: accept array or single string
    subs = data.get("subreddits")
    if not subs:
        one = data.get("subreddit", DEFAULT_SUBREDDIT)
        subs = [one]
    elif isinstance(subs, str):
        subs = [subs]

    try:
        MANAGER.start(
            players=players,
            subreddits=subs,  # <— CHANGED
            search_limit=data.get("search_limit", None),
            time_filter=data.get("time_filter", "all"),
            sort=data.get("sort", "new"),
        )
        return jsonify({"status": "accepted", "message": "Job started"}), 202
    except RuntimeError as e:
        return jsonify({"status": "busy", "message": str(e)}), 409
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@scrape_bp.get("/scrape/progress")
def scrape_progress():
    with MANAGER.lock:
        pct = (
            (MANAGER.completed_units / MANAGER.total_units * 100.0)
            if MANAGER.total_units
            else 0.0
        )
        return jsonify(
            {
                "status": MANAGER.status,
                "message": MANAGER.message,
                "total_units": MANAGER.total_units,
                "completed_units": MANAGER.completed_units,
                "percent": round(pct, 2),
                "current_player_index": MANAGER.current_player_index,
            }
        )


# ---------- GCS-backed results ----------


@scrape_bp.get("/scrape/results")
def list_results():
    """
    Lists per-player CSV info for the CURRENT job.
    Returns: status, job_id, job_prefix, and one entry per player:
      { player, slug, final_blob, parts }
    """
    info = MANAGER.current_job_info()
    job_prefix = info.get("job_prefix")
    if not job_prefix:
        return jsonify({"status": MANAGER.status, "files": []})

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
    """
    Compose header+parts into the final player CSV if needed,
    then stream the object bytes back to the client.
    """
    info = MANAGER.current_job_info()
    job_prefix = info.get("job_prefix")
    if not job_prefix:
        abort(404, "No current job")

    # Compose the final CSV if it doesn't exist yet
    final_blob_name = MANAGER.compose_final_if_needed(slug)

    # Stream from GCS (for very large files, consider the signed-URL endpoint below)
    client = storage.Client()
    # Use the same bucket name your manager uses (it stores it in its config)
    from .config import (
        GCP_BUCKET,
    )  # import here to avoid circular imports at module load

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


# Optional: return a time-limited signed URL instead of streaming through Flask
@scrape_bp.get("/scrape/results/<slug>.url")
def signed_url_for_player_csv(slug: str):
    info = MANAGER.current_job_info()
    job_prefix = info.get("job_prefix")
    if not job_prefix:
        abort(404, "No current job")

    final_blob_name = MANAGER.compose_final_if_needed(slug)

    client = storage.Client()
    from .config import GCP_BUCKET

    bucket = client.bucket(GCP_BUCKET)
    blob = bucket.blob(final_blob_name)
    if not blob.exists():
        abort(404, f"Final CSV not found for slug '{slug}'")

    url = blob.generate_signed_url(version="v4", expiration=3600, method="GET")
    return jsonify({"url": url})


# --- controls: pause / resume / cancel ---


@scrape_bp.post("/scrape/pause")
def scrape_pause():
    try:
        MANAGER.pause()
        return (
            jsonify(
                {
                    "status": "paused",
                    "message": "Job paused",
                }
            ),
            200,
        )
    except Exception as e:
        # e.g., "Job is not running."
        return jsonify({"error": str(e)}), 400


@scrape_bp.post("/scrape/resume")
def scrape_resume():
    try:
        MANAGER.resume()
        return (
            jsonify(
                {
                    "status": "running",
                    "message": "Job resumed",
                }
            ),
            200,
        )
    except Exception as e:
        # e.g., "Job is not paused."
        return jsonify({"error": str(e)}), 400


@scrape_bp.post("/scrape/cancel")
def scrape_cancel():
    try:
        MANAGER.cancel()
        # Manager sets status to "cancelling" immediately; background loop exits on next check
        return (
            jsonify(
                {
                    "status": "cancelling",
                    "message": "Cancellation requested",
                }
            ),
            200,
        )
    except Exception as e:
        # e.g., "No running job to cancel."
        return jsonify({"error": str(e)}), 400
