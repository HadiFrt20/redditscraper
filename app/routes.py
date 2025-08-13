# app/routes.py
import io
from flask import Blueprint, jsonify, request
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
    return f"Hello there"

# NEW: start job
@scrape_bp.post("/scrape")
def start_scrape():
    data = request.get_json(silent=True) or {}
    players = data.get("players")
    if not players:
        players = players_from_csv(PLAYERS_CSV_PATH)[1:2]

    try:
        # subreddit/search args ignored by the simulator for now; kept for API shape
        MANAGER.start(
            players=players,
            subreddit=data.get("subreddit", DEFAULT_SUBREDDIT),
            search_limit=data.get("search_limit", None),
            time_filter=data.get("time_filter", "year"),
            sort=data.get("sort", "new"),
        )
        return jsonify({"status": "accepted", "message": "Job started"}), 202
    except RuntimeError as e:
        return jsonify({"status": "busy", "message": str(e)}), 409
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# NEW: progress
@scrape_bp.get("/scrape/progress")
def scrape_progress():
    with MANAGER.lock:
        pct = (MANAGER.completed_units / MANAGER.total_units * 100.0) if MANAGER.total_units else 0.0
        return jsonify({
            "status": MANAGER.status,
            "message": MANAGER.message,
            "total_units": MANAGER.total_units,
            "completed_units": MANAGER.completed_units,
            "percent": round(pct, 2),
            "current_player_index": MANAGER.current_player_index,
        })