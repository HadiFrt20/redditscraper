# app/routes.py
import io
from flask import Blueprint, jsonify

scrape_bp = Blueprint("scrape", __name__)

@scrape_bp.get("/health")
def health():
    return jsonify({"status": "ok"})

@scrape_bp.get("/")
def home():
    return f"Hello there"

