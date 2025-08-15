from flask import Flask
from .routes import scrape_bp


def create_app() -> Flask:
    app = Flask(__name__)
    app.register_blueprint(scrape_bp)
    return app
