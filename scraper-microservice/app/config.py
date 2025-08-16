import os

REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "nba scrape agent")

GCP_BUCKET = "nba-datalake"
RESULTS_PREFIX = "reddit_scrapes"
CHUNK_ROWS = 200
