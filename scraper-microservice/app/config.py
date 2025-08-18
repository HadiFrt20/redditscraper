import os

REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "nba scrape agent")

GCP_BUCKET = "nba-datalake"
RESULTS_PREFIX = "reddit_scrapes"
CHUNK_ROWS = 200


RATELIMIT_SECONDS = 600  # 10 minutes
# Max retries we’ll attempt for transient API errors (429/5xx).
MAX_RETRIES = 6
# Upper bound for our own sleeps in case headers/messages are missing.
MAX_BACKOFF_SECONDS = 600  # 10 minutes
