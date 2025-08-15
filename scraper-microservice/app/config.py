import os

REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "nba scrape agent")

# CHECKPOINT_PATH = os.getenv("SCRAPE_CHECKPOINT", "scrape_checkpoint.pkl")
# CHECKPOINT_EVERY_N_ROWS = int(os.getenv("SCRAPE_CHECKPOINT_EVERY_N_ROWS", "50"))

GCP_BUCKET = "nba-datalake"
RESULTS_PREFIX = "reddit_scrapes"
CHUNK_ROWS = 200
