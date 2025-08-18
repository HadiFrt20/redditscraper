# Reddit NBA Scraper

[![CI/CD](https://github.com/HadiFrt20/redditscraper/actions/workflows/ci.yml/badge.svg)](https://github.com/HadiFrt20/redditscraper/actions/workflows/ci.yml)

A Flask microservice for scraping NBA player mentions on Reddit using **Async PRAW**.  
It runs long-lived scrape jobs, tracks progress, writes results to **Google Cloud Storage**, and exposes a simple HTTP API.

Designed to run on **Google App Engine (Standard, Python 3.12)**. Works locally and in Docker.

---

## ✨ Features

- Health & readiness: `/_ah/health`, `/health`
- Start / monitor / pause / resume / cancel a single running scrape job
- Progress endpoint with units, percent, and current player index
- GCS-backed streaming CSV output (header + part files + final composed CSV)
- Download CSV directly or via time-limited **signed URL**
- Default player list from `players_names.csv` (fallback)
- **Rate-limit aware** Reddit access (Async PRAW)
- **Checkpointing**: job state persisted to GCS so you can resume after restarts
- CI: tests + lint + formatting on GitHub Actions
- One-click deploy (GAE) via GitHub Actions

---

## 🧱 Project Structure

```
scraper-microservice/
├─ app/
│  ├─ __init__.py   # Flask factory (registers routes)
│  ├─ routes.py     # HTTP endpoints
│  ├─ manager.py    # ScrapeManager (state, progress, checkpointing, GCS IO)
│  ├─ scraper.py    # Async PRAW scraper
│  ├─ utils.py      # helpers (slugify, read players CSV)
│  ├─ gcs_io.py     # thin GCS wrappers (upload, exists, compose, list, download)
│  └─ config.py     # configuration (env + static constants)
├─ tests/           # pytest suite + fakes (GCS, scraper)
├─ players_names.csv # default fallback player list
├─ wsgi.py          # gunicorn entrypoint: wsgi:app
├─ requirements.txt
├─ Dockerfile
├─ Makefile
└─ README.md
```

---

## ⚙️ Configuration

Only Reddit credentials are environment-driven. GCS details are static in `app/config.py`.

```python
# app/config.py
import os

REDDIT_CLIENT_ID     = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT    = os.getenv("REDDIT_USER_AGENT", "nba scrape agent")

# Static (edit as you wish):
GCP_BUCKET     = "nba-datalake"
RESULTS_PREFIX = "reddit_scrapes"
CHUNK_ROWS     = 200  # rows per part file before flushing to GCS
```

**Required env vars (local & prod):**

- `REDDIT_CLIENT_ID`  
- `REDDIT_CLIENT_SECRET`  
- `REDDIT_USER_AGENT`  

For local access to GCS (one of the following):

- `GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json`
- Or mount ADC from gcloud CLI: `~/.config/gcloud/application_default_credentials.json`

Optionally:  
`GOOGLE_CLOUD_PROJECT=<your-project-id>`

---

## 🛠️ Local Development

### Prerequisites
- Python 3.12
- (Optional) Google Cloud SDK (for ADC)
- (Optional) Docker

### Setup

```bash
cd scraper-microservice

python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

pip install -U pip
pip install -r requirements.txt
```

Export env vars:

```bash
export REDDIT_CLIENT_ID="..."
export REDDIT_CLIENT_SECRET="..."
export REDDIT_USER_AGENT="nba scrape agent"

# If using ADC:
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.config/gcloud/application_default_credentials.json"
export GOOGLE_CLOUD_PROJECT="nbadatalake"
```

### Run (gunicorn)

```bash
gunicorn -w 1 -k gthread -b :8080 wsgi:app
```

Health check:

```bash
curl -s localhost:8080/health
```

---

## 🐳 Docker

### Build
```bash
docker build -t reddit-scraper .
```

### Run
```bash
docker run --rm -p 8080:8080   -e REDDIT_CLIENT_ID="..."   -e REDDIT_CLIENT_SECRET="..."   -e REDDIT_USER_AGENT="nba scrape agent"   -e GOOGLE_CLOUD_PROJECT="nbadatalake"   -v "$HOME/.config/gcloud/application_default_credentials.json:/secrets/adc.json:ro"   -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/adc.json   reddit-scraper
```

---

## 🧪 Tests, Linting, Formatting

Use the **Makefile** shortcuts:

```bash
make deps     # upgrade pip + install deps
make test     # pytest (with fakes for GCS + scraper)
make lint     # ruff lint
make format   # black + ruff --fix
make check    # black --check + ruff
```

All tests run without real GCS/Reddit thanks to in-memory fakes and monkeypatching.

---

## 🌐 API

Base URL locally: `http://localhost:8080`  
On App Engine: `https://<PROJECT-ID>.<REGION>.r.appspot.com`

### Health & Readiness
- `GET /_ah/health` → App Engine readiness (200 plain text)
- `GET /health` → `{ "status": "ok" }`

### Home
- `GET /` → simple HTML page

### Start a Job
`POST /scrape`  

Body example:
```json
{
  "players": ["LeBron James", "Nikola Jokic"],
  "subreddits": ["nba", "nbadiscussion"],
  "search_limit": 2,
  "time_filter": "year",
  "sort": "new"
}
```

Responses:
- `202 {"status":"accepted","message":"Job started"}`
- `409 {"status":"busy", ...}`

### Progress
`GET /scrape/progress` → JSON:
```json
{
  "status": "running",
  "message": "text",
  "total_units": 1282,
  "completed_units": 39,
  "percent": 3.04,
  "current_player_index": 19
}
```

### Pause / Resume / Cancel
- `POST /scrape/pause`
- `POST /scrape/resume`
- `POST /scrape/cancel`

### Results List
`GET /scrape/results` → per-player info:
```json
{
  "status": "running",
  "job_id": "job-2025-08-16T13-31-41",
  "job_prefix": "reddit_scrapes/job-2025-08-16T13-31-41/",
  "files": [
    {
      "player": "LeBron James",
      "slug": "lebron-james",
      "final_blob": "reddit_scrapes/job-.../lebron-james.csv",
      "parts": 3
    }
  ]
}
```

### Download CSV for a Player
- `GET /scrape/results/<slug>.csv` → streams CSV content  
- `GET /scrape/results/<slug>.url` → returns signed URL

---

## 🧠 How It Works

### Streaming CSV to GCS
Each player slug produces:
- `.../<slug>/header.csv`
- `.../<slug>/part-00001.csv`, `part-00002.csv`, …
- Final composed CSV: `.../<slug>.csv`

### Checkpointing
Writes JSON checkpoints to:
```
gs://<bucket>/checkpointing/<job_id>.json
```

### Rate Limiting
- Async PRAW handles Reddit headers
- Exponential backoff on transient errors

---

## ☁️ Deploying to Google App Engine

GitHub Actions workflow:
- Installs deps, runs tests, lints, formats
- Deploys with App Engine

Secrets required:
- `GCP_PROJECT`
- `GCP_APP_REGION`
- `GCP_SA_KEY`
- `REDDIT_CLIENT_ID`
- `REDDIT_CLIENT_SECRET`

Generated `app.yaml`:
```yaml
runtime: python312
entrypoint: gunicorn -w 1 -k gthread -b :$PORT wsgi:app

env_variables:
  REDDIT_CLIENT_ID: "${{ secrets.REDDIT_CLIENT_ID }}"
  REDDIT_CLIENT_SECRET: "${{ secrets.REDDIT_CLIENT_SECRET }}"
  REDDIT_USER_AGENT: "nba scrape agent"

automatic_scaling:
  min_instances: 0
  max_instances: 2
  target_cpu_utilization: 0.65
  target_throughput_utilization: 0.7
```

---

## 🔬 Postman Collection

Available at:
```
postman/NBA Scraper.postman_collection.json
```

Includes:
- `GET /_ah/health`
- `GET /health`
- `POST /scrape`
- `GET /scrape/progress`
- `POST /scrape/pause|resume|cancel`
- `GET /scrape/results`
- `GET /scrape/results/:slug.csv`
- `GET /scrape/results/:slug.url`

---

## 🧩 Troubleshooting

- **503 on first request in App Engine**  
  Ensure `gunicorn` is in `requirements.txt` and health endpoints are fast.

- **409 “A job is already running.”**  
  Only one job at a time is supported.

- **“Default credentials were not found.”**  
  Set `GOOGLE_APPLICATION_CREDENTIALS` locally or configure GitHub Secrets.

- **Reddit 404s or empty results**  
  Check subreddit names and reduce `search_limit`.

- **Rate-limit exceptions**  
  Tune `ratelimit_seconds` or reduce frequency.

---
