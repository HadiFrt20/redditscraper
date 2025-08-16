# Reddit NBA Scraper

[![CI/CD](https://github.com/HadiFrt20/redditscraper/actions/workflows/ci.yml/badge.svg)](https://github.com/HadiFrt20/redditscraper/actions/workflows/ci.yml)

A Flask-based microservice for scraping NBA player mentions on Reddit.  
It manages scrape jobs, monitors progress, and serves results through a simple API.  
Designed to run on **Google App Engine** with GCS-backed storage.

---

## 🚀 Features
- Health and readiness endpoints (`/_ah/health`, `/health`)
- Start/monitor/pause/resume/cancel scrape jobs
- Download results as CSV or signed GCS URLs
- Progress tracking with percentage complete
- Default players loaded from CSV
- Configurable subreddits, limits, filters, and sorting

---

## 🛠️ Installation & Local Development

### Prerequisites
- Python 3.11+
- Virtual environment
- Google Cloud SDK (if deploying to App Engine)

### Setup
```bash
git clone https://github.com/HadiFrt20/redditscraper.git
cd redditscraper

python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate

pip install -r requirements.txt
