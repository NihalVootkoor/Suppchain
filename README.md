# Automotive Supply Chain Risk Monitor

A Streamlit dashboard that monitors automotive supply chain disruption events in real time. It ingests curated RSS feeds, validates and enriches events with LLM-powered analysis, scores risk, and surfaces the top threats with actionable mitigation guidance.

Built by Nihal Vootkoor.

## Features

- **Command Center** — KPIs, top-3 high-risk events, and AI-generated mitigation playbooks
- **AI-Powered Mitigation** — Groq LLM-personalized response actions for the highest-risk events
- **All Events** — Filterable explorer across all enriched supply chain events
- **8-stage pipeline** — RSS ingestion → deduplication → hard filtering → LLM validation → enrichment → scoring → aggregation → mitigation
- **Dual storage** — SQLite for local dev, PostgreSQL (Supabase) for cloud deployment
- **Scheduled refresh** — Daily GitHub Actions workflow keeps data current

## Quick Start

### Prerequisites

- Python 3.11+
- A [Groq API key](https://console.groq.com) (free tier works)

### Local Setup

```bash
git clone <repo-url>
cd AutoSupplyChain
pip install -r requirements.txt
```

Create `.streamlit/secrets.toml`:
```toml
GROQ_API_KEY = "your_groq_api_key_here"
```

Run the app:
```bash
streamlit run app.py
```

The app uses SQLite by default (`data/app.db`). Click **Refresh** in the sidebar to ingest the latest RSS feeds.

## Cloud Deployment (Streamlit Cloud)

1. Fork this repo and connect it to [Streamlit Cloud](https://streamlit.io/cloud)
2. In Streamlit Cloud → **Secrets**, add:
   ```toml
   GROQ_API_KEY = "your_groq_api_key_here"
   SUPABASE_DB_URL = "your_supabase_connection_string"
   ```
3. In your GitHub repo → **Settings → Secrets**, add `SUPABASE_DB_URL` for the daily refresh Action

See [docs/SUPABASE.md](docs/SUPABASE.md) for Supabase setup details.

## Architecture

```
RSS Feeds → Ingest → Dedupe → Hard Filter → LLM Validate
         → Enrich → Score → Store → Aggregate → Mitigate
```

**Data sources:** Supply Chain Dive, DC Velocity, AutomotiveWorld, Automotive News, and more
**LLM:** Groq `llama-3.3-70b-versatile` for event validation, extraction, and mitigation
**Risk taxonomy:** 7 PESTEL + Operational categories, 10 disruption types, 200+ geo entries

## Running Tests

```bash
pytest tests/ -v
```

118 tests covering URL utils, date parsing, config validation, hard filtering, LLM extraction, scoring, storage, RSS ingestion, and scheduling.

## Project Structure

```
app.py                    # Streamlit entrypoint
pages/                    # Multi-page app pages
src/                      # Core pipeline modules
  config.py               # Constants and taxonomies
  rss_ingest.py           # RSS fetch and pipeline orchestration
  filters.py              # Hard filtering (automotive + disruption keywords)
  llm_extract.py          # LLM validation and structured extraction
  scoring.py              # Risk score computation
  mitigation.py           # Mitigation playbooks + LLM humanization
  storage.py              # SQLite/PostgreSQL abstraction
  aggregation.py          # KPI and dashboard aggregation
scripts/                  # Utility and refresh scripts
tests/                    # pytest test suite
docs/                     # PRD and setup guides
.github/workflows/        # CI: daily refresh + test suite on push/PR
```

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `GROQ_API_KEY` | Yes | Groq API key for LLM validation and mitigation |
| `SUPABASE_DB_URL` | Cloud only | PostgreSQL connection string (Supabase pooler) |
| `GROQ_MODEL` | No | Override LLM model (default: `llama-3.3-70b-versatile`) |
| `REFRESH_INTERVAL_HOURS` | No | Refresh interval for logging (default: `24`) |
