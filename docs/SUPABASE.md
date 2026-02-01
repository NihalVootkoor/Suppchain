# Supabase Database Setup

## 1) Create a Supabase Postgres database
- Go to Supabase and create a project.
- In the project, open **Settings → Database** and copy the connection string.

## 2) Configure Streamlit Cloud secrets
Set one of the following in Streamlit Cloud **Secrets**:

```
SUPABASE_DB_URL = "postgresql://user:pass@host:port/dbname"
```

You can also use `DATABASE_URL` if you prefer.

## 3) Scheduler (external)
Use a scheduler outside Streamlit Cloud (GitHub Actions, cron, etc.) to run:

```
python scripts/refresh_pipeline.py --log-json --interval-hours 3
```

This will refresh the Supabase database that all Streamlit Cloud users share.
