# Supabase Database Setup

## 1) Create a Supabase Postgres database
- Go to Supabase and create a project.
- In the project, open **Settings → Database**.

## 2) Use the Connection Pooler (required for Streamlit Cloud)
Streamlit Cloud does **not** support IPv6. Supabase's direct connection uses IPv6 and will fail with "Cannot assign requested address".

**Use the Connection Pooler string instead:**
- In Supabase: **Settings → Database**
- Under "Connection string", select **"Use connection pooler"**
- Copy the URI (Session mode, port 5432, or Transaction mode, port 6543)
- It looks like: `postgresql://postgres.PROJECT_REF:PASSWORD@aws-0-REGION.pooler.supabase.com:6543/postgres`

## 3) Configure Streamlit Cloud secrets
Set in Streamlit Cloud **Secrets**:

```
SUPABASE_DB_URL = "postgresql://postgres.PROJECT_REF:PASSWORD@aws-0-REGION.pooler.supabase.com:6543/postgres"
```

Replace with your actual pooler URI from step 2.

## 4) Scheduler (external)
Use a scheduler outside Streamlit Cloud (GitHub Actions, cron, etc.) to run:

```
python scripts/refresh_pipeline.py --log-json --interval-hours 3
```

This will refresh the Supabase database that all Streamlit Cloud users share.
