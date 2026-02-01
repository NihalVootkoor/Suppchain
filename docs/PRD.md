# Automotive Supply Chain Risk Monitor (RSS-Only) — Spec

Built by Nihal Vootkoor

## Goal
A Streamlit dashboard that ingests curated RSS feeds, detects automotive supply chain disruption events, validates and enriches them (geo/PESTEL/severity), aggregates insights, and highlights the Top 3 current high-risk events with mitigation guidance. RSS-only. No forecasting.

## Tabs (Option A)
1) Command Center
2) Risk Radar
3) Trends
4) Mitigation Explorer

## Sidebar (Global)
- Refresh data button
- Download PDF report button (prints KPIs + Top 3 events with mitigation)
- Filters:
  - Date range
  - Categories (PESTEL + Operational)
  - Regions
  - Severity range slider (risk_score 0–100)
- Bottom status line:
  - “Currently displaying X events across Y categories in Z regions”
- Debug toggle at bottom showing pipeline counts + rejected reasons sample
- Footer text: “Built by Nihal Vootkoor” (small)

## Data Sources
Curated RSS feeds only. No NewsData.io.

## Storage
Use SQLite or DuckDB.
- raw_articles: retain 30–60 days (automatic cleanup)
- enriched_events: dashboard single source of truth

## Pipeline (must be followed)
1) Ingest RSS -> normalize schema -> store raw_articles
2) Canonicalize URL + hash article_id -> dedupe
3) Hard filter (deterministic):
   - Must have automotive anchor AND disruption trigger
   - Must not have negative keywords (review/MSRP/test drive/etc.)
4) LLM validation + structured extraction (JSON-only):
   - validate supply-chain event
   - extract geo_country/geo_region + geo_confidence
   - classify risk_category (PESTEL + Operational) + disruption_type
   - extract severity signals (impact/probability/time_sensitivity/exposure_proxy)
   - extract delay estimate if possible + confidence/rationale
   - extract entities (OEMs/suppliers/components) + component_criticality
   - create event_summary and reason_flagged
5) Deterministic enrichment:
   - compute risk_score_0to100 from severity signals
   - assign severity_band
   - compute estimated $ exposure at risk (estimated; based on configurable company profile assumptions)
6) Store in enriched_events
7) Aggregation for dashboard:
   - KPIs
   - geo heat map summaries
   - PESTEL breakdowns
   - time series: (a) risk severity over time, (b) event volume over time
   - Top 3 current high-risk events ranked by:
     (1) risk_score, (2) exposure_usd_est, (3) recency
8) Mitigation generation: ONLY Top 3 events in Command Center and PDF export
   - Deterministic playbook by disruption_type/time_sensitivity/component
   - LLM humanizes wording and prioritizes actions; no invention
   - Cache mitigation text in enriched_events

## enriched_events schema (authoritative)
Identity & Metadata:
- event_id (string hash)
- article_url (string)
- source_name (string)
- source_weight (float 0–1)
- published_at (datetime)
- ingested_at (datetime)

Text & Explanation:
- title (string)
- event_summary (string)
- dashboard_blurb (string optional)
- reason_flagged (string)

Automotive Context:
- oem_entities (list[string])
- supplier_entities (list[string])
- component_entities (list[string])
- component_criticality (enum low/medium/high)

Classification:
- risk_category (enum Political/Economic/Social/Technological/Environmental/Legal/Operational)
- disruption_type (enum Labor Strike/Plant Shutdown/Port Congestion/Export Restriction/Cyberattack/Natural Disaster/Supplier Insolvency/Regulatory Change/Other)

Geo:
- geo_country (string)
- geo_region (enum North America/Europe/East Asia/South Asia/Southeast Asia/Middle East/Latin America/Africa)
- geo_confidence (enum High/Medium/Low)

Severity signals:
- impact_1to5 (int)
- probability_1to5 (int)
- time_sensitivity_1to3 (int)
- exposure_proxy_1to5 (int)
- severity_confidence (enum High/Medium/Low)

Computed:
- risk_score_0to100 (float)
- severity_band (enum Low/Medium/High/Critical)

Delay:
- estimated_delay_days (int)
- delay_confidence (enum High/Medium/Low)
- delay_rationale (string)

Exposure:
- exposure_usd_est (float)
- exposure_confidence (enum High/Medium/Low)
- exposure_assumptions (string)

Mitigation (Top 3 only):
- mitigation_description (string)
- mitigation_actions (list[string])
- mitigation_generated_at (datetime)

Debug/audit:
- llm_validation_passed (bool)
- rejected_reason (string|null)

## KPIs (Command Center)
- Total Active Risk Events
- High/Critical Events
- Avg Severity Today
- Delta vs Yesterday Avg Severity
- Avg Estimated Delay (days)
- Total $ Exposure at Risk (Estimated)

Also show: Top 3 Current High-Risk Events with:
- title + link
- event_summary
- “Why this is a risk” (1–2 sentences)
- mitigation actions (Immediate / Near-term / Longer-term)

## Risk Radar
- Global heat map (metric toggle: count / avg severity / $ exposure)
- PESTEL category breakdown (counts + avg severity)

## Trends
- Risk severity over time (avg risk_score by day/week)
- Event volume over time (count by day/week; optionally stacked by category)

## Mitigation Explorer
- Event table with filters and sorting
- No mitigation generation here (view-only)

## Reliability
If refresh fails, dashboard should still load last saved enriched_events.
