# Flood Claims Analytics Pipeline

> Work in progress - May 2026 - Present

An end-to-end data engineering project using FEMA NFIP flood claims data and synthetic policy, customer, and agent data.

The goal is to build a small but realistic insurance analytics pipeline: ingest raw claims data, clean and standardize it through bronze and silver layers, model business-ready tables in a gold layer, and surface claims KPIs in a Streamlit dashboard.

## What's Working Week 2

End-to-end pipeline: FEMA NFIP raw → Bronze Delta → Silver typed/cleaned → Gold star schema → Streamlit dashboard.

[Dashboard](docs/dashboard_screenshot.png)

**KPI validation** — total payouts by event match published NFIP industry figures within ~5%:

| Event | Project total | Published NFIP | Δ |
|---|---|---|---|
| Hurricane Katrina | $16.26B | ~$16.3B | <1% |
| Hurricane Sandy | $8.96B | ~$8.5B | ~5% |
| Hurricane Harvey | $9.06B | ~$9B | ~1% |

**Domain insight surfaced by the model** — hurricane-season claims (Jun–Nov) average $40,769 vs $14,641 off-season (2.8x severity). The query is one line because `hurricane_season_flag` is a domain attribute on `dim_date`.

### Tables built

| Layer | Table | Rows |
|---|---|---|
| Bronze | `bronze.fema_claims_raw` | 2,721,780 |
| Bronze | `bronze.synthetic_{agents,customers,policies}_raw` | 75 / 2.72M / 2.72M |
| Silver | `silver.claims_clean` | 2,721,780 |
| Silver | `silver.claims_enriched` | 2,721,780 |
| Gold | `gold.dim_date` | 22,280 |
| Gold | `gold.fact_claims` v0 | 2,721,780 (partitioned by loss_year) |


# Architecture

_Diagram coming soon._

**Data flow:** External sources → Bronze (Delta) → Silver (Delta) → Gold (dbt marts) → Streamlit

## Stack

Current / planned stack:

- **Compute / storage:** Databricks, Delta Lake
- **Transformation:** PySpark, dbt
- **Orchestration:** Apache Airflow
- **Quality:** dbt tests
- **Visualization:** Streamlit

## Why this domain

I chose insurance claims because the data has enough real-world complexity to make the project useful: claim amounts, dates, locations, catastrophe events, policy attributes, and historical changes.

The project uses flood insurance as the example domain, but the engineering patterns are general: raw ingestion, medallion architecture, dimensional modeling, incremental processing, orchestration, testing, and dashboarding.

## Data Sources

- [FEMA NFIP Redacted Claims](https://www.fema.gov/openfema-data-page/fima-nfip-redacted-claims-v2) - public flood insurance claims data
- Synthetic policies, customers, and agents - generated to support policy-level analytics and dimensional modeling

## Business Questions

- How do claim frequency and severity vary by state, flood zone, and property type?
- How do major catastrophe events affect claim volume and paid losses?
- What is the average claim cycle time?
- Which regions or policy segments show unusual claim patterns?
- What is the estimated loss ratio by state or policy segment?