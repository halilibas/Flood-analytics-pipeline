"""
Flood Claims Analytics Dashboard - Streamlit demo.

Reads aggregated CSVs exported from gold.fact_claims (Databricks).
Designed so the read source can be swapped to live Databricks SQL connection
without changing chart code. 
"""

from pathlib import Path
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(
    page_title = "Flood Claims Analytics Pipeline",
    page_icon = "🌊",
    layout = "wide",
)

DATA_DIR = Path(__file__).parent / "sample_data"

# Data loading

@st.cache_data
def load_claims_by_year() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / "claims_by_year.csv")

@st.cache_data
def load_top_events() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / "top_flood_events.csv")

@st.cache_data
def load_claims_by_state() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / "claims_by_state.csv")

@st.cache_data
def load_season_comparison() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / "season_comparison.csv")

# Page header

st.title("Flood Claims Analytics Pipeline")
st.caption(
"End-to-end DE pipeline over FEMA NFIP claims + synthetic policy data. "
"Bronze → Silver → Gold star schema in Delta Lake. "
"Source: [github.com/halilibas/insurance-claims-pipeline](https://github.com/halilibas/Flood-analytics-pipeline)"
)

# Top KPI strip

events = load_top_events()
total_paid = events["total_paid_billions"].sum()
total_claims = events["claim_count"].sum()
biggest_event = events.iloc[0]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Records Modeled", "2.72M")
col2.metric("CAT Events Tracked", f"{len(events):,}")
col3.metric("Top Event Payout", f"${biggest_event['total_paid_billions']:.1f}B",
                help=f"{biggest_event['flood_event_name']}"
            )
col4.metric("Pipeline Layers", "Bronze -> Silver -> Gold")

st.divider()

# Chart 1: Claims paid by year

st.subheader("Total Claims Paid by Year")
st.caption("Validates against published NFIP industry totals. Spikes align with Katrina (2005), Sandy (2012), Harvey (2017), Helene (2024).")

yearly = load_claims_by_year()
fig_yearly = px.bar(
    yearly,
    x="year",
    y="total_paid_billions",
    labels={"year": "Year of loss", "total_paid_billions": "Total paid ($B)"},
    text="total_paid_billions",
)
fig_yearly.update_traces(textposition="outside", texttemplate="$%{text}B")
fig_yearly.update_layout(showlegend=False, height=400)
st.plotly_chart(fig_yearly, use_container_width=True)

# Chart 2: Top CAT events

st.subheader("Top Catastrophe Events by Total Paid")
fig_events = px.bar(
    events.sort_values("total_paid_billions"),
    x="total_paid_billions",
    y="flood_event_name",
    orientation="h",
    labels={"total_paid_billions": "Total paid ($B)", "flood_event_name": ""},
    text="total_paid_billions",
)
fig_events.update_traces(textposition="outside", texttemplate="$%{text}B")
fig_events.update_layout(showlegend=False, height=500)
st.plotly_chart(fig_events, use_container_width=True)

# Chart 3: Geographic concentration

st.subheader("Claim Concentration by State")
states = load_claims_by_state().head(15)
fig_states = px.bar(
    states,
    x="state",
    y="total_paid_billions",
    labels={"state": "State", "total_paid_billions": "Total paid ($B)"},
    color="total_paid_billions",
    color_continuous_scale="Blues",
)
fig_states.update_layout(coloraxis_showscale=False, height=400)
st.plotly_chart(fig_states, use_container_width=True)

# Chart 4: Hurricane season insight

st.subheader("Hurricane Season vs Off-Season")
st.caption("Hurricane season (Jun–Nov) claims are far more severe. This insight comes from a one-line query against gold.dim_date.hurricane_season_flag — a domain attribute baked into the calendar dimension.")

season = load_season_comparison()
col_a, col_b = st.columns(2)
with col_a:
    fig_season_total = px.pie(
        season,
        names="season",
        values="total_paid_billions",
        title="Total paid ($B)",
        hole=0.45,
    )
    st.plotly_chart(fig_season_total, use_container_width=True)
with col_b:
    fig_season_avg = px.bar(
        season,
        x="season",
        y="avg_severity",
        labels={"season": "", "avg_severity": "Avg claim severity ($)"},
        text="avg_severity",
        title="Average claim severity",
    )
    fig_season_avg.update_traces(textposition="outside", texttemplate="$%{text:,.0f}")
    fig_season_avg.update_layout(showlegend=False)
    st.plotly_chart(fig_season_avg, use_container_width=True)

# Footer

st.divider()
st.caption(
    "Architecture: Databricks · PySpark · Delta Lake · dbt · "
    "Airflow (Week 4) · Streamlit. "
    "Data: real FEMA NFIP claims (2.7M+) joined to deterministic synthetic policies."
)