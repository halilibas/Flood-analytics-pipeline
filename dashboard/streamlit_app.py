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
    layout = "wide",
)

DATA_DIR = Path(__file__).parent / "data"

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

@st.cache_data
def load_cycle_time_histogram() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / "cycle_time_histogram.csv")

@st.cache_data
def load_coastal_vs_inland() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / "coastal_vs_inland.csv")

@st.cache_data
def load_damage_by_cause() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / "damage_by_cause_category.csv")

@st.cache_data
def load_region_x_event() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / "region_x_event_type.csv")

# Page header

st.title("Flood Claims Analytics Pipeline")
st.caption(
"End-to-end DE pipeline over FEMA NFIP claims + synthetic policy data. "
"Bronze → Silver → Gold star schema in Delta Lake. "
"Source: [github.com/halilibas/Flood-analytics-pipeline](https://github.com/halilibas/Flood-analytics-pipeline)"
)

# Top KPI strip

events = load_top_events()
total_paid = events["total_paid_billions"].sum()
total_claims = events["claim_count"].sum()
biggest_event = events.iloc[0]

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Records Modeled", "2.72M")
col2.metric("Dimensions", "6")
col3.metric("CAT Events Tracked", "192")
col4.metric("Top Event Payout", "$16.3B", help="Hurricane Katrina — matches published NFIP")
col5.metric("Stack Depth", "Bronze → Silver → Gold")

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

st.divider()
st.header("Star Schema Analytics")
st.caption(
    "Charts below use the full v1 star schema — six dimensions with SCD Type 2 on policy "
    "and customer, role-playing date dimensions for claim lifecycle, and Kimball UNKNOWN "
    "sentinel row for orphan geography FKs. Each query joins fact_claims to 2-4 dimensions."
)

# Chart 5: Cycle time distribution from synthesized lifecycle dates
st.subheader("Claim Cycle Time Distribution")
st.caption(
    "Distribution of days from claim filing to closure across all 2.72M claims. "
    "Lifecycle dates were synthesized deterministically from `dateOfLoss` "
    "(FEMA doesn't provide them) — the resulting distribution is uniform between "
    "30 and 365 days by design. The chart demonstrates that the role-playing date "
    "dimensions in fact_claims v1 (`date_filed_key`, `date_closed_key`) "
    "produce measurable cycle-time analytics; real historical dates would show "
    "the actual claim-processing shape."
)

cycle = load_cycle_time_histogram()
fig_cycle = px.bar(
    cycle,
    x="days_filed_to_closed",
    y="n_claims",
    labels={
        "days_filed_to_closed": "Days from filing to closure",
        "n_claims": "Number of claims",
    },
)
fig_cycle.update_traces(marker_color="#1e40af")
fig_cycle.update_layout(
    height=400,
    showlegend=False,
    bargap=0.05,
    margin=dict(l=60, r=20, t=20, b=60),  
)
st.plotly_chart(fig_cycle, use_container_width=True)


# Chart 6: Coastal vs Inland 
st.subheader("Coastal vs Inland Claim Severity")
st.caption(
    "Enabled by dim_geography's `is_coastal` domain attribute. One-line filter to compare "
    "loss profiles between coastal and inland flood exposure. Note: coastal here means any "
    "ocean or Great Lakes border, not hurricane-exposed states only."
)

coastal = load_coastal_vs_inland()
coastal["exposure"] = coastal["is_coastal"].map({True: "Coastal", False: "Inland"})

col_a, col_b = st.columns(2)
with col_a:
    fig_coastal_severity = px.bar(
        coastal,
        x="exposure",
        y="avg_severity",
        labels={"exposure": "", "avg_severity": "Avg claim severity ($)"},
        text="avg_severity",
        title="Average claim severity",
        color="exposure",
        color_discrete_map={"Coastal": "#1e40af", "Inland": "#93c5fd"},
    )
    fig_coastal_severity.update_traces(textposition="outside", texttemplate="$%{text:,.0f}")
    fig_coastal_severity.update_layout(showlegend=False)
    st.plotly_chart(fig_coastal_severity, use_container_width=True)

with col_b:
    fig_coastal_total = px.pie(
        coastal,
        names="exposure",
        values="total_paid_billions",
        title="Total paid ($B) by exposure",
        color="exposure",
        color_discrete_map={"Coastal": "#1e40af", "Inland": "#93c5fd"},
        hole=0.4,
    )
    st.plotly_chart(fig_coastal_total, use_container_width=True)

# Chart 7: Regional CAT event impact 
st.subheader("CAT Event Impact by Region")
st.caption(
    "3-dimension join: fact_claims × dim_geography × dim_cat_event. Shows how named storms "
    "concentrate impact in the Southeast."
)

region_event = load_region_x_event()
fig_region = px.bar(
    region_event,
    x="region",
    y="total_paid_billions",
    color="event_type",
    labels={
        "region": "Region",
        "total_paid_billions": "Total paid ($B)",
        "event_type": "Event type",
    },
    barmode="group",
    text="total_paid_billions",
)
fig_region.update_traces(textposition="outside", texttemplate="$%{text}B")
fig_region.update_layout(height=450)
fig_region.update_yaxes(
    type="log",
    showgrid=True,
    minor=dict(showgrid=False),
)
st.plotly_chart(fig_region, use_container_width=True)

# Chart 8: Damage by cause category 
st.subheader("Cause of Damage: Analytical Rollup")
st.caption(
    "Uses `bronze.ref_cause_of_damage` reference table with `cause_category` rollup "
    "(Coastal / Inland Flooding / Rainfall / Erosion / Earth Movement / Expedited)."
)

cause = load_damage_by_cause()
fig_cause = px.bar(
    cause.sort_values("total_paid_billions", ascending=True),
    x="total_paid_billions",
    y="cause_category",
    orientation="h",
    labels={
        "total_paid_billions": "Total paid ($B)",
        "cause_category": "",
    },
    text="total_paid_billions",
)
fig_cause.update_traces(textposition="outside", texttemplate="$%{text}B")
fig_cause.update_layout(height=400)
st.plotly_chart(fig_cause, use_container_width=True)

# Footer

st.divider()
st.caption(
    "Architecture: Databricks · PySpark · Delta Lake · dbt · "
    "Airflow (Week 4) · Streamlit. "
    "Data: real FEMA NFIP claims (2.7M+) joined to deterministic synthetic policies."
)