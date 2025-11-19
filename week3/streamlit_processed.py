import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import os

st.set_page_config(page_title="HPC Cluster Dashboard", layout="wide")

# ---------------- PATH RESOLUTION ----------------
def get_csv_path():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(script_dir, "processed_logs.csv"),
        os.path.join(script_dir, "..", "processed_logs.csv"),
        "processed_logs.csv",
    ]
    for p in candidates:
        if os.path.exists(p):
            return p

    st.error("❌ processed_logs.csv not found!")
    st.stop()

# ---------------- DATA LOAD ----------------
@st.cache_data(ttl=300)
def load_data():
    df = pd.read_csv(get_csv_path())
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return df

df = load_data()

# ---------------- SIDEBAR FILTERS ----------------
with st.sidebar:
    st.title("🖥 HPC Cluster Dashboard")
    st.markdown("---")

    years = sorted(df["timestamp"].dt.year.unique(), reverse=True)
    selected_year = st.selectbox("Year", ["All"] + [str(y) for y in years])

    selected_month = "All"
    selected_day = "All"

    if selected_year != "All":
        months = sorted(df[df["timestamp"].dt.year == int(selected_year)]["timestamp"].dt.month.unique())
        selected_month = st.selectbox("Month", ["All"] + [f"{m:02d}" for m in months])

        if selected_month != "All":
            days = sorted(df[
                (df["timestamp"].dt.year == int(selected_year)) &
                (df["timestamp"].dt.month == int(selected_month))
            ]["timestamp"].dt.day.unique())
            selected_day = st.selectbox("Day", ["All"] + [f"{d:02d}" for d in days])

# ---------------- FILTER DATA ----------------
filtered = df.copy()

if selected_year != "All":
    filtered = filtered[filtered["timestamp"].dt.year == int(selected_year)]
if selected_month != "All":
    filtered = filtered[filtered["timestamp"].dt.month == int(selected_month)]
if selected_day != "All":
    filtered = filtered[filtered["timestamp"].dt.day == int(selected_day)]

if filtered.empty:
    st.warning("No data for selected filters.")
    st.stop()

# ---------------- PRECOMPUTE EVERYTHING (FAST) ----------------
last = filtered.iloc[-1]
prev = filtered.iloc[-2] if len(filtered) > 1 else last

def pct_delta(curr, prev):
    if prev == 0:
        return "0%"
    return f"{((curr - prev) / prev) * 100:+.1f}%"

metrics = {
    "cpu": {
        "value": last.get("cpu_percent", 0),
        "delta": last.get("cpu_percent", 0) - prev.get("cpu_percent", 0),
        "pct": pct_delta(last.get("cpu_percent", 0), prev.get("cpu_percent", 0))
    },
    "node": {
        "value": last.get("node_utilization", 0),
        "delta": last.get("node_utilization", 0) - prev.get("node_utilization", 0),
        "pct": pct_delta(last.get("node_utilization", 0), prev.get("node_utilization", 0))
    },
    "run": {
        "value": last.get("jobs_running", 0),
        "delta": last.get("jobs_running", 0) - prev.get("jobs_running", 0),
        "pct": pct_delta(last.get("jobs_running", 0), prev.get("jobs_running", 0))
    },
    "queued": {
        "value": last.get("jobs_queued", 0),
        "delta": last.get("jobs_queued", 0) - prev.get("jobs_queued", 0),
        "pct": pct_delta(last.get("jobs_queued", 0), prev.get("jobs_queued", 0))
    },
}

# Efficiency
if "cpu_percent" in filtered and "node_utilization" in filtered:
    filtered["efficiency"] = (filtered["cpu_percent"] * filtered["node_utilization"]) / 100


# ---------------- PLOTTING FUNCTION ----------------
def fast_plot(df, cols, title):
    fig = go.Figure()
    for col in cols:
        if col in df:
            fig.add_trace(go.Scatter(
                x=df["timestamp"], y=df[col],
                mode="lines", name=col.replace("_", " ").title()
            ))
    fig.update_layout(
        title=title, template="plotly_white",
        height=400, hovermode="x unified"
    )
    return fig


# ---------------- DASHBOARD RENDER ----------------
st.title("📊 HPC Cluster Resource Monitoring Dashboard")

st.markdown(
    f"**Data range:** {df['timestamp'].min()} → {df['timestamp'].max()}  
    **Filtered Rows:** {len(filtered):,}"
)

# --- Latest Metrics ---
st.subheader("🔥 Latest Snapshot")

c1, c2, c3, c4 = st.columns(4)

c1.metric("CPU Usage", f"{metrics['cpu']['value']}%", f"{metrics['cpu']['delta']} ({metrics['cpu']['pct']})")
c2.metric("Node Utilization", f"{metrics['node']['value']}%", f"{metrics['node']['delta']} ({metrics['node']['pct']})")
c3.metric("Jobs Running", metrics["run"]["value"], f"{metrics['run']['delta']} ({metrics['run']['pct']})")
c4.metric("Jobs Queued", metrics["queued"]["value"], f"{metrics['queued']['delta']} ({metrics['queued']['pct']})")

# --- Gauges ---
st.subheader("🎯 Utilization Gauges")

g1, g2 = st.columns(2)

with g1:
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=metrics["cpu"]["value"],
        delta={'reference': prev.get("cpu_percent", 0)},
        gauge={'axis': {'range': [0, 100]}},
        title={'text': "CPU Utilization %"}
    ))
    st.plotly_chart(fig, use_container_width=True)

with g2:
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=metrics["node"]["value"],
        delta={'reference': prev.get("node_utilization", 0)},
        gauge={'axis': {'range': [0, 100]}},
        title={'text': "Node Utilization %"}
    ))
    st.plotly_chart(fig, use_container_width=True)

# --- Plots ---
st.subheader("📈 Job Queue Trends")
st.plotly_chart(fast_plot(filtered, ["jobs_running", "jobs_queued", "jobs_held", "jobs_exiting"], "Job Trends"))

st.subheader("🖥 Node States")
st.plotly_chart(fast_plot(filtered, ["nodes_running", "nodes_offline", "nodes_down", "nodes_idle", "nodes_total"], "Node States"))

if "efficiency" in filtered:
    st.subheader("⚡ Cluster Efficiency")
    st.plotly_chart(fast_plot(filtered, ["efficiency"], "Efficiency"))

# --- Raw Data ---
with st.expander("📄 View Raw Data (last 1000 rows)"):
    st.dataframe(filtered.tail(1000))
