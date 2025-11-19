import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import os
from datetime import datetime

# ------------------- PAGE CONFIG -------------------
st.set_page_config(page_title="HPC Cluster Dashboard", layout="wide")

# ------------------- RESOLVE CSV PATH SAFELY -------------------
def get_csv_path():
    # Method 1: Try relative to this script file (works locally + most deployments)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    path1 = os.path.join(script_dir, "processed_logs.csv")
    if os.path.exists(path1):
        return path1

    # Method 2: Try one level up (common when script is in a subfolder like hpcruntime/)
    path2 = os.path.join(script_dir, "..", "processed_logs.csv")
    path2 = os.path.normpath(path2)
    if os.path.exists(path2):
        return path2

    # Method 3: Try current working directory
    path3 = "processed_logs.csv"
    if os.path.exists(path3):
        return path3

    # If not found anywhere → show helpful error
    st.error("""
    ❌ **processed_logs.csv not found!**
    
    We looked in these locations:
    - Same folder as this script
    - One level up (parent folder)
    - Current working directory
    
    **How to fix:**
    - If running locally → place `processed_logs.csv` next to `streamlit.py`
    - If on Streamlit Cloud → go to your app → three dots → Manage app → Upload `processed_logs.csv`
    """)
    st.stop()

# ------------------- LOAD DATA -------------------
@st.cache_data(ttl=300)  # Refresh every 5 minutes
def load_data():
    csv_path = get_csv_path()
    st.success(f"Loaded data from: `{csv_path}`")
    
    df = pd.read_csv(csv_path)
    
    # Clean and parse timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    if df["timestamp"].isna().all():
        st.error("No valid timestamps found in the 'timestamp' column!")
        st.stop()
    
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return df

df = load_data()

# ------------------- SIDEBAR FILTERS -------------------
with st.sidebar:
    st.title("🖥 HPC Cluster Dashboard")
    st.markdown("---")
    st.header("⚙️ Time Filters")

    if len(df) == 0:
        st.warning("No data available")
    else:
        years = sorted(df["timestamp"].dt.year.unique(), reverse=True)
        selected_year = st.selectbox("Year", options=["All"] + [str(y) for y in years])

        selected_month = "All"
        selected_day = "All"

        if selected_year != "All":
            months = sorted(df[df["timestamp"].dt.year == int(selected_year)]["timestamp"].dt.month.unique())
            selected_month = st.selectbox("Month", options=["All"] + [f"{m:02d}" for m in months])

            if selected_month != "All":
                month_int = int(selected_month)
                days = sorted(df[
                    (df["timestamp"].dt.year == int(selected_year)) &
                    (df["timestamp"].dt.month == month_int)
                ]["timestamp"].dt.day.unique())
                selected_day = st.selectbox("Day", options=["All"] + [f"{d:02d}" for d in days])

# ------------------- APPLY FILTERS -------------------
filtered_df = df.copy()

if selected_year != "All":
    filtered_df = filtered_df[filtered_df["timestamp"].dt.year == int(selected_year)]
if selected_month != "All":
    filtered_df = filtered_df[filtered_df["timestamp"].dt.month == int(selected_month)]
if selected_day != "All":
    filtered_df = filtered_df[filtered_df["timestamp"].dt.day == int(selected_day)]

if filtered_df.empty:
    st.warning("No data matches the selected filters.")
    st.stop()

# ------------------- HELPER FUNCTIONS -------------------
def calculate_delta(series):
    if len(series) < 2:
        return 0, "0%"
    current = series.iloc[-1]
    previous = series.iloc[-2]
    delta = current - previous
    percent = (delta / previous * 100) if previous != 0 else 0
    return delta, f"{percent:+.1f}%"

def display_metric(col, title, value, delta=None, delta_percent=None, unit=""):
    with col:
        st.metric(
            label=title,
            value=f"{value:,.0f}{unit}",
            delta=f"{delta:+,.0f} ({delta_percent})" if delta is not None else None
        )

def plot_timeseries(data, columns, title, colors=None):
    if data.empty:
        st.info("No data to display.")
        return
    fig = go.Figure()
    if colors is None:
        colors = ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A", "#19D3F3"]
    
    for i, col in enumerate(columns):
        if col in data.columns:
            fig.add_trace(go.Scatter(
                x=data["timestamp"],
                y=data[col],
                mode="lines+markers",
                name=col.replace("_", " ").title(),
                line=dict(color=colors[i % len(colors)], width=2),
                marker=dict(size=4)
            ))
    
    fig.update_layout(
        title=title,
        xaxis_title="Time",
        yaxis_title="Value",
        hovermode="x unified",
        template="plotly_white",
        height=450,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    st.plotly_chart(fig, use_container_width=True)

# ------------------- MAIN DASHBOARD -------------------
st.title("📊 HPC Cluster Resource Monitoring Dashboard")
st.markdown(f"**Data range:** {df['timestamp'].min().strftime('%Y-%m-%d %H:%M')} → {df['timestamp'].max().strftime('%Y-%m-%d %H:%M')} | **Filtered:** {len(filtered_df):,} rows")

# Latest Metrics Row
st.subheader("🔥 Latest Snapshot")
c1, c2, c3, c4 = st.columns(4)

latest = filtered_df.iloc[-1]

cpu_delta, cpu_pct = calculate_delta(filtered_df["cpu_percent"]) if "cpu_percent" in filtered_df.columns else (0, "0%")
node_delta, node_pct = calculate_delta(filtered_df["node_utilization"]) if "node_utilization" in filtered_df.columns else (0, "0%")
run_delta, run_pct = calculate_delta(filtered_df["jobs_running"]) if "jobs_running" in filtered_df.columns else (0, "0%")
que_delta, que_pct = calculate_delta(filtered_df["jobs_queued"]) if "jobs_queued" in filtered_df.columns else (0, "0%")

display_metric(c1, "CPU Usage", latest.get("cpu_percent", 0), cpu_delta, cpu_pct, "%")
display_metric(c2, "Node Utilization", latest.get("node_utilization", 0), node_delta, node_pct, "%")
display_metric(c3, "Jobs Running", latest.get("jobs_running", 0), run_delta, run_pct)
display_metric(c4, "Jobs Queued", latest.get("jobs_queued", 0), que_delta, que_pct)

# Gauges
st.subheader("🎯 Current Utilization Gauges")
g1, g2 = st.columns(2)

with g1:
    fig_cpu = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=latest.get("cpu_percent", 0),
        delta={'reference': filtered_df["cpu_percent"].iloc[-2] if len(filtered_df) > 1 else 0},
        gauge={'axis': {'range': [0, 100]},
               'bar': {'color': "#3366CC"},
               'steps': [
                   {'range': [0, 50], 'color': "lightgreen"},
                   {'range': [50, 80], 'color': "yellow"},
                   {'range': [80, 100], 'color': "red"}]},
        title={'text': "CPU Utilization %"}))
    fig_cpu.update_layout(height=300)
    st.plotly_chart(fig_cpu, use_container_width=True)

with g2:
    fig_node = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=latest.get("node_utilization", 0),
        delta={'reference': filtered_df["node_utilization"].iloc[-2] if len(filtered_df) > 1 and "node_utilization" in filtered_df.columns else 0},
        gauge={'axis': {'range': [0, 100]},
               'bar': {'color': "#00CC96"},
               'steps': [
                   {'range': [0, 50], 'color': "lightgreen"},
                   {'range': [50, 80], 'color': "yellow"},
                   {'range': [80, 100], 'color': "red"}]},
        title={'text': "Node Utilization %"}))
    fig_node.update_layout(height=300)
    st.plotly_chart(fig_node, use_container_width=True)

# Time Series Charts
st.subheader("📈 Job Queue Trends")
plot_timeseries(filtered_df, ["jobs_running", "jobs_queued", "jobs_held", "jobs_exiting"], "Jobs Over Time")

st.subheader("🖥 Node States")
plot_timeseries(filtered_df, ["nodes_running", "nodes_offline", "nodes_down", "nodes_idle", "nodes_total"], "Node States Over Time")

st.subheader("⚡ Efficiency Trend")
if "cpu_percent" in filtered_df.columns and "node_utilization" in filtered_df.columns:
    filtered_df = filtered_df.copy()
    filtered_df["efficiency"] = (filtered_df["cpu_percent"] * filtered_df["node_utilization"]) / 100
    plot_timeseries(filtered_df, ["efficiency"], "Cluster Efficiency (CPU × Node Util)")

# Raw Data (collapsible)
with st.expander("📄 View Raw Filtered Data (last 1000 rows)", expanded=False):
    st.dataframe(filtered_df.tail(1000), use_container_width=True)
