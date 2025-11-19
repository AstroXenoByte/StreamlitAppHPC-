import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import os
from datetime import datetime

# ------------------- PAGE CONFIG -------------------
st.set_page_config(page_title="HPC Cluster Dashboard", layout="wide")

# ------------------- SETTINGS -------------------
DOWNSAMPLE_POINTS = 1000  # max points to plot per timeseries (tune if needed)
CSV_NAME = "processed_logs.csv"

# ------------------- RESOLVE CSV PATH SAFELY -------------------
def get_csv_path():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(script_dir, CSV_NAME),
        os.path.normpath(os.path.join(script_dir, "..", CSV_NAME)),
        os.path.join(os.getcwd(), CSV_NAME),
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    st.error(
        "❌ **processed_logs.csv not found!**\n\n"
        "Looked in script folder, parent folder, and current working directory.\n\n"
        "Place `processed_logs.csv` next to this script or upload it in your deployment."
    )
    st.stop()

# ------------------- LOAD DATA -------------------
@st.cache_data(ttl=300, show_spinner=False)
def load_data(csv_path: str):
    # parse_dates speeds up timestamp conversion; infer_datetime_format helps further
    try:
        df = pd.read_csv(
            csv_path,
            parse_dates=["timestamp"],
            infer_datetime_format=True,
            low_memory=True
        )
    except Exception as e:
        # Fall back to safe load + conversion
        df = pd.read_csv(csv_path, low_memory=True)
        df["timestamp"] = pd.to_datetime(df.get("timestamp"), errors="coerce", infer_datetime_format=True)

    # basic validation
    if "timestamp" not in df.columns:
        raise ValueError("CSV must contain a 'timestamp' column.")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    # set index for fast time slicing; keep timestamp column too for compatibility
    df.index = pd.DatetimeIndex(df["timestamp"])
    return df

# ------------------- UTILS -------------------
def downsample_df(df: pd.DataFrame, max_points: int = DOWNSAMPLE_POINTS):
    """Return a downsampled DataFrame keeping first+last and evenly spaced intermediate rows."""
    n = len(df)
    if n <= max_points:
        return df
    # include first and last always
    idx = np.linspace(0, n - 1, num=max_points, dtype=int)
    return df.iloc[idx]

@st.cache_data(ttl=300)
def compute_efficiency(df: pd.DataFrame):
    # Vectorized efficiency computation if columns exist
    if ("cpu_percent" in df.columns) and ("node_utilization" in df.columns):
        # avoid modifying original by returning an assigned copy
        return df.assign(efficiency=(df["cpu_percent"] * df["node_utilization"]) / 100.0)
    return df

def safe_last_two(series: pd.Series):
    """Return last and previous value (or (last, last) if only one) safely and fast."""
    if series is None or len(series) == 0:
        return 0.0, 0.0
    if len(series) == 1:
        v = float(series.iloc[-1])
        return v, v
    return float(series.iloc[-1]), float(series.iloc[-2])

def format_metric_value(val, unit=""):
    # round integers without unnecessary decimals
    if pd.isna(val):
        val = 0
    if abs(val - int(val)) < 1e-9:
        return f"{int(val):,}{unit}"
    return f"{val:,.1f}{unit}"

# ------------------- PLOTTING -------------------
PLOT_CONFIG = {"displayModeBar": False}

def plot_timeseries(data: pd.DataFrame, columns: list, title: str):
    if data.empty:
        st.info("No data to display.")
        return
    # Downsample before plotting to keep interactive fast
    ds = downsample_df(data, DOWNSAMPLE_POINTS)
    fig = go.Figure()
    for col in columns:
        if col in ds.columns:
            fig.add_trace(go.Scatter(
                x=ds["timestamp"],
                y=ds[col],
                mode="lines",  # lines only -> faster rendering
                name=col.replace("_", " ").title(),
                line=dict(width=2),
                hovertemplate=None
            ))
    fig.update_layout(
        title=title,
        xaxis_title="Time",
        yaxis_title="Value",
        hovermode="x unified",
        template="plotly_white",
        height=420,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    st.plotly_chart(fig, use_container_width=True, config=PLOT_CONFIG)

def plot_gauge(value, reference, title_text):
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=value,
        delta={'reference': reference},
        gauge={'axis': {'range': [0, 100]},
               'bar': {'color': "#3366CC"},
               'steps': [
                   {'range': [0, 50], 'color': "lightgreen"},
                   {'range': [50, 80], 'color': "yellow"},
                   {'range': [80, 100], 'color': "red"}]},
        title={'text': title_text}
    ))
    fig.update_layout(height=300)
    return fig

# ------------------- MAIN -------------------
csv_path = get_csv_path()
try:
    df = load_data(csv_path)
except Exception as e:
    st.error(f"Failed to load CSV: {e}")
    st.stop()

# Sidebar filters (compute options once)
with st.sidebar:
    st.title("🖥 HPC Cluster Dashboard")
    st.markdown("---")
    st.header("⚙️ Time Filters")
    if df.empty:
        st.warning("No data available")
        st.stop()
    # get unique year/month/day strings quickly from index (fast because index is DatetimeIndex)
    years = sorted({str(y) for y in df.index.year}, reverse=True)
    selected_year = st.selectbox("Year", options=["All"] + years, index=0)

    selected_month = "All"
    selected_day = "All"

    if selected_year != "All":
        # filter by year using boolean mask (fast)
        year_int = int(selected_year)
        months = sorted({m for m in df.index[df.index.year == year_int].month})
        months_str = [f"{m:02d}" for m in months]
        selected_month = st.selectbox("Month", options=["All"] + months_str, index=0)

        if selected_month != "All":
            month_int = int(selected_month)
            days = sorted({d for d in df.index[(df.index.year == year_int) & (df.index.month == month_int)].day})
            days_str = [f"{d:02d}" for d in days]
            selected_day = st.selectbox("Day", options=["All"] + days_str, index=0)

# Apply filters using fast boolean masks (no copies unless necessary)
mask = np.ones(len(df), dtype=bool)
if selected_year != "All":
    mask &= (df.index.year == int(selected_year))
if selected_month != "All":
    mask &= (df.index.month == int(selected_month))
if selected_day != "All":
    mask &= (df.index.day == int(selected_day))

filtered_df = df.loc[mask]
if filtered_df.empty:
    st.warning("No data matches the selected filters.")
    st.stop()

# Compute derived columns cached
filtered_df = compute_efficiency(filtered_df)

# Header summary
start_ts = filtered_df["timestamp"].min()
end_ts = filtered_df["timestamp"].max()
st.title("📊 HPC Cluster Resource Monitoring Dashboard")
st.markdown(f"**Data range:** {start_ts.strftime('%Y-%m-%d %H:%M')} → {end_ts.strftime('%Y-%m-%d %H:%M')} | **Filtered:** {len(filtered_df):,} rows")

# Latest metrics row (vectorized access)
st.subheader("🔥 Latest Snapshot")
c1, c2, c3, c4 = st.columns(4)

latest_row = filtered_df.iloc[-1]

# CPU
cpu_last, cpu_prev = safe_last_two(filtered_df.get("cpu_percent"))
cpu_delta = cpu_last - cpu_prev
cpu_pct_str = f"{(cpu_delta / cpu_prev * 100):+.1f}%" if cpu_prev != 0 else "+0.0%"

# Node
node_last, node_prev = safe_last_two(filtered_df.get("node_utilization"))
node_delta = node_last - node_prev
node_pct_str = f"{(node_delta / node_prev * 100):+.1f}%" if node_prev != 0 else "+0.0%"

# Jobs running
run_last, run_prev = safe_last_two(filtered_df.get("jobs_running"))
run_delta = run_last - run_prev
run_pct_str = f"{(run_delta / run_prev * 100):+.1f}%" if run_prev != 0 else "+0.0%"

# Jobs queued
que_last, que_prev = safe_last_two(filtered_df.get("jobs_queued"))
que_delta = que_last - que_prev
que_pct_str = f"{(que_delta / que_prev * 100):+.1f}%" if que_prev != 0 else "+0.0%"

def display_metric(col, title, val, delta, delta_pct, unit=""):
    with col:
        st.metric(label=title, value=format_metric_value(val, unit), delta=f"{delta:+,.0f} ({delta_pct})")

display_metric(c1, "CPU Usage", cpu_last, cpu_delta, cpu_pct_str, "%")
display_metric(c2, "Node Utilization", node_last, node_delta, node_pct_str, "%")
display_metric(c3, "Jobs Running", run_last, run_delta, run_pct_str)
display_metric(c4, "Jobs Queued", que_last, que_delta, que_pct_str)

# Gauges (use smaller snapshot references if not available)
st.subheader("🎯 Current Utilization Gauges")
g1, g2 = st.columns(2)

with g1:
    ref = float(filtered_df["cpu_percent"].iloc[-2]) if len(filtered_df) > 1 and "cpu_percent" in filtered_df.columns else 0.0
    fig_cpu = plot_gauge(cpu_last, ref, "CPU Utilization %")
    st.plotly_chart(fig_cpu, use_container_width=True, config=PLOT_CONFIG)

with g2:
    ref_node = float(filtered_df["node_utilization"].iloc[-2]) if len(filtered_df) > 1 and "node_utilization" in filtered_df.columns else 0.0
    fig_node = plot_gauge(node_last, ref_node, "Node Utilization %")
    st.plotly_chart(fig_node, use_container_width=True, config=PLOT_CONFIG)

# Time Series Charts (downsampled inside function)
st.subheader("📈 Job Queue Trends")
plot_timeseries(filtered_df, ["jobs_running", "jobs_queued", "jobs_held", "jobs_exiting"], "Jobs Over Time")

st.subheader("🖥 Node States")
plot_timeseries(filtered_df, ["nodes_running", "nodes_offline", "nodes_down", "nodes_idle", "nodes_total"], "Node States Over Time")

st.subheader("⚡ Efficiency Trend")
if "efficiency" in filtered_df.columns:
    plot_timeseries(filtered_df, ["efficiency"], "Cluster Efficiency (CPU × Node Util)")

# Raw Data (collapsible) - limit rows to keep UI responsive
with st.expander("📄 View Raw Filtered Data (last 1000 rows)", expanded=False):
    st.dataframe(filtered_df.tail(1000), use_container_width=True)
