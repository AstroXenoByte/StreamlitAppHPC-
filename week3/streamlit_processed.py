import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta

# ------------------- CONFIG -------------------
st.set_page_config(page_title="HPC Cluster Dashboard", layout="wide")

# ------------------- LOAD DATA -------------------
@st.cache_data
def load_data():
    df = pd.read_csv("processed_logs.csv")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
    return df

df = load_data()

# ------------------- SIDEBAR -------------------
with st.sidebar:
    st.title("HPC Cluster Dashboard")
    st.header("⚙️ Filters")

    years = sorted(df["timestamp"].dt.year.unique())
    selected_year = st.selectbox("Year", options=[""] + [str(y) for y in years])

    selected_month = ""
    selected_day = ""

    if selected_year != "":
        selected_year = int(selected_year)
        months = sorted(df[df["timestamp"].dt.year == selected_year]["timestamp"].dt.month.unique())
        selected_month = st.selectbox("Month", options=[""] + [str(m) for m in months])

        if selected_month != "":
            selected_month = int(selected_month)
            days = sorted(df[
                (df["timestamp"].dt.year == selected_year) &
                (df["timestamp"].dt.month == selected_month)
            ]["timestamp"].dt.day.unique())
            selected_day = st.selectbox("Day", options=[""] + [str(d) for d in days])

# ------------------- FILTER DATA -------------------
filtered_df = df.copy()

if selected_year != "":
    filtered_df = filtered_df[filtered_df["timestamp"].dt.year == selected_year]

if selected_month != "":
    filtered_df = filtered_df[filtered_df["timestamp"].dt.month == selected_month]

if selected_day != "":
    filtered_df = filtered_df[filtered_df["timestamp"].dt.day == selected_day]

# ------------------- METRIC DELTA -------------------
def calculate_delta(df, column):
    if len(df) < 2:
        return 0, 0
    current_value = df[column].iloc[-1]
    previous_value = df[column].iloc[-2]
    delta = current_value - previous_value
    percent = (delta / previous_value) * 100 if previous_value != 0 else 0
    return delta, percent

def display_metric(col, title, df, column, unit=""):
    if df.empty:
        value, delta, percent = 0, 0, 0
    else:
        value = df[column].iloc[-1]
        delta, percent = calculate_delta(df, column)

    with col:
        with st.container(border=True):
            st.metric(title, f"{value:,.0f}{unit}", f"{delta:+,.0f} ({percent:+.2f}%)")

# ------------------- PLOTLY TIME SERIES FUNCTION -------------------
def plot_timeseries(df, y_columns, title, colors=None):
    if df.empty:
        st.info("No data available for this filter.")
        return

    fig = go.Figure()

    if colors is None:
        colors = [None] * len(y_columns)

    for col, color in zip(y_columns, colors):
        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df[col],
                mode="lines",
                name=col.replace("_", " ").title(),
                line=dict(width=2, color=color)
            )
        )

    fig.update_layout(
        title=title,
        xaxis_title="Time",
        yaxis_title="Value",
        hovermode="x unified",
        template="plotly_white",
        height=400,
    )

    st.plotly_chart(fig, use_container_width=True)

# ------------------- TITLE -------------------
st.title("📊 HPC Resource Usage Overview")

# ------------------- METRICS -------------------
st.subheader("Latest Metrics (Live Snapshot)")
c1, c2, c3, c4 = st.columns(4)

display_metric(c1, "CPU Usage (%)", filtered_df, "cpu_percent", "%")
display_metric(c2, "Node Utilization (%)", filtered_df, "node_utilization", "%")
display_metric(c3, "Jobs Running", filtered_df, "jobs_running")
display_metric(c4, "Jobs Queued", filtered_df, "jobs_queued")

# ------------------- GAUGE CHARTS -------------------
st.subheader("Cluster Utilization Gauges")

gc1, gc2 = st.columns(2)

# CPU GAUGE
with gc1:
    st.write("### CPU Utilization")
    latest_cpu = filtered_df["cpu_percent"].iloc[-1] if not filtered_df.empty else 0
    fig_cpu = go.Figure(go.Indicator(
        mode="gauge+number",
        value=latest_cpu,
        gauge={
            "axis": {"range": [0, 100]},
            "steps": [
                {"range": [0, 50], "color": "lightgreen"},
                {"range": [50, 80], "color": "yellow"},
                {"range": [80, 100], "color": "red"},
            ],
            "bar": {"color": "blue"}
        }
    ))
    st.plotly_chart(fig_cpu, use_container_width=True)

# NODE GAUGE
with gc2:
    st.write("### Node Utilization")
    latest_node = filtered_df["node_utilization"].iloc[-1] if not filtered_df.empty else 0
    fig_node = go.Figure(go.Indicator(
        mode="gauge+number",
        value=latest_node,
        gauge={
            "axis": {"range": [0, 100]},
            "steps": [
                {"range": [0, 50], "color": "lightgreen"},
                {"range": [50, 80], "color": "yellow"},
                {"range": [80, 100], "color": "red"},
            ],
            "bar": {"color": "green"}
        }
    ))
    st.plotly_chart(fig_node, use_container_width=True)

# ------------------- MODERN PLOTS -------------------

# JOB QUEUE
st.subheader("🧵 Job Queue Over Time")
plot_timeseries(
    filtered_df,
    ["jobs_running", "jobs_queued", "jobs_held", "jobs_exiting"],
    title="Job Queue Trends",
    colors=["blue", "orange", "purple", "red"]
)

# NODE STATE
st.subheader("🖥 Node States Over Time")
plot_timeseries(
    filtered_df,
    ["nodes_running", "nodes_offline", "nodes_total"],
    title="Node State Trends",
    colors=["green", "red", "gray"]
)

# EFFICIENCY
st.subheader("⚡ Combined Efficiency Over Time")
if not filtered_df.empty:
    filtered_df["efficiency"] = (
        filtered_df["cpu_percent"] * filtered_df["node_utilization"]
    ) / 100

plot_timeseries(
    filtered_df,
    ["efficiency"],
    title="Cluster Efficiency Trend",
    colors=["red"]
)

# ------------------- RAW DATA -------------------
with st.expander("📄 Raw Data (Filtered)"):
    st.dataframe(filtered_df.tail(1000))
