import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.graph_objects as go

st.title("Cluster Resource Usage Dashboard")

@st.cache_data
def load_data():
    df = pd.read_csv("processed_logs.csv")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
    return df

df = load_data()

# --- Optional Date Filter Sidebar ---
st.sidebar.write("### Filter by Date (optional)")

years = sorted(df["timestamp"].dt.year.unique())
selected_year = st.sidebar.selectbox("Year (or leave blank)", options=[""] + [str(y) for y in years])

selected_month = ""
selected_day = ""

if selected_year != "":
    selected_year = int(selected_year)
    months = sorted(df[df["timestamp"].dt.year == selected_year]["timestamp"].dt.month.unique())
    selected_month = st.sidebar.selectbox("Month (optional)", options=[""] + [str(m) for m in months])

    if selected_month != "":
        selected_month = int(selected_month)
        days = sorted(df[
            (df["timestamp"].dt.year == selected_year) &
            (df["timestamp"].dt.month == selected_month)
        ]["timestamp"].dt.day.unique())
        selected_day = st.sidebar.selectbox("Day (optional)", options=[""] + [str(d) for d in days])
	
# --- Filtering dataframe ---
filtered_df = df.copy()

if selected_year != "":
    filtered_df = filtered_df[filtered_df["timestamp"].dt.year == selected_year]

if selected_month != "":
    filtered_df = filtered_df[filtered_df["timestamp"].dt.month == selected_month]

st.write(f"### Summary Statistics")
st.write(filtered_df.describe())

# --- CPU Utilization Gauge ---
st.write("### CPU Utilization Gauge")
if not filtered_df.empty:
    latest_cpu = filtered_df["cpu_percent"].iloc[-1]
else:
    latest_cpu = 0

fig_cpu_gauge = go.Figure(go.Indicator(
    mode="gauge+number",
    value=latest_cpu,
    title={'text': "CPU Usage (%)"},
    gauge={
        'axis': {'range': [0, 100]},
        'bar': {'color': "blue"},
        'steps': [
            {'range': [0, 50], 'color': "lightgreen"},
            {'range': [50, 80], 'color': "yellow"},
            {'range': [80, 100], 'color': "red"},
        ],
        'threshold': {
            'line': {'color': "black", 'width': 4},
            'thickness': 0.75,
            'value': latest_cpu
        }
    }
))
st.plotly_chart(fig_cpu_gauge, use_container_width=True)

# --- Node Utilization Gauge ---
st.write("### Node Utilization Gauge")
if not filtered_df.empty and "node_utilization" in filtered_df.columns:
    latest_node = filtered_df["node_utilization"].iloc[-1]
else:
    latest_node = 0

fig_node_gauge = go.Figure(go.Indicator(
    mode="gauge+number",
    value=latest_node,
    title={'text': "Node Utilization (%)"},
    gauge={
        'axis': {'range': [0, 100]},
        'bar': {'color': "green"},
        'steps': [
            {'range': [0, 50], 'color': "lightgreen"},
            {'range': [50, 80], 'color': "yellow"},
            {'range': [80, 100], 'color': "red"},
        ],
        'threshold': {
            'line': {'color': "black", 'width': 4},
            'thickness': 0.75,
            'value': latest_node
        }
    }
))
st.plotly_chart(fig_node_gauge, use_container_width=True)

# --- Job Queue Trends ---
st.write("### Job Queue Dynamics")
if not filtered_df.empty:
    fig_jobs, ax_jobs = plt.subplots()
    ax_jobs.plot(filtered_df["timestamp"], filtered_df["jobs_running"], label="Running", linewidth=1)
    ax_jobs.plot(filtered_df["timestamp"], filtered_df["jobs_queued"], label="Queued", linewidth=1)
    ax_jobs.plot(filtered_df["timestamp"], filtered_df["jobs_held"], label="Held", linewidth=1)
    ax_jobs.plot(filtered_df["timestamp"], filtered_df["jobs_exiting"], label="Exiting", linewidth=1)
    ax_jobs.set_xlabel("Time")
    ax_jobs.set_ylabel("Number of Jobs")
    ax_jobs.set_title("Jobs Over Time")
    ax_jobs.legend()
    ax_jobs.tick_params(axis='x', rotation=90)
    st.pyplot(fig_jobs)
else:
    st.write("No job queue data available for selected filter.")

# --- Node State Trends ---
st.write("### Node States Over Time")
if not filtered_df.empty:
    fig_states, ax_states = plt.subplots()
    ax_states.plot(filtered_df["timestamp"], filtered_df["nodes_running"], label="Running", linewidth=1.2)
    ax_states.plot(filtered_df["timestamp"], filtered_df["nodes_offline"], label="Offline", linewidth=1.2)
    ax_states.plot(filtered_df["timestamp"], filtered_df["nodes_total"], label="Total", linewidth=1.2)
    ax_states.set_xlabel("Time")
    ax_states.set_ylabel("Number of Nodes")
    ax_states.set_title("Node States Over Time")
    ax_states.legend()
    ax_states.tick_params(axis='x', rotation=90)
    st.pyplot(fig_states)
else:
    st.write("No node state data available for selected filter.")

# --- Efficiency Graph (Node Utilization * CPU Utilization) ---
st.write("### Combined Efficiency Over Time")

if "node_utilization" in filtered_df.columns and "cpu_percent" in filtered_df.columns and not filtered_df.empty:
    filtered_df = filtered_df.copy()  # to avoid SettingWithCopyWarning
    filtered_df["combined_efficiency"] = (filtered_df["node_utilization"] * filtered_df["cpu_percent"]) / 100

    fig_eff, ax_eff = plt.subplots()
    ax_eff.plot(filtered_df["timestamp"], filtered_df["combined_efficiency"], color="tab:red", linewidth=1.5)
    ax_eff.set_xlabel("Time")
    ax_eff.set_ylabel("Efficiency (%)")
    ax_eff.set_title("Combined Efficiency (Node × CPU) Over Time")
    ax_eff.tick_params(axis='x', rotation=90)
    st.pyplot(fig_eff)
else:
    st.write("No efficiency data available for selected filter.")

# --- Latest Snapshot ---
st.write("### Latest Snapshot")
st.write(filtered_df.tail(1))

st.caption("Data source: processed_logs.csv (generated by processed_logs.py)")
