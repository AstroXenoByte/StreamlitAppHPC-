import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.graph_objects as go

st.title("Cluster Resource Usage Dashboard")

#----------------------------------------------------
# File Upload
#----------------------------------------------------
uploaded_file = st.file_uploader("Upload processed_logs.csv", type=["csv"])

if uploaded_file is None:
    st.info("Please upload your processed_logs.csv file to view the dashboard.")
    st.stop()

#----------------------------------------------------
# Load Data After Upload
#----------------------------------------------------
@st.cache_data
def load_data(file):
    df = pd.read_csv(file)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
    return df

df = load_data(uploaded_file)
st.success("File uploaded successfully!")
st.write(df.head())

#----------------------------------------------------
# Date Filter Sidebar
#----------------------------------------------------
st.sidebar.write("### Filter by Date")

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

#----------------------------------------------------
# Filtering dataframe
#----------------------------------------------------
filtered_df = df.copy()

if selected_year != "":
    filtered_df = filtered_df[filtered_df["timestamp"].dt.year == selected_year]

if selected_month != "":
    filtered_df = filtered_df[filtered_df["timestamp"].dt.month == selected_month]

if selected_day != "":
    filtered_df = filtered_df[filtered_df["timestamp"].dt.day == selected_day]

#----------------------------------------------------
# Summary
#----------------------------------------------------
st.write("### Summary Statistics")
st.write(filtered_df.describe())

#----------------------------------------------------
# CPU Gauge
#----------------------------------------------------
st.write("### CPU Utilization Gauge")
latest_cpu = filtered_df["cpu_percent"].iloc[-1] if not filtered_df.empty else 0

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
        'threshold': {'line': {'color': "black", 'width': 4}, 'value': latest_cpu}
    }
))
st.plotly_chart(fig_cpu_gauge, use_container_width=True)

#----------------------------------------------------
# Node Utilization Gauge
#----------------------------------------------------
st.write("### Node Utilization Gauge")
latest_node = filtered_df["node_utilization"].iloc[-1] if not filtered_df.empty else 0

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
        'threshold': {'line': {'color': "black", 'width': 4}, 'value': latest_node}
    }
))
st.plotly_chart(fig_node_gauge, use_container_width=True)

# ----------------------------------------------------
# Derived Core-Hours Calculation
# ----------------------------------------------------
filtered_df["timestamp"] = pd.to_datetime(filtered_df["timestamp"])

# Sort by timestamp 
filtered_df = filtered_df.sort_values("timestamp").reset_index(drop=True)

# time difference between rows in hours
filtered_df['time_diff_hours'] = filtered_df['timestamp'].diff().dt.total_seconds() / 3600
filtered_df['time_diff_hours'] = filtered_df['time_diff_hours'].fillna(0)  # first diff NaN → 0

# core_hours = cpu_used * time_diff_hours
filtered_df['core_hours'] = filtered_df['cpu_used'] * filtered_df['time_diff_hours']

if "core_hours" in filtered_df.columns and not filtered_df.empty and filtered_df['core_hours'].sum() > 0:

    st.write("### Core Hours Over Time")

    fig_ch, ax_ch = plt.subplots()
    ax_ch.plot(filtered_df["timestamp"], filtered_df["core_hours"], linewidth=1.5)
    ax_ch.set_xlabel("Time")
    ax_ch.set_ylabel("Core Hours")
    ax_ch.set_title("Core Hours Consumption Per Interval")
    ax_ch.tick_params(axis='x', rotation=90)

    st.pyplot(fig_ch)
else:
    st.info("Core-hour data unavailable or insufficient for plotting.")


# ensuringg timestamp is datetime type
filtered_df["timestamp"] = pd.to_datetime(filtered_df["timestamp"])

# Sort by timestamp
filtered_df = filtered_df.sort_values("timestamp").reset_index(drop=True)

# Calculate time difference between timestamps in hours
filtered_df['time_diff_hours'] = filtered_df['timestamp'].diff().dt.total_seconds() / 3600
filtered_df['time_diff_hours'] = filtered_df['time_diff_hours'].fillna(0)

# Calculate wait time = jobs_queued * time_diff_hours
filtered_df['wait_time'] = filtered_df['jobs_queued'] * filtered_df['time_diff_hours']

if filtered_df['wait_time'].sum() > 0:
    st.write("### Job Wait Time Over Time")

    fig_wait, ax_wait = plt.subplots()
    ax_wait.plot(filtered_df["timestamp"], filtered_df["wait_time"], color="orange", linewidth=1.5, label="Wait Time per Interval (job-hours)")
    ax_wait.set_xlabel("Time")
    ax_wait.set_ylabel("Wait Time (job-hours)")
    ax_wait.set_title("Job Wait Time Per Interval")
    ax_wait.tick_params(axis='x', rotation=90)
    ax_wait.legend()
    st.pyplot(fig_wait)
else:
    st.info("Wait time data unavailable or insufficient for plotting.")

#----------------------------------------------------
# Job Queue Trends
#----------------------------------------------------
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

#----------------------------------------------------
# Node States Over Time
#----------------------------------------------------
st.write("### Node States Over Time")
if not filtered_df.empty:
    fig_states, ax_states = plt.subplots()
    ax_states.plot(filtered_df["timestamp"], filtered_df["nodes_running"], label="Running", linewidth=1.2)
    ax_states.plot(filtered_df["timestamp"], filtered_df["nodes_offline"], label="Offline", linewidth=1.2)
    ax_states.set_xlabel("Time")
    ax_states.set_ylabel("Number of Nodes")
    ax_states.set_title("Node States Over Time")
    ax_states.legend()
    ax_states.tick_params(axis='x', rotation=90)
    st.pyplot(fig_states)
else:
    st.write("No node state data available for selected filter.")

#----------------------------------------------------
# Combined Efficiency
#----------------------------------------------------
st.write("### Combined Efficiency Over Time")

if not filtered_df.empty:
    temp = filtered_df.copy()
    temp["combined_efficiency"] = (temp["node_utilization"] * temp["cpu_percent"]) / 100

    fig_eff, ax_eff = plt.subplots()
    ax_eff.plot(temp["timestamp"], temp["combined_efficiency"], color="tab:red", linewidth=1.5)
    ax_eff.set_xlabel("Time")
    ax_eff.set_ylabel("Efficiency (%)")
    ax_eff.set_title("Combined Efficiency (Node × CPU) Over Time")
    ax_eff.tick_params(axis='x', rotation=90)
    st.pyplot(fig_eff)

#----------------------------------------------------
# Latest Snapshot
#----------------------------------------------------
st.write("### Latest Snapshot")
st.write(filtered_df.tail(1))

st.caption("Data source: uploaded processed_logs.csv")

