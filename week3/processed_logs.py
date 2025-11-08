import pandas as pd
import glob
import os

LOGS_FOLDER = "cleaned"

if not os.path.exists(LOGS_FOLDER):
    os.makedirs(LOGS_FOLDER)
    print(f"Created folder: {LOGS_FOLDER} (currently empty)")

# Load all CSV log files
csv_files = glob.glob(os.path.join(LOGS_FOLDER, "*.csv"))
print(f"Found {len(csv_files)} CSV files in '{LOGS_FOLDER}': {csv_files}")

if not csv_files:
    print("No CSV files found. Please place your log files inside the 'logs' folder.")
    exit()

# Combine all logs
dfs = []
for file in csv_files:
    df = pd.read_csv(file)
    dfs.append(df)

full_df = pd.concat(dfs, ignore_index=True)

# Convert timestamp
full_df["timestamp"] = pd.to_datetime(full_df["timestamp"])

# Sort by timestamp
full_df = full_df.sort_values("timestamp")

# Compute derived metrics
full_df["node_utilization"] = (full_df["nodes_running"] / full_df["nodes_total"]) * 100
full_df["cpu_idle_percent"] = 100 - full_df["cpu_percent"]
full_df["jobs_total"] = full_df["jobs_running"] + full_df["jobs_queued"] + full_df["jobs_held"] + full_df["jobs_exiting"]

# Summary stats
summary = full_df[[
    "cpu_percent",
    "node_utilization",
    "jobs_running",
    "jobs_queued",
    "jobs_held",
    "jobs_exiting"
]].describe()

print("=== Cluster Summary ===")
print(summary)

# Save processed dataset
output_file = "processed_logs.csv"
full_df.to_csv(output_file, index=False)
print(f"Processed data saved to {output_file}")
