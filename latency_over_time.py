from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt

# Data as a dictionary
data = {
    "Start Time": [
        "2024-11-30 01:25:44", "2024-11-30 01:25:44", "2024-11-30 01:25:44", 
        "2024-11-30 01:25:44", "2024-11-30 01:25:44", "2024-11-30 01:25:44", 
        "2024-11-30 01:25:44", "2024-11-30 01:25:44", "2024-11-30 01:25:44", 
        "2024-11-30 01:25:44"
    ],
    "End Time": [
        "2024-11-30 01:26:02", "2024-11-30 01:26:03", "2024-11-30 01:26:02",
        "2024-11-30 01:26:03", "2024-11-30 01:26:02", "2024-11-30 01:26:03",
        "2024-11-30 01:26:03", "2024-11-30 01:26:02", "2024-11-30 01:26:03",
        "2024-11-30 01:26:02"
    ],
    "Total Messages Processed": [4876, 3500, 3876, 2700, 2700, 3912, 3462, 3311, 2595, 1920],
    "Consumer Number": ["con-1", "con-2", "con-3", "con-4", "con-5", "con-6", "con-7", "con-8", "con-9", "con-10"]
}

# Convert the data into a DataFrame
df = pd.DataFrame(data)

# Function to calculate latencies for each consumer
def calculate_latencies(row):
    start_time = datetime.strptime(row["Start Time"], "%Y-%m-%d %H:%M:%S")
    end_time = datetime.strptime(row["End Time"], "%Y-%m-%d %H:%M:%S")
    elapsed_time = (end_time - start_time).total_seconds()
    return elapsed_time

# Add latencies column
df["Latency (seconds)"] = df.apply(calculate_latencies, axis=1)

# Plot latencies over time for each consumer
plt.figure(figsize=(12, 8))
for consumer in df["Consumer Number"]:
    consumer_data = df[df["Consumer Number"] == consumer]
    plt.plot(consumer_data["Consumer Number"], consumer_data["Latency (seconds)"], marker='o', label=consumer)

plt.title("Latencies Over Time for Each Consumer")
plt.xlabel("Consumer")
plt.ylabel("Latency (seconds)")
plt.legend(title="Consumer")
plt.grid(True)
plt.tight_layout()
plt.show()
