from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt

# Data as a dictionary
data = {
    "Start Time": [
        "2024-11-30 01:25:37", "2024-11-30 01:25:39", "2024-11-30 01:25:39", 
        "2024-11-30 01:25:40", "2024-11-30 01:25:41", "2024-11-30 01:25:41", 
        "2024-11-30 01:25:43", "2024-11-30 01:25:45", "2024-11-30 01:25:49", 
        "2024-11-30 01:25:53"
    ],
    "End Time": [
        "2024-11-30 01:26:02", "2024-11-30 01:26:02", "2024-11-30 01:26:02",
        "2024-11-30 01:26:02", "2024-11-30 01:26:02", "2024-11-30 01:26:08",
        "2024-11-30 01:26:08", "2024-11-30 01:26:08", "2024-11-30 01:26:08",
        "2024-11-30 01:26:08"
    ],
    "Total Messages Processed": [4876, 3500, 3876, 2700, 2700, 3912, 3462, 3311, 2595, 1920],
    "Consumer Number": ["con-1", "con-2", "con-3", "con-4", "con-5", "con-6", "con-7", "con8", "con-9", "con-10"]
}

# Convert the data into a DataFrame
df = pd.DataFrame(data)

# Calculate throughput for each consumer
def calculate_throughput(row):
    start_time = datetime.strptime(row["Start Time"], "%Y-%m-%d %H:%M:%S")
    end_time = datetime.strptime(row["End Time"], "%Y-%m-%d %H:%M:%S")
    elapsed_time = (end_time - start_time).total_seconds()
    return row["Total Messages Processed"] / elapsed_time

df["Throughput (messages/second)"] = df.apply(calculate_throughput, axis=1)

# Plot throughput for each consumer
plt.figure(figsize=(10, 6))
plt.bar(df["Consumer Number"], df["Throughput (messages/second)"], color='skyblue')
plt.title("Throughput by Consumer")
plt.xlabel("Consumer")
plt.ylabel("Throughput (messages/second)")
plt.grid(axis='y')
plt.tight_layout()
plt.show()
