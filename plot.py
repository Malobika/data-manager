import re
from collections import defaultdict
import matplotlib.pyplot as plt
from datetime import datetime
# File name containing the logs
logs = "consumer_2_log.txt"



pattern = re.compile(r'\[(.*?)\] \[Consumer-(\d+)\] Received: Message')

# Dictionary to store timestamps for each consumer
consumer_timestamps = {}

# Parse the logs
for line in logs.splitlines():
    match = pattern.search(line)
    if match:
        timestamp = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
        consumer_id = match.group(2)
        if consumer_id not in consumer_timestamps:
            consumer_timestamps[consumer_id] = []
        consumer_timestamps[consumer_id].append(timestamp)

print(f"Consumers found in logs: {list(consumer_timestamps.keys())}")

# Choose the consumer to analyze
consumer_id_to_analyze = "2"  # Adjust the ID to analyze another consumer

if consumer_id_to_analyze in consumer_timestamps:
    timestamps = consumer_timestamps[consumer_id_to_analyze]
    if len(timestamps) > 1:
        # Calculate latencies between consecutive messages
        latencies = [(timestamps[i] - timestamps[i - 1]).total_seconds() for i in range(1, len(timestamps))]

        # Print latencies
        print(f"Consumer-{consumer_id_to_analyze} Latencies: {latencies}")

        # Plot the latencies
        plt.figure(figsize=(10, 6))
        plt.plot(range(len(latencies)), latencies, marker='o', linestyle='-', color='blue')
        plt.title(f"Message Latency Over Time for Consumer-{consumer_id_to_analyze}")
        plt.xlabel("Message Index")
        plt.ylabel("Latency (seconds)")
        plt.grid(True)
        plt.tight_layout()
        plt.show()
    else:
        print(f"Consumer-{consumer_id_to_analyze} has only one timestamp, no latency to calculate.")
else:
    print(f"No data found for Consumer-{consumer_id_to_analyze}.")