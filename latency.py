from datetime import datetime

# Data
timestamps = [
    "2024-11-30 01:25:04",
    "2024-11-30 01:26:02"
]
messages_processed = [1, 2692]

# Convert string timestamps to datetime objects
start_time = datetime.strptime(timestamps[0], "%Y-%m-%d %H:%M:%S")
end_time = datetime.strptime(timestamps[1], "%Y-%m-%d %H:%M:%S")

# Calculate total messages processed
total_messages_processed = messages_processed[1] - messages_processed[0]

# Calculate elapsed time in seconds
elapsed_time = (end_time - start_time).total_seconds()

# Calculate throughput
throughput = total_messages_processed / elapsed_time

# Display results
print(f"Total Messages Processed: {total_messages_processed}")
print(f"Elapsed Time (seconds): {elapsed_time}")
print(f"Throughput (messages/second): {throughput:.2f}")