# Input log file name
input_file_name = "consumer_logs.txt"

# Output log file name for Consumer-1 logs
output_file_name = "consumer_10_logs.txt"

# Filter logs for Consumer-1
try:
    with open(input_file_name, "r") as input_file, open(output_file_name, "w") as output_file:
        for line in input_file:
            if "[Consumer-10]" in line:
                output_file.write(line)
    print(f"Filtered logs for Consumer-10 saved to {output_file_name}")
except FileNotFoundError:
    print(f"Error: The file {input_file_name} was not found.")
