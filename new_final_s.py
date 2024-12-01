import pika
import time
import threading
from datetime import datetime
from collections import defaultdict
lock = threading.Lock()
# Global dictionary to track the message count for each consumer
message_counts = defaultdict(int)
log_file = "consumer_logs.txt"
# Connection parameters
rabbitmq_host = 'localhost'  # Replace with your RabbitMQ server address
queue_1 = 'example_queue_1'
queue_2 = 'example_queue_2'

# Establish a connection to RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
channel = connection.channel()

# Declare the first queue
channel.queue_declare(queue=queue_1)

# Declare the second queue
channel.queue_declare(queue=queue_2)

# Message counters
message_count_queue_1 = 0
message_threshold = 1000  # Threshold for creating a new dynamic queue
new_queue_created = False  # Flag to check if a new queue has been created

# Function to publish a message to a specific queue
def publish_message(queue, message):
    channel.basic_publish(exchange='', routing_key=queue, body=message)

# Callback function for the first queue
def callback_queue_1(ch, method, properties, body):
    global message_count_queue_1, new_queue_created
    message_count_queue_1 += 1
    print(f" [x] Received from {queue_1}: '{body.decode()}'")

    # Check if the threshold is exceeded
    if message_count_queue_1 > message_threshold and not new_queue_created:
        create_new_queue()

# Callback function for the second queue
def callback_queue_2(ch, method, properties, body):
    print(f" [x] Received from {queue_2}: '{body.decode()}'")

# Function to create a new queue dynamically
def create_new_queue():
    global new_queue_created
    new_queue_name = f"dynamic_queue_{message_count_queue_1}"
    channel.queue_declare(queue=new_queue_name)
    print(f" [x] New queue created: '{new_queue_name}'")

    # Start consuming from the new queue
    def callback_new_queue(ch, method, properties, body):
        print(f" [x] Received from {new_queue_name}: '{body.decode()}'")

    channel.basic_consume(queue=new_queue_name, on_message_callback=callback_new_queue, auto_ack=True)
    new_queue_created = True

# Function to publish messages at different rates
def publish_messages_at_rate(rate, total_messages):
    interval = 1 / rate  # Interval between messages in seconds
    print(f"Publishing {total_messages} messages at {rate} messages per second...")
    for i in range(total_messages):
        publish_message(queue_1, f"Message {i + 1} to Queue 1")
        publish_message(queue_2, f"Message {i + 1} to Queue 2")
        time.sleep(interval)

# Function to start consuming from the queues
def start_consuming():
    num_consumers_per_queue = 5

    # Function to handle a message and update message count
    def handle_message(ch, method, properties, body, consumer_id):
        with lock:
            message_counts[consumer_id] += 1
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
            # Write the log to a file
            with open(log_file, "a") as file:
                file.write(f"[{timestamp}] [Consumer-{consumer_id}] Received: {body.decode()}\n")
                file.write(f"[{timestamp}] [Consumer-{consumer_id}] Total Messages Processed: {message_counts[consumer_id]}\n")
        print(f"[{timestamp}] [Consumer-{consumer_id}] Received: {body.decode()}")
        print(f"[{timestamp}] [Consumer-{consumer_id}] Total Messages Processed: {message_counts[consumer_id]}")

    # Assign consumers to queue_1
    for i in range(num_consumers_per_queue):
        channel.basic_consume(
            queue=queue_1,
            on_message_callback=lambda ch, method, properties, body, cid=i + 1: handle_message(ch, method, properties, body, cid),
            auto_ack=True,
        )

    # Assign consumers to queue_2
    for i in range(num_consumers_per_queue):
        channel.basic_consume(
            queue=queue_2,
            on_message_callback=lambda ch, method, properties, body, cid=i + 6: handle_message(ch, method, properties, body, cid),
            auto_ack=True,
        )

    print(' [*] Waiting for messages. To exit, press CTRL+C')
    channel.start_consuming()
def handle_message(body, consumer_id):
    message_counts[f"Consumer-{consumer_id}"] += 1
    print(f" [Consumer-{consumer_id}] Received: {body.decode()}")

if __name__ == "__main__":
    try:
        # Publish messages at increasing rates
        publish_messages_at_rate(5, 100)  # 5 messages per second
        publish_messages_at_rate(10, 100)  # 10 messages per second
        publish_messages_at_rate(50, 500)  # 50 messages per second
        publish_messages_at_rate(100, 1000)  # 100 messages per second
        publish_messages_at_rate(1000, 5000)  # 1000 messages per second
        publish_messages_at_rate(10000, 10000)  # 10000 messages per second

        # Start consuming messages from both queues
        start_consuming()
    except KeyboardInterrupt:
        print("Interrupted")
        connection.close()
