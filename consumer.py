
#this is just a baseline scheduler that handles spikey messages and makes file order_processing_baseline.log
import pika
import time
import json
import threading
import logging
from datetime import datetime
from collections import defaultdict

# Set up logging to save only critical performance metrics to a file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("order_processing_baseline.log")  # Log to file only
    ]
)

# Define RabbitMQ settings
QUEUE_NAMES = {
    'standard': 'standard_orders',
    'express': 'express_orders',
    'priority': 'priority_orders'
}
EXCHANGE_NAME = 'order_exchange'
ROUTING_KEYS = {
    'standard': 'order.standard',
    'express': 'order.express',
    'priority': 'order.priority'
}

# Metrics storage for latency and throughput
metrics = {
    'latency': defaultdict(list),
    'throughput': defaultdict(int)
}

# Connect to RabbitMQ, declare exchange, and bind queue
def connect(queue_name, prefetch_count):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    # Declare the exchange
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic')
    # Declare and bind the queue to the exchange with the appropriate routing key
    channel.queue_declare(queue=queue_name, durable=True)
    routing_key = ROUTING_KEYS[queue_name.split('_')[0]]  # Get routing key based on queue name
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key=routing_key)
    channel.basic_qos(prefetch_count=prefetch_count)  # Control the number of unacknowledged messages
    return channel, connection

# Process each message and log processing times
def process_message(ch, method, properties, body):
    start_time = datetime.now()
    message = json.loads(body)
    
    time.sleep(1)  # Simulate processing time
    
    ch.basic_ack(delivery_tag=method.delivery_tag)
    end_time = datetime.now()
    
    # Calculate and log processing time (latency)
    processing_time = (end_time - start_time).total_seconds()
    queue_name = method.routing_key.split('.')[1]
    
    # Record latency and increment throughput
    metrics['latency'][queue_name].append(processing_time)
    metrics['throughput'][queue_name] += 1

# Start consumers for each queue type, with configurable prefetch count
def start_consumer(queue_name, prefetch_count=10, num_workers=1):
    for _ in range(num_workers):
        channel, connection = connect(queue_name, prefetch_count)
        channel.basic_consume(queue=queue_name, on_message_callback=process_message)
        threading.Thread(target=channel.start_consuming).start()

# Function to dynamically allocate workers based on queue backlog
def adjust_workers():
    # Manually set a fixed number of workers for each queue for the baseline test
    start_consumer(QUEUE_NAMES['priority'], prefetch_count=1, num_workers=3)    # High priority, more workers
    start_consumer(QUEUE_NAMES['express'], prefetch_count=5, num_workers=2)     # Medium priority, moderate workers
    start_consumer(QUEUE_NAMES['standard'], prefetch_count=10, num_workers=1)   # Low priority, fewer workers

# Function to simulate load testing with sample messages
def load_test():
    # Simulate a load by sending a set of test messages
    order_data = {
        'order_id': 'test123',
        'customer_id': 'test_cust',
        'items': ['test_item1', 'test_item2']
    }
    publish_order('standard', order_data)
    publish_order('express', order_data)
    publish_order('priority', order_data)

# Log performance metrics periodically
def log_metrics():
    while True:
        for queue_name, latencies in metrics['latency'].items():
            avg_latency = sum(latencies) / len(latencies) if latencies else 0
            throughput = metrics['throughput'][queue_name] / 5  # messages per second, measured over 5 seconds
            # Log only essential metrics to the file
            logging.info(f"{queue_name} - Avg Latency: {avg_latency:.2f}s, Throughput: {throughput:.2f} orders/sec")
            # Reset throughput for the next interval
            metrics['throughput'][queue_name] = 0
        time.sleep(5)

# Function to simulate order creation for testing
def publish_order(order_type, order_data):
    channel, connection = connect(QUEUE_NAMES[order_type], prefetch_count=1)
    routing_key = ROUTING_KEYS[order_type]
    message = {
        'order_id': order_data['order_id'],
        'customer_id': order_data['customer_id'],
        'items': order_data['items'],
        'timestamp': datetime.now().isoformat()
    }
    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=routing_key, body=json.dumps(message))
    connection.close()

if __name__ == '__main__':
    # Start logging metrics in a separate thread
    threading.Thread(target=log_metrics, daemon=True).start()
    
    # Start consumers with fixed allocation (baseline test)
    adjust_workers()

    # Simulate load testing by publishing sample messages
    load_test()
