import pika
import json
import time
import threading
import logging
from datetime import datetime
from collections import defaultdict
import random

# Configure logging to save to a file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("order_processing.log"), logging.StreamHandler()]
)

# RabbitMQ configuration
RABBITMQ_HOST = 'localhost'
EXCHANGE_NAME = 'order_exchange'
ROUTING_KEYS = {'standard': 'order.standard', 'express': 'order.express', 'priority': 'order.priority'}
QUEUE_NAMES = {'standard': 'standard_orders', 'express': 'express_orders', 'priority': 'priority_orders'}

# Store metrics
metrics = {'latency': defaultdict(list), 'throughput': defaultdict(int)}
queue_status = {name: {'length': 0, 'processing_time': []} for name in QUEUE_NAMES.values()}

class OrderConsumer(threading.Thread):
    def __init__(self, queue_name):
        threading.Thread.__init__(self)
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic')
        self.channel.queue_declare(queue=queue_name, durable=True)
        routing_key = ROUTING_KEYS[queue_name.split('_')[0]]
        self.channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key=routing_key)
        self.channel.basic_qos(prefetch_count=10)
        self.channel.basic_consume(queue=queue_name, on_message_callback=self.process_message)
        self.lock = threading.Lock()

    def process_message(self, ch, method, properties, body):
        message = json.loads(body)
        start_time = datetime.fromisoformat(message['timestamp'])

        time.sleep(0.5)  # Simulated processing time
        ch.basic_ack(delivery_tag=method.delivery_tag)

        latency = (datetime.now() - start_time).total_seconds()
        with self.lock:
            metrics['latency'][self.queue_name].append(latency)
            metrics['throughput'][self.queue_name] += 1
            queue_status[self.queue_name]['processing_time'].append(latency)

    def run(self):
        try:
            self.channel.start_consuming()
        except pika.exceptions.ConnectionClosedByBroker:
            pass

def generate_random_order(order_type):
    return {
        'order_id': f"{order_type}_{random.randint(1000, 9999)}",
        'customer_id': f"cust_{random.randint(100, 999)}",
        'items': [f"item_{random.randint(1, 5)}" for _ in range(random.randint(1, 3))],
        'timestamp': datetime.now().isoformat()
    }

def publish_order(order_type, order_data, connection, channel):
    routing_key = ROUTING_KEYS[order_type]
    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=routing_key, body=json.dumps(order_data))

def start_producers():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic')

    try:
        while True:
            start_time = time.time()
            for _ in range(100):  # Publish 100 messages per second
                order_type = random.choice(['priority'] * 3 + ['express', 'standard'])
                order_data = generate_random_order(order_type)
                publish_order(order_type, order_data, connection, channel)
            elapsed = time.time() - start_time
            sleep_time = max(0, 1 - elapsed)
            time.sleep(sleep_time)
    except KeyboardInterrupt:
        connection.close()

def log_metrics():
    while True:
        time.sleep(5)
        for queue_name, latencies in metrics['latency'].items():
            avg_latency = sum(latencies[-100:]) / len(latencies[-100:]) if latencies else 0
            throughput = metrics['throughput'][queue_name] / 5  # Throughput per second over last 5 seconds
            metrics['throughput'][queue_name] = 0  # Reset throughput counter
            logging.info(f"{queue_name} - Avg Latency: {avg_latency:.2f}s, Throughput: {throughput:.2f} orders/sec")
            metrics['latency'][queue_name] = []  # Reset latency list to keep the metrics windowed

if __name__ == '__main__':
    # Start multiple consumers for each queue
    consumers = []

    # Define the number of consumers per queue
    num_consumers = {
        'priority_orders': 30,
        'express_orders': 8,
        'standard_orders': 8
    }

    for queue_name, count in num_consumers.items():
        for _ in range(count):
            consumer = OrderConsumer(queue_name)
            consumer.daemon = True
            consumer.start()
            consumers.append(consumer)

    # Start a thread to log metrics
    metrics_thread = threading.Thread(target=log_metrics, daemon=True)
    metrics_thread.start()

    # Start producers
    start_producers()
