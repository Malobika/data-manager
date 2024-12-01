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
consumer_threads = defaultdict(list)


class CentralizedScheduler:
    def __init__(self):
        self.lock = threading.Lock()
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        self.channels = {}  # To store channels for each queue

    def setup_channel(self, queue_name, prefetch_count=1):
        if queue_name not in self.channels:
            channel = self.connection.channel()
            # Ensure exchange and queue declarations are consistent
            channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic')
            channel.queue_declare(queue=queue_name, durable=True)
            # Bind the queue to the exchange with the appropriate routing key
            routing_key = ROUTING_KEYS[queue_name.split('_')[0]]
            channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key=routing_key)
            channel.basic_qos(prefetch_count=prefetch_count)
            self.channels[queue_name] = channel
        return self.channels[queue_name]

    def process_message(self, ch, method, properties, body, queue_name):
        message = json.loads(body)
        start_time = datetime.fromisoformat(message['timestamp'])
        logging.info(f"Processing message from {queue_name}: {message}")
        
        time.sleep(1)  # Simulate processing time
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
        latency = (datetime.now() - start_time).total_seconds()
        with self.lock:
            metrics['latency'][queue_name].append(latency)
            metrics['throughput'][queue_name] += 1
            queue_status[queue_name]['processing_time'].append(latency)
        logging.info(f"Processed message from {queue_name} in {latency:.2f} seconds")

    def start_consumer(self, queue_name, prefetch_count=10):
        channel = self.setup_channel(queue_name, prefetch_count)
        channel.basic_consume(queue=queue_name, on_message_callback=lambda ch, method, props, body: self.process_message(ch, method, props, body, queue_name))
        logging.info(f"Starting consumer for {queue_name} with prefetch_count={prefetch_count}")
        try:
            channel.start_consuming()
        except pika.exceptions.ConnectionClosedByBroker:
            logging.error(f"Connection to {queue_name} closed by broker.")

    def add_consumer(self, queue_name, prefetch_count):
        thread = threading.Thread(target=self.start_consumer, args=(queue_name, prefetch_count), daemon=True)
        thread.start()
        consumer_threads[queue_name].append(thread)
        logging.info(f"Added consumer to {queue_name}")

    def remove_consumer(self, queue_name):
        if consumer_threads[queue_name]:
            thread = consumer_threads[queue_name].pop()
            # Note: Implement logic to stop threads gracefully
            logging.info(f"Removed consumer from {queue_name}")

    def monitor_and_adjust(self):
        while True:
            with self.lock:
                for queue_name in QUEUE_NAMES.values():
                    queue = self.channels[queue_name].queue_declare(queue=queue_name, passive=True)
                    queue_length = queue.method.message_count
                    queue_status[queue_name]['length'] = queue_length

                # Decision-making logic based on queue lengths
                for queue_name, queue_data in queue_status.items():
                    if queue_data['length'] > 10 and len(consumer_threads[queue_name]) < 3:
                        self.add_consumer(queue_name, prefetch_count=1)
                    elif queue_data['length'] == 0 and len(consumer_threads[queue_name]) > 1:
                        self.remove_consumer(queue_name)

            self.log_metrics()
            time.sleep(5)

    def log_metrics(self):
        for queue_name, latencies in metrics['latency'].items():
            avg_latency = sum(latencies) / len(latencies) if latencies else 0
            throughput = metrics['throughput'][queue_name]
            logging.info(f"{queue_name} - Avg Latency: {avg_latency:.2f}s, Throughput: {throughput} orders/sec")


def generate_random_order(order_type):
    return {
        'order_id': f"{order_type}_{random.randint(1000, 9999)}",
        'customer_id': f"cust_{random.randint(100, 999)}",
        'items': [f"item_{random.randint(1, 5)}" for _ in range(random.randint(1, 3))],
        'timestamp': datetime.now().isoformat()
    }


def publish_order(order_type, order_data):
    channel = scheduler.setup_channel(QUEUE_NAMES[order_type])
    routing_key = ROUTING_KEYS[order_type]
    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=routing_key, body=json.dumps(order_data))
    logging.info(f"Sent {order_type} order: {order_data}")


if __name__ == '__main__':
    scheduler = CentralizedScheduler()
    monitoring_thread = threading.Thread(target=scheduler.monitor_and_adjust, daemon=True)
    monitoring_thread.start()

    try:
        while True:
            order_type = random.choice(['priority'] * 3 + ['express', 'standard'])
            order_data = generate_random_order(order_type)
            publish_order(order_type, order_data)
            time.sleep(random.uniform(0.5, 2))
    except KeyboardInterrupt:
        logging.info("Stopped sending orders.")
        scheduler.connection.close()
