#this is just a not an advanced scheduler that handles spikey messages and makes file order_processing.log




import pika
import time
import json
import threading
import logging
from datetime import datetime
from collections import defaultdict
import random

# Set up logging to save to a file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("order_processing.log"),  # Log to file
        logging.StreamHandler()  # Optional: Log to console
    ]
)

# Define RabbitMQ connection settings and queue names
RABBITMQ_HOST = 'localhost'
QUEUE_NAMES = {
    'standard': 'standard_orders',
    'express': 'express_orders',
    'priority': 'priority_orders'
}

# Store consumer threads and status
consumer_threads = defaultdict(list)
queue_status = {
    'standard_orders': {'length': 0, 'processing_time': []},
    'express_orders': {'length': 0, 'processing_time': []},
    'priority_orders': {'length': 0, 'processing_time': []}
}

# Centralized scheduler to monitor and adjust consumers
class CentralizedScheduler:
    def __init__(self):
        self.lock = threading.Lock()
    
    def connect(self, queue_name, prefetch_count):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        channel.basic_qos(prefetch_count=prefetch_count)
        return channel, connection

    def process_message(self, ch, method, properties, body, queue_name):
        start_time = datetime.now()
        message = json.loads(body)
        logging.info(f"Processing message from {queue_name}: {message}")
        
        time.sleep(1)  # Simulate processing time
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        # Log processing time
        with self.lock:
            queue_status[queue_name]['processing_time'].append(processing_time)
        
        logging.info(f"Processed message from {queue_name} in {processing_time:.2f} seconds")
        
    def start_consumer(self, queue_name, prefetch_count=10):
        channel, connection = self.connect(queue_name, prefetch_count)
        channel.basic_consume(
            queue=queue_name, 
            on_message_callback=lambda ch, method, properties, body: self.process_message(ch, method, properties, body, queue_name)
        )
        logging.info(f"Starting consumer for {queue_name} with prefetch_count={prefetch_count}")
        channel.start_consuming()
    
    def monitor_and_adjust(self):
        while True:
            with self.lock:
                for queue_name in QUEUE_NAMES.values():
                    # Get current message count in the queue
                    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
                    channel = connection.channel()
                    queue = channel.queue_declare(queue=queue_name, passive=True)
                    queue_length = queue.method.message_count
                    queue_status[queue_name]['length'] = queue_length
                    connection.close()
                
                # Decision-making: Adjust consumers based on queue lengths and processing times
                priority_length = queue_status['priority_orders']['length']
                express_length = queue_status['express_orders']['length']
                standard_length = queue_status['standard_orders']['length']
                
                # Redistribute consumers to prioritize high-priority queues
                if priority_length > 0:
                    # Increase consumers for priority_orders if backlog exists
                    if len(consumer_threads['priority_orders']) < 3:
                        self.add_consumer('priority_orders', prefetch_count=1)
                
                elif express_length > 0 and len(consumer_threads['express_orders']) < 2:
                    # Allocate more consumers to express_orders if needed
                    self.add_consumer('express_orders', prefetch_count=5)
                
                elif standard_length > 5 and len(consumer_threads['standard_orders']) < 1:
                    # Allocate a consumer to standard_orders if backlog builds up
                    self.add_consumer('standard_orders', prefetch_count=10)
                
                # Sleep before re-evaluating the queue status
                time.sleep(5)
    
    def add_consumer(self, queue_name, prefetch_count):
        thread = threading.Thread(target=self.start_consumer, args=(queue_name, prefetch_count))
        consumer_threads[queue_name].append(thread)
        thread.start()
        logging.info(f"Added consumer to {queue_name} with prefetch_count={prefetch_count}")
        
    def log_queue_status(self):
        # Log queue processing times and lengths
        for queue_name, stats in queue_status.items():
            avg_time = sum(stats['processing_time']) / len(stats['processing_time']) if stats['processing_time'] else 0
            logging.info(f"{queue_name} - Length: {stats['length']}, Avg Processing Time: {avg_time:.2f} seconds")

# Producer function to simulate order creation
def publish_order(order_type, order_data):
    channel, connection = scheduler.connect(QUEUE_NAMES[order_type], prefetch_count=1)
    routing_key = f"order.{order_type}"
    message = {
        'order_id': order_data['order_id'],
        'customer_id': order_data['customer_id'],
        'items': order_data['items'],
        'timestamp': datetime.now().isoformat()
    }
    channel.basic_publish(exchange='order_exchange', routing_key=routing_key, body=json.dumps(message))
    logging.info(f"Sent {order_type} order: {message}")
    connection.close()

# Generate test orders and publish to queues
def generate_random_order(order_type):
    order_id = f"{order_type}_{random.randint(1000, 9999)}"
    customer_id = f"cust_{random.randint(100, 999)}"
    items = [f"item_{random.randint(1, 5)}" for _ in range(random.randint(1, 3))]
    return {
        'order_id': order_id,
        'customer_id': customer_id,
        'items': items
    }

if __name__ == '__main__':
    scheduler = CentralizedScheduler()
    
    # Start the scheduler's monitoring thread
    monitoring_thread = threading.Thread(target=scheduler.monitor_and_adjust)
    monitoring_thread.start()
    
    # Simulate publishing orders to the queues
    try:
        while True:
            # Publish random orders to different queues
            order_type = random.choice(['standard', 'express', 'priority'])
            order_data = generate_random_order(order_type)
            publish_order(order_type, order_data)
            
            # Wait a short time before sending the next order
            time.sleep(random.uniform(0.5, 2))
    except KeyboardInterrupt:
        logging.info("Stopped sending orders.")
