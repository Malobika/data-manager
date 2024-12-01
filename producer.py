import pika
import json
import time
from datetime import datetime
import random

# RabbitMQ configuration
RABBITMQ_HOST = 'localhost'
EXCHANGE_NAME = 'order_exchange'
ROUTING_KEYS = {
    'standard': 'order.standard',
    'express': 'order.express',
    'priority': 'order.priority'
}

# Connect to RabbitMQ and declare the exchange
def connect():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic')
    return channel, connection

# Publish a message to RabbitMQ
def publish_order(order_type, order_data):
    channel, connection = connect()
    routing_key = ROUTING_KEYS[order_type]
    message = {
        'order_id': order_data['order_id'],
        'customer_id': order_data['customer_id'],
        'items': order_data['items'],
        'timestamp': datetime.now().isoformat()
    }
    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=routing_key, body=json.dumps(message))
    print(f"Sent {order_type} order: {message}")
    connection.close()

# Generate random orders and send them to RabbitMQ
def generate_random_order(order_type):
    order_id = f"{order_type}_{random.randint(1000, 9999)}"
    customer_id = f"cust_{random.randint(100, 999)}"
    items = [f"item_{random.randint(1, 5)}" for _ in range(random.randint(1, 3))]
    return {
        'order_id': order_id,
        'customer_id': customer_id,
        'items': items
    }

# Main function to continuously send test messages
if __name__ == '__main__':
    try:
        while True:
            # Randomly choose an order type to simulate different workloads
            order_type = random.choice(['standard', 'express', 'priority'])
            order_data = generate_random_order(order_type)
            publish_order(order_type, order_data)
            
            # Wait for a short time before sending the next message
            time.sleep(random.uniform(0.5, 2))
    except KeyboardInterrupt:
        print("Stopped sending orders.")
