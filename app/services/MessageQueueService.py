import json
import pika
from typing import Callable
import os
import threading

class MessageQueueService:
    """
    Abstracts RabbitMQ operations.
    Allows registering callbacks for incoming messages, and publishing messages.
    """
    def __init__(self, rabbitmq_url: str | None = None):
        if not rabbitmq_url:
            rabbitmq_url = os.getenv("RABBITMQ_URL", None)
        rabbitmq_url = rabbitmq_url or "amqp://guest:guest@localhost:5672/"
        self.rabbitmq_url = rabbitmq_url

        params = pika.URLParameters(self.rabbitmq_url)
        params.heartbeat = 0 # turns heartbeat off
        params.blocked_connection_timeout = 30
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        self.lock = threading.Lock()
    
    def clone(self):
        with self.lock:
            return MessageQueueService(self.rabbitmq_url)

    def declare_queue(self, queue_name: str):
        self.channel.queue_declare(queue=queue_name, durable=True)

    def publish_message(self, queue_name: str, message: dict):
        self.channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # persistent
            )
        )

    def register_callback(self, queue_name: str, callback: Callable[[dict], None]):
        """
        Registers a callback for a queue. The callback receives the deserialized JSON message.
        """
        def _internal_callback(ch, method, properties, body):
            try:
                message = json.loads(body)
                callback(message)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                print(f"[MessageQueueService] Error processing message: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=queue_name, on_message_callback=_internal_callback)

    def start_consuming(self):
        print("[MessageQueueService] Starting consumption...")
        self.channel.start_consuming()
