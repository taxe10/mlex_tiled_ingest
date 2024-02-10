import asyncio
import pika
import json

from ..ingest import process_file
from .schemas import NEW_FILE_PATH_KEY


def process_message(ch, method, properties, body):
    # Decode the JSON message
    message = json.loads(body)
    # Prcess the message
    # TODO: Add your logic here

    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)
    new_file_path = message.get(NEW_FILE_PATH_KEY)
    assert (
        new_file_path
    ), f"Message received from rabbitMQ does not contain {NEW_FILE_PATH_KEY}"
    asyncio.run(process_file(new_file_path))


# Connect to RabbitMQ
credentials = pika.PlainCredentials("guest", "guest")
parameters = pika.ConnectionParameters("localhost", credentials=credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# Declare the queue
channel.queue_declare(queue="tomo_reconstruction")

# Set the prefetch count to limit the number of unacknowledged messages
channel.basic_qos(prefetch_count=1)

# Start consuming messages
channel.basic_consume(queue="tomo_reconstruction", on_message_callback=process_message)

# Enter a loop to continuously consume messages
channel.start_consuming()
