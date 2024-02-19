import asyncio
import logging
import json
import os

import pika

# import config
from ..ingest import get_tiled_config, process_file

import tiled_ingestor.rabbitmq.schemas as schemas

TILED_INGEST_PIKA_LOG_LEVEL = os.getenv("TILED_INGEST_PIKE_LOG_LEVEL", logging.CRITICAL)
logging.basicConfig(level=logging.CRITICAL)
logging.getLogger("pika").setLevel(TILED_INGEST_PIKA_LOG_LEVEL)

TILED_INGEST_RMQ_HOST = os.getenv("TILED_INGEST_RMQ_HOST")
TILED_INGEST_RMQ_USER = os.getenv("TILED_INGEST_RMQ_USER")
TILED_INGEST_RMQ_PW = os.getenv("TILED_INGEST_RMQ_PW")

TILED_INGEST_TILED_CONFIG_PATH = os.getenv("TILED_INGEST_TILED_CONFIG_PATH")
tiled_config = get_tiled_config(TILED_INGEST_TILED_CONFIG_PATH)


def process_message(ch, method, properties, body):
    # Decode the JSON message
    message = json.loads(body)
    # Prcess the message
    # TODO: Add your logic here

    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)
    new_file_path = message.get(schemas.NEW_FILE_PATH_KEY)
    assert (
        new_file_path
    ), f"Message received from rabbitMQ does not contain {schemas.NEW_FILE_PATH_KEY}"
    asyncio.run(process_file(new_file_path, tiled_config))


def start_consumer():
    # Connect to RabbitMQ

    credentials = pika.PlainCredentials(TILED_INGEST_RMQ_USER, TILED_INGEST_RMQ_PW)
    parameters = pika.ConnectionParameters(
        TILED_INGEST_RMQ_HOST, credentials=credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # # Declare the queue
    channel.queue_declare(queue=schemas.RABBITMQ_TOMO_QUEUE)

    # Set the prefetch count to limit the number of unacknowledged messages
    channel.basic_qos(prefetch_count=1)

    # Start consuming messages
    channel.basic_consume(
        queue="tomo_reconstruction", on_message_callback=process_message
    )

    # Enter a loop to continuously consume messages
    channel.start_consuming()


if __name__ == "__main__":
    start_consumer()
