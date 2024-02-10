import json
import logging
import sys

import pika
from pika import DeliveryMode
from pika.exchange_type import ExchangeType

from schemas import NEW_FILE_PATH_KEY


logging.basicConfig(level=logging.INFO)

def send_message(new_file: str):
    json_message = json.dumps({NEW_FILE_PATH_KEY: new_file})

    credentials = pika.PlainCredentials("guest", "guest")
    parameters = pika.ConnectionParameters("localhost", credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(
        exchange="test_exchange",
        exchange_type=ExchangeType.direct,
        passive=False,
        durable=True,
        auto_delete=False,
    )

    print("Sending message to create a queue")
    channel.basic_publish(
        "mlexchange_exchange",
        "tomo_reconstruction",
        json_message,
        pika.BasicProperties(
            content_type="application/json", delivery_mode=DeliveryMode.Transient
        ),
    )
    connection.close()


if __name__ == "__main__":
    new_file = sys.argv[1]
