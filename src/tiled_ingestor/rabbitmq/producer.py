import json
import logging
import sys

import pika
from pika import DeliveryMode
# from pika.exchange_type import ExchangeType

import schemas

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('pika').setLevel(logging.CRITICAL)


def send_message(new_file: str):
    logging.info(f"!!!!!!!!!!    {new_file}")
    json_message = json.dumps({schemas.NEW_FILE_PATH_KEY: new_file})

    credentials = pika.PlainCredentials("guest", "guest")
    parameters = pika.ConnectionParameters("localhost", credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    # channel.exchange_declare(
    #     exchange=schemas.RABBITMQ_EXCHANGE,
    #     exchange_type=ExchangeType.direct,
    #     passive=False,
    #     durable=True,
    #     auto_delete=False,
    # )

    logging.info(f"Sending {json_message} to queue {schemas.RABBITMQ_TOMO_QUEUE}")
    channel.basic_publish(
        '',
        schemas.RABBITMQ_TOMO_QUEUE,
        json_message,
        pika.BasicProperties(
            content_type="application/json", delivery_mode=DeliveryMode.Transient
        ),
    )
    connection.close()


if __name__ == "__main__":
    if len(sys.argv) == 1:
        new_file = "../mlex_tomo_framework/data/tiled_storage/recons/rec20240207_120829_test_no_xrays_n1313"
    else:
        new_file = sys.argv[1]
    send_message(new_file=new_file)
