import json
import logging
import os
import sys

import pika
from pika import DeliveryMode

# from pika.exchange_type import ExchangeType

# import config
import schemas

TILED_INGEST_PIKA_LOG_LEVEL = os.getenv("TILED_INGEST_PIKE_LOG_LEVEL", logging.CRITICAL)
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("pika").setLevel(TILED_INGEST_PIKA_LOG_LEVEL)


TILED_INGEST_RMQ_HOST = os.getenv("TILED_INGEST_RMQ_HOST")
TILED_INGEST_RMQ_USER = os.getenv("TILED_INGEST_RMQ_USER")
TILED_INGEST_RMQ_PW = os.getenv("TILED_INGEST_RMQ_PW")


def send_message(new_file: str):
    logging.info(f"Received request for  {new_file}")
    json_message = json.dumps({schemas.NEW_FILE_PATH_KEY: new_file})

    credentials = pika.PlainCredentials(TILED_INGEST_RMQ_USER, TILED_INGEST_RMQ_PW)
    parameters = pika.ConnectionParameters(
        TILED_INGEST_RMQ_HOST, credentials=credentials
    )
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
        "",
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
