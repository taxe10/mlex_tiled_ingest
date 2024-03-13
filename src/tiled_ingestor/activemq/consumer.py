import asyncio
from collections import deque
import json
from pathlib import Path
from time import sleep
import os

import stomp

from .schemas import DIAMOND_FILEPATH_KEY, DIAMOND_STATUS_KEY
from ..ingest import get_tiled_config, process_file
import logging

TILED_INGEST_TILED_CONFIG_PATH = os.getenv("TILED_INGEST_TILED_CONFIG_PATH")
STOMP_SERVER = os.getenv("STOMP_SERVER")
STOMP_LOG_LEVEL = os.getenv("STOMP_LOG_LEVEL", "INFO")
STOMP_TOPIC_NAME = os.getenv("STOMP_TOPIC_NAME", "INFO")

logging.getLogger("stomp").setLevel(logging.getLevelName(STOMP_LOG_LEVEL))
logging.getLogger("asyncio").setLevel(logging.INFO)
logging.getLogger("aiosqlite").setLevel(logging.INFO)

logger = logging.getLogger("activemq_consumer")
logger.info(f"TILED_INGEST_TILED_CONFIG_PATH: {TILED_INGEST_TILED_CONFIG_PATH} ")
logger.info(f"STOMPSERVER: {STOMP_SERVER} ")
logger.info(f"STOMP_TOPIC_NAME: {STOMP_TOPIC_NAME} ")


class ScanListener(stomp.ConnectionListener):
    def __init__(self):
        self.messages = deque()

    def on_error(self, message):
        print("received an error %s" % message)

    def on_message(self, message):
        # this might be a difference in version of stomp from what
        # diamond is using. In their example, messsage and headers are
        # separate parameters. But in the version I'm using, the message
        # is an object that contains body and headers
        ob = json.loads(message.body)
        logger.info(f"Received message: {ob}")
        self.messages.append(ob['filePath'])

def start_consumer():
    tiled_config = get_tiled_config(TILED_INGEST_TILED_CONFIG_PATH)
    conn = stomp.Connection([(STOMP_SERVER, 61613)])
    scan_listener = ScanListener()
    conn.set_listener("", scan_listener)
    conn.connect()
    conn.subscribe(destination=STOMP_TOPIC_NAME, id=1, ack="auto")
    while True:
        if scan_listener.messages:
            new_file_path = scan_listener.messages.popleft()
            try:
                logger.info(f"Ingesting file: {new_file_path}")
                # we get a path to the nexus file, but we want the TiffSaver_3 file next to it
                nxs_path = Path(new_file_path)
                tiff_path = nxs_path.parent / Path("TiffSaver_3")
                asyncio.run(process_file(str(tiff_path), tiled_config, path_prefix="reconstructions"))
            except Exception as e:
                print("Failed to process file " + new_file_path)
                print(str(e))

        sleep(1)


if __name__ == "__main__":
    start_consumer()
