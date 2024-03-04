import json
import logging
import os
import sys

import stomp

from .schemas import DIAMOND_FILEPATH_KEY, DIAMOND_STATUS_KEY, STOMP_TOPIC_NAME

STOMP_LOG_LEVEL = os.getenv("STOMP_LOG_LEVEL", "INFO")
STOMP_SERVER = os.getenv("STOMP_SERVER")
logging.getLogger("stomp").setLevel(logging.getLevelName(STOMP_LOG_LEVEL))


def send_message(new_file: str):
    logging.info(f"Received request for  {new_file}")
    json_message = json.dumps(
        {DIAMOND_STATUS_KEY: "COMPLETE", DIAMOND_FILEPATH_KEY: new_file}
    )

    conn = stomp.Connection([(STOMP_SERVER, 61613)])
    conn.connect(wait=True)

    conn.send(
        body=json_message,
        destination=STOMP_TOPIC_NAME,
        headers={"content-type": "application/json"},
    )

    conn.disconnect()


if __name__ == "__main__":
    if len(sys.argv) == 1:
        new_file = "../mlex_tomo_framework/data/tiled_storage/recons/rec20240207_120829_test_no_xrays_n1313"
    else:
        new_file = sys.argv[1]
    send_message(new_file=new_file)
