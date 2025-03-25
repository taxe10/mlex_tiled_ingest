import asyncio
import logging
import os

import watchgod
from dotenv import load_dotenv
from tiled.client import from_uri

from tiled_ingestor.file_watcher.file_handling import add_scan_tiled

load_dotenv()
PATH_TO_RAW_DATA = os.getenv("PATH_TO_RAW_DATA")
# Initialize the Tiled server
TILED_URI = os.getenv("TILED_URI", "")
TILED_API_KEY = os.getenv("TILED_API_KEY")

logger = logging.getLogger("data_watcher")
logger.setLevel("INFO")
logname = "data_watcher"
logger.addHandler(logging.StreamHandler())

try:
    logger.info(f"Connecting to Tiled at {TILED_URI}")
    client = from_uri(TILED_URI, api_key=TILED_API_KEY)
    TILED_BASE_URI = client.uri
except Exception as e:
    print(e)


async def post_file_created(dataset_path):
    logger.info(f"Adding {dataset_path} to Tiled detected at {PATH_TO_RAW_DATA}")
    input_file_uri = add_scan_tiled(client, PATH_TO_RAW_DATA, dataset_path)
    logger.info(f"Added {input_file_uri} to Tiled")
    pass


async def watch_directory():
    logger.info(f"Watching directory {PATH_TO_RAW_DATA}")
    async for changes in watchgod.awatch(PATH_TO_RAW_DATA):
        for change in changes:
            logger.info(change)
            logger.info(f"Detected change in {change[1]}")
            if change[0] != watchgod.Change.added:
                continue
            if ".tmp" in change[1]:
                continue
            # if Path(change[1]).suffix != ".gb":
            #     continue
            dataset_path = change[1]
            await post_file_created(dataset_path)


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError as e:
        if str(e).startswith("There is no current event loop in thread"):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        else:
            raise

    loop = asyncio.get_event_loop()
    loop.run_until_complete(watch_directory())
