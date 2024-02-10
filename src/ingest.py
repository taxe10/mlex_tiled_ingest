import asyncio
import logging
from pathlib import Path
import sys

from tiled.catalog.register import register
from tiled.catalog import from_uri
import tiled.config

logger = logging.getLogger(__name__)

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


async def process_file(file_path: str, tiled_tree_path: str = "/"):
    
    config = tiled.config.parse_configs("/deploy/config")
    # find the tree in configuration that matches the provided tiled_tree_path
    matching_tree = next(
        (tree for tree in config["trees"] if tree["path"] == tiled_tree_path), None
    )
    assert matching_tree, f"No tiled tree configured for tree path {tiled_tree_path}"
    assert (
        matching_tree["tree"] == "catalog"
    ), f"Matching tiled tree {tiled_tree_path} is not a catalog"

    catalog_adapter = from_uri(
        matching_tree["args"]["uri"],
        readable_storage=matching_tree["args"]["readable_storage"],
        adapters_by_mimetype=matching_tree["args"].get("adapters_by_mimetype"),
    )
    response = await register(catalog=catalog_adapter, path=Path(file_path))
    print(response)

asyncio.run(process_file("/tiled_storage/beamlines/8.3.2/recons/rec20240207_120550_test_no_xrays_n257/"))
# asyncio.run(process_file("/tiled_storage/test1"))
