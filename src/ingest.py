import asyncio
import logging
from pathlib import Path
import sys

from tiled.catalog.register import identity, register
from tiled.catalog import from_uri
import tiled.config

logger = logging.getLogger(__name__)

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


async def process_file(
    file_path: str,
    tiled_config_tree_path: str = "/",
    config_path: str = "/deploy/config",
    path_prefix: str = "/"
):
    config = tiled.config.parse_configs(config_path)
    # find the tree in configuration that matches the provided tiled_tree_path
    matching_tree = next(
        (tree for tree in config["trees"] if tree["path"] == tiled_config_tree_path), None
    )
    assert matching_tree, f"No tiled tree configured for tree path {tiled_config_tree_path}"
    assert (
        matching_tree["tree"] == "catalog"
    ), f"Matching tiled tree {tiled_config_tree_path} is not a catalog"

    catalog_adapter = from_uri(
        matching_tree["args"]["uri"],
        readable_storage=matching_tree["args"]["readable_storage"],
        adapters_by_mimetype=matching_tree["args"].get("adapters_by_mimetype")
    )
    await register(
        catalog=catalog_adapter,
        key_from_filename=identity,
        path=file_path,
        prefix=path_prefix)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "outside_container":
        # if we're debugging this outside of a container, we might want our
        # own settings
        import dotenv
        dotenv.load_dotenv()
        asyncio.run(
            process_file(
                "/tiled_storage/beamlines/8.3.2/recons/rec20240207_120550_test_no_xrays_n257",
                config_path="../mlex_tomo_framework/tiled/deploy/config",
                path_prefix="/tiled_storage/beamlines/8.3.2/recons/"
            )
        )
    else:
        from pprint import pprint
        import os
        pprint(os.environ)
        asyncio.run(
            process_file(
                "/tiled_storage/beamlines/8.3.2/recons/rec20240207_120550_test_no_xrays_n257",
                path_prefix="/tiled_storage/beamlines/8.3.2/recons/"
            )
        )

