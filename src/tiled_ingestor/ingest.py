import asyncio
import logging
import sys

from tiled.catalog.register import identity, register
from tiled.catalog import from_uri
import tiled.config

logger = logging.getLogger(__name__)

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def get_tiled_config(config_path: str):
    return tiled.config.parse_configs(config_path)


async def process_file(
    file_path: str,
    tiled_config: dict,
    tiled_config_tree_path: str = "/",
    path_prefix: str = "/",
):
    """
    Process a file that already exists and register it with tiled as a catalog.
    We looks for a match in the tiled config file based on tiled_config_tree_path. This will be
    the tree that we import to. Should work with folders of TIFF sequence as well as single filed like
    hdf5 or datasets like zarr. But honestly, on tiff sequence is tested.

    Args:
        file_path (str): The path of the file to be processed.
        tiled_config_tree_path (str, optional): The path of the tiled tree configuration. Defaults to "/".
        config_path (str, optional): The path of the configuration file. Defaults to "/deploy/config".
        path_prefix (str, optional): The prefix to be added to the registered path. Defaults to "/".

    Raises:
        AssertionError: If no tiled tree is configured for the provided tree path.
        AssertionError: If the matching tiled tree is not a catalog.

    Returns:
        None
    """

    # find the tree in tiled configuration that matches the provided tiled_tree_path
    matching_tree = next(
        (
            tree
            for tree in tiled_config["trees"]
            if tree["path"] == tiled_config_tree_path
        ),
        None,
    )
    assert (
        matching_tree
    ), f"No tiled tree configured for tree path {tiled_config_tree_path}"
    assert (
        matching_tree["tree"] == "catalog"
    ), f"Matching tiled tree {tiled_config_tree_path} is not a catalog"

    # using thre tree in the configuration, generate a catalog(adapter)
    catalog_adapter = from_uri(
        matching_tree["args"]["uri"],
        readable_storage=matching_tree["args"]["readable_storage"],
        adapters_by_mimetype=matching_tree["args"].get("adapters_by_mimetype"),
    )

    # Register with tiled. This writes entries into the database for all of the nodes down to the data node
    await register(
        catalog=catalog_adapter,
        key_from_filename=identity,
        path=file_path,
        prefix=path_prefix,
        overwrite=False,
    )


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "outside_container":
        # if we're debugging this outside of a container, we might want our
        # own settings
        import dotenv

        dotenv.load_dotenv()
        tiled_config = get_tiled_config("../mlex_tomo_framework/tiled/deploy/config")
        asyncio.run(
            process_file(
                "../mlex_tomo_framework/data/tiled_storage/beamlines/8.3.2/recons/"
                "rec20240207_120829_test_no_xrays_n1313",
                tiled_config,
                path_prefix="/beamlines/8.3.2/recons/",
            )
        )
    else:
        from pprint import pprint
        import os

        pprint(os.environ)
        tiled_config = get_tiled_config(
            "/tiled_storage/beamlines/8.3.2/recons/rec20240207_120829_test_no_xrays_n1313"
        )
        asyncio.run(
            process_file(
                # "/tiled_storage/beamlines/8.3.2/recons/rec20240207_120550_test_no_xrays_n257",
                tiled_config,
                path_prefix="/beamlines/8.3.2/recons/",
            )
        )
