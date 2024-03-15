import asyncio
import logging
import sys

import h5py
from tiled.catalog.register import identity, register
from tiled.catalog import from_uri
import tiled.config
from tiled.adapters.hdf5 import HDF5Adapter, SWMR_DEFAULT
from tiled.structures.core import Spec
from tiled.utils import path_from_uri

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

DLS_TOMO_SPEC = Spec("DLSNXTomo", "1")

# In a future version of tiled, the register method will move from
# server to client. This will allow us to remove a lot of code here, including
# looking for the server config, connecting to the database, writing an
# adapter that serves just to mark files with a particular spec.
# See https://github.com/bluesky/tiled/pull/661


def get_tiled_config(config_path: str):
    return tiled.config.parse_configs(config_path)


def diamond_tomo_h5_read_adapter(
    data_uri, swmr=SWMR_DEFAULT, libver="latest", specs=None, **kwargs
):
    # this serves as enough of an adapter to read a file, verify that it's
    # the right type, and register a spec for it. It's like a piece of an
    # adapter that wouldn't work for reading data set, but works for adding
    # a spec to a dataset when registered externally.
    specs = specs or []
    specs.append(DLS_TOMO_SPEC)
    filepath = path_from_uri(data_uri)
    file = h5py.File(filepath, "r", swmr=swmr, libver=libver)
    return HDF5Adapter.from_file(file, specs=specs, **kwargs)


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

    # using the tree in the configuration, generate a catalog(adapter)
    catalog_adapter = from_uri(
        matching_tree["args"]["uri"],
        readable_storage=matching_tree["args"]["readable_storage"],
    )

    # Register with tiled. This writes entries into the database for all of the nodes down to the data node
    await register(
        catalog=catalog_adapter,
        key_from_filename=identity,
        path=file_path,
        prefix=path_prefix,
        overwrite=False,
        adapters_by_mimetype={
            "application/x-hdf5": "tiled_ingestor.ingest:diamond_tomo_h5_read_adapter"
        },
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
                "../mlex_tomo_framework/data/tiled_storage/recons/nexus-example.nxs",
                tiled_config,
                path_prefix="/recons",
            )
        )
    else:
        from pprint import pprint
        import os

        pprint(os.environ)
        config_path = os.getenv("TILED_INGEST_TILED_CONFIG_PATH")
        tiled_config = get_tiled_config("../mlex_tomo_framework/tiled_conda/deploy/config")
        asyncio.run(
            process_file(
               # "/dls/k11/data/2024/mg37376-2/processing/mg32801-1/processed/Savu_k11-37074_full_fd_Fresnel_rmrings_vo_AST_tiff/TiffSaver_5",
               # "/dls/tmp/mlex/mlex_tomo_framework/data/tiled_storage/recons/rec20240207_120550_test_no_xrays_n257",
               # "/dls/k11/data/2024/mg37376-1/processed/Savu_k11-38639_3x_fd_vo_AST_tiff/TiffSaver_3",
                "/dls/k11/data/2024/mg37376-1/processing/i23_data/tiff",
                tiled_config,
                path_prefix="reconstructions",
            )
        )
