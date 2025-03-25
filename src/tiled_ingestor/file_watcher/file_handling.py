import logging
import os
from pathlib import Path

import numpy as np
from tiled.structures.array import ArrayStructure, BuiltinDtype
from tiled.structures.core import Spec, StructureFamily
from tiled.structures.data_source import Asset, DataSource, Management
from tiled.utils import ensure_uri

logger = logging.getLogger("data_watcher")
logger.setLevel("INFO")
logname = "data_watcher"
logger.addHandler(logging.StreamHandler())


def parse_txt_accompanying_edf(filepath):
    """Pase a .txt file produced at ALS beamline 7.3.3 into a dictionary.

    Parameters
    ----------
    filepath: str or Path
        Filepath of the .edf file.
    """
    txt_filepath = None
    if isinstance(filepath, str):
        txt_filepath = filepath.replace(".edf", ".txt")
    if isinstance(filepath, Path):
        txt_filepath = filepath.with_suffix(".txt")

    # File does not exist, return empty dictionary
    if not os.path.isfile(txt_filepath):
        return dict()

    with open(txt_filepath, "r") as file:
        lines = file.readlines()

    # Some lines have the format
    # key: value
    # others are just values with no key
    keyless_lines = 0
    txt_params = dict()
    for line in lines:
        line_components = list(map(str.strip, line.split(":", maxsplit=1)))
        if len(line_components) >= 2:
            txt_params[line_components[0]] = line_components[1]
        else:
            if line_components[0] != "!0":
                txt_params[f"Keyless Parameter #{keyless_lines}"] = line_components[0]
                keyless_lines += 1
    return txt_params


def add_scan_tiled(client, path_to_raw_data, scan_filepath):
    common_path = os.path.commonpath([path_to_raw_data, scan_filepath])
    if common_path is None:
        return None

    relative_scan_filepath = os.path.relpath(scan_filepath, path_to_raw_data)
    scan_container, scan = os.path.split(relative_scan_filepath)
    scan_container_parts = os.path.normpath(scan_container).split(os.sep)

    current_container_client = client["raw"]
    for part in scan_container_parts:
        if part in current_container_client:
            current_container_client = current_container_client[part]
        else:
            current_container_client = current_container_client.create_container(
                key=part
            )
    key = os.path.splitext(scan)[0]

    if key in current_container_client:
        current_container_client.delete(key)

    structure = ArrayStructure(
        data_type=BuiltinDtype.from_numpy_dtype(
            np.dtype("float32") if scan_filepath.endswith(".gb") else np.dtype("int32")
        ),
        shape=(1679, 1475),
        chunks=((1679,), (1475,)),
    )

    if scan_filepath.endswith(".edf"):
        metadata = parse_txt_accompanying_edf(scan_filepath)
    else:
        metadata = {}

    # TODO: Add metadata and spec

    scan_client = current_container_client.new(
        key=key,
        structure_family=StructureFamily.array,
        data_sources=[
            DataSource(
                management=Management.external,
                mimetype=(
                    "application/x-gb"
                    if scan_filepath.endswith(".gb")
                    else "application/x-edf"
                ),
                structure_family=StructureFamily.array,
                structure=structure,
                assets=[
                    Asset(
                        data_uri=ensure_uri(scan_filepath),
                        is_directory=False,
                        parameter="data_uri",
                    )
                ],
            ),
        ],
        metadata=metadata,
        specs=[Spec("gb") if scan_filepath.endswith(".gb") else Spec("edf")],
    )
    return scan_client.uri
