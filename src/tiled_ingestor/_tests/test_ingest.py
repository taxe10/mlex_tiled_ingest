# import asyncio
# from unittest.mock import patch

import pytest
# from tiled_ingestor.ingest import process_file, get_tiled_config


@pytest.fixture
def tiled_config(tmpdir):
    return {
        "trees": [
            {
                "path": "/",
                "tree": "catalog",
                "args": {
                    "uri": f"sqlite+aiosqlite:///{tmpdir}/catalog.db",
                    "writable_storage": str(tmpdir / "data"),
                    "readable_storage": [str(tmpdir / "data")],
                    "init_if_not_exists": True,
                },
            }
        ]
    }


# this test doesn't quite work, but since we'll be converting to the
# client soon, I'll wait

# @pytest.mark.asyncio
# async def test_process_file(tiled_config):
#     file_path = "/path/to/file"
#     tiled_config_tree_path = "/"
#     path_prefix = "/"

#     with patch("tiled.catalog.from_uri") as mock_from_uri:
#         # Mock the return value of 'from_uri' function
#         mock_catalog_adapter = mock_from_uri.return_value

#         # Call the process_file function
#         await process_file(
#             file_path,
#             tiled_config,
#             tiled_config_tree_path,
#             path_prefix,
#         )

#         # Assert that the file was registered with the catalog
#         mock_catalog_adapter.get_entry.assert_called_once_with(file_path, prefix=path_prefix)
