import asyncio
import logging
import os
from pathlib import Path

from tiled.catalog.register import register
from tiled.catalog import from_uri
import tiled.config

logger = logging.getLogger(__name__)

TILED_URL = os.getenv("TILED_URL", "http://127.0.0.1:8000")
TILED_API_KEY = os.getenv("TILED_SINGLE_USER_API_KEY")


def register_file(path: str):
    config = tiled.config.parse_configs("../mlex_tomo_framework/tiled/deploy/config")
    first_tree = config['trees'][0]
    assert first_tree['tree'] == 'catalog'
    catalog_adapter = from_uri(
        first_tree['args']['uri'],
        readable_storage=first_tree['args']['readable_storage'],
        adapters_by_mimetype=config['media_types'] or None,
    )

    asyncio.run(register(
        catalog=catalog_adapter,
        path=Path(path)
    ))


register_file("./test.csv")