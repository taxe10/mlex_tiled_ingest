import pytest
from unittest.mock import MagicMock
from collections import deque

from tiled_ingestor.activemq.consumer import ScanListener


@pytest.fixture
def listener():
    return ScanListener()


def test_on_error(listener):
    message = "Error message"
    listener.on_error(message)
    assert listener.messages == deque()


def test_on_message(listener, monkeypatch):
    message = MagicMock()
    message.body = '{"status": "COMPLETE", "filePath": "/path/to/file"}'


    listener.on_message(message)
    assert listener.messages == deque(['/path/to/file'])

