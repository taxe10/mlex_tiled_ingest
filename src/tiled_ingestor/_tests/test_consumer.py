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

    def mock_process_file(file_path):
        assert file_path == "/path/to/file"

    monkeypatch.setattr(process_file, "delay", mock_process_file)

    listener.on_message(message)
    assert listener.messages == deque(['/path/to/file'])

