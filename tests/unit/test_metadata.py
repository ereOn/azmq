"""
Unit tests for metadata.
"""

import pytest

from azmq.errors import ProtocolError
from azmq.metadata import buffer_to_metadata


def test_buffer_to_metadata_empty():
    result = buffer_to_metadata(b'')
    assert result == {}


def test_buffer_to_metadata_invalid_value_size():
    buffer = b'\x03foo\x00\x00\x00\x03bar'
    result = buffer_to_metadata(buffer)
    assert result == {b'foo': b'bar'}


def test_buffer_to_metadata_invalid_name_size():
    buffer = b'\x03'

    with pytest.raises(ProtocolError):
        buffer_to_metadata(buffer)


def test_buffer_to_metadata_invalid_value_size():
    buffer = b'\x03foo\x00\x00\x00\x03ba'

    with pytest.raises(ProtocolError):
        buffer_to_metadata(buffer)
