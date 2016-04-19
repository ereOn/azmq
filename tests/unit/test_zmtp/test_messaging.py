"""
Unit tests for the messaging utilities.
"""

import pytest

from io import BytesIO

from azmq.zmtp.errors import ZMTPFrameInvalid
from azmq.zmtp.messaging import (
    read_greeting,
    read_short_greeting,
    write_greeting,
    write_short_greeting,
)


def test_write_short_greeting():
    buffer = BytesIO()
    write_short_greeting(buffer, 3)
    buffer.seek(0)
    assert buffer.read() == b'\xff\x00\x00\x00\x00\x00\x00\x00\x00\x7f\x03'


def test_write_greeting():
    buffer = BytesIO()
    write_greeting(buffer, (3, 1), 'plain', False)
    buffer.seek(0)
    assert buffer.read() == (
        b'\xff\x00\x00\x00\x00\x00\x00\x00\x00\x7f\x03\x01'
        b'plain\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
    )


def test_read_short_greeting_too_short():
    buffer = BytesIO(b'\xff\x00\x00\x00\x00\x00\x00\x00\x00\x7f')

    with pytest.raises(ZMTPFrameInvalid):
        read_short_greeting(buffer)


def test_read_short_greeting_invalid_signature():
    buffer = BytesIO(b'\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x7f\x03')

    with pytest.raises(ZMTPFrameInvalid):
        read_short_greeting(buffer)


def test_read_short_greeting():
    buffer = BytesIO(b'\xff\x00\x00\x00\x00\x00\x00\x00\x00\x7f\x03')
    major_version = read_short_greeting(buffer)
    assert major_version == 3


def test_read_greeting():
    buffer = BytesIO(
        b'\xff\x00\x00\x00\x00\x00\x00\x00\x00\x7f\x03\x01'
        b'plain\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
    )
    version, mechanism, as_server = read_greeting(buffer)
    assert version == (3, 1)
    assert mechanism == 'plain'
    assert as_server is False
