"""
Unit tests for the messaging utilities.
"""

import pytest

from io import BytesIO

from azmq.zmtp.errors import ZMTPFrameInvalid
from azmq.zmtp.messaging import (
    write_first_greeting,
)


def test_write_first_greeting():
    buffer = BytesIO()
    write_first_greeting(buffer, 3)
    buffer.seek(0)
    assert buffer.read() == b'\xff\x00\x00\x00\x00\x00\x00\x00\x00\x7f\x03'
