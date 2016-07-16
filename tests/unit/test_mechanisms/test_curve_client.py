"""
Unit tests for the curve client mechanism class.
"""

import pytest

from azmq.crypto import curve_gen_keypair
from azmq.mechanisms.curve_client import CurveClient
from azmq.errors import ProtocolError

from ...conftest import requires_libsodium


@pytest.fixture
def mechanism():
    public_key, _ = curve_gen_keypair()
    return CurveClient(
        server_key=public_key,
    )()


@requires_libsodium
@pytest.mark.asyncio
async def test_read_curve_welcome_invalid(reader, mechanism):
    reader.write(b'\x04\x07\x07WELCOME')
    reader.seek(0)

    with pytest.raises(ProtocolError):
        await mechanism._read_curve_welcome(reader=reader)


@requires_libsodium
@pytest.mark.asyncio
async def test_read_curve_welcome_invalid_box(reader, mechanism):
    reader.write(b'\x04\xa8\x07WELCOME' + b'\0' * 160)
    reader.seek(0)

    with pytest.raises(ProtocolError):
        await mechanism._read_curve_welcome(reader=reader)
