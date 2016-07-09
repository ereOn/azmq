"""
Unit tests for the stream connection class.
"""

import asyncio
import pytest

from io import BytesIO

from mock import MagicMock

from azmq.connections.stream import StreamConnection
from azmq.errors import ProtocolError


@pytest.fixture
def reader():
    reader = BytesIO()

    async def readexactly(count):
        return reader.read(count)

    reader.readexactly = readexactly
    return reader


@pytest.fixture
def writer():
    writer = BytesIO()

    return writer


@pytest.yield_fixture
def connection(event_loop, reader, writer):
    connection = StreamConnection(
        reader=reader,
        writer=writer,
        address='myaddress',
        zap_client=None,
        socket_type=b'REQ',
        identity=b'myconnection',
        mechanism=MagicMock(),
        on_ready=MagicMock(),
        on_lost=MagicMock(),
        on_failure=MagicMock(),
        loop=event_loop,
    )
    yield connection
    connection.close()


@pytest.mark.asyncio
async def test_invalid_signature_first_byte(reader):
    reader.write(b'invalid')
    reader.seek(0)

    with pytest.raises(ProtocolError) as error:
        await StreamConnection._read_first_greeting(reader=reader)

    assert error.value.fatal


@pytest.mark.asyncio
async def test_invalid_signature_last_byte(reader):
    reader.write(b'\xff\x00\x00\x00\x00\x00\x00\x00\x00\xaa')
    reader.seek(0)

    with pytest.raises(ProtocolError) as error:
        await StreamConnection._read_first_greeting(reader=reader)

    assert error.value.fatal


@pytest.mark.asyncio
async def test_invalid_signature_low_version(reader):
    reader.write(b'\xff\x00\x00\x00\x00\x00\x00\x00\x00\x7f\x02')
    reader.seek(0)

    with pytest.raises(ProtocolError) as error:
        await StreamConnection._read_first_greeting(reader=reader)

    assert error.value.fatal
