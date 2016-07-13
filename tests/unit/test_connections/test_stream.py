"""
Unit tests for the stream connection class.
"""

import pytest

from mock import MagicMock

from azmq.connections.stream import StreamConnection
from azmq.errors import ProtocolError


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


@pytest.mark.asyncio
async def test_process_command_subscribe():
    self_ = MagicMock()
    self_.version = (3, 1)

    async def subscribe(*args):
        self_.check(*args)

    self_.subscribe = subscribe

    await StreamConnection._process_command(self_, b'SUBSCRIBE', b'data')

    self_.check.assert_called_once_with(b'data')


@pytest.mark.asyncio
async def test_process_command_cancel():
    self_ = MagicMock()
    self_.version = (3, 1)

    async def unsubscribe(*args):
        self_.check(*args)

    self_.unsubscribe = unsubscribe

    await StreamConnection._process_command(self_, b'CANCEL', b'data')

    self_.check.assert_called_once_with(b'data')


@pytest.mark.asyncio
async def test_process_command_ping(connection):
    connection.version = (3, 1)
    connection._expect_ping_timeout = MagicMock()

    await connection._process_command(b'PING', b'\x01\x00data')

    connection._expect_ping_timeout.revive.assert_called_once_with()


@pytest.mark.asyncio
async def test_process_command_pong(connection):
    connection.version = (3, 1)
    connection._pending_ping_contexts = {b'data'}

    await connection._process_command(b'PONG', b'data')

    assert connection._pending_ping_contexts == set()


@pytest.mark.asyncio
async def test_process_command_unknown(connection):
    connection.version = (3, 1)

    await connection._process_command(b'COMMAND', b'data')


@pytest.mark.asyncio
async def test_process_command_bad_version(connection):
    connection.version = (3, 0)

    await connection._process_command(b'COMMAND', b'data')


@pytest.mark.asyncio
async def test_send_ping(connection):
    connection.version = (3, 1)
    connection._base_ping_context = 0

    await connection._send_ping()

    assert connection._base_ping_context == 1
    assert connection._pending_ping_contexts == {b'\x00\x00\x00\x00'}


@pytest.mark.asyncio
async def test_send_ping_too_many_pings_already(connection):
    connection.version = (3, 1)
    connection._max_pending_pings = 0

    await connection._send_ping()

    assert connection._pending_ping_contexts == set()


@pytest.mark.asyncio
async def test_send_ping_ping_context_overlap(connection):
    connection.version = (3, 1)
    connection._base_ping_context = 2 ** 32 - 1

    await connection._send_ping()

    assert connection._base_ping_context == 0
    assert connection._pending_ping_contexts == {b'\xff\xff\xff\xff'}
