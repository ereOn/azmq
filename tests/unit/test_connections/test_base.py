"""
Unit tests for the base connection class.
"""

import asyncio
import pytest

from mock import MagicMock

from azmq.connections.base import BaseConnection
from azmq.errors import ProtocolError


@pytest.yield_fixture
def connection(event_loop):
    class MyConnection(BaseConnection):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.done = asyncio.Event(loop=event_loop)

        async def on_run(self):
            await self.done.wait()

    connection = MyConnection(
        socket_type=b'REQ',
        identity=b'myconnection',
        mechanism=MagicMock(),
        on_ready=MagicMock(),
        on_lost=MagicMock(),
        on_failure=MagicMock(),
        loop=event_loop,
    )
    yield connection
    connection.done.set()


@pytest.mark.asyncio
async def test_set_remote_metadata(connection):
    connection.set_remote_metadata({
        b'socket-TYPE': b'REP',
        b'idEntIty': b'foo',
        b'mycustom': b'bar',
    })


@pytest.mark.asyncio
async def test_set_remote_metadata_invalid_identity(connection):
    with pytest.raises(ProtocolError) as error:
        connection.set_remote_metadata({
            b'socket-type': b'REP',
            b'identity': b'\0invalid',
        })

    assert error.value.fatal


@pytest.mark.asyncio
async def test_set_remote_metadata_invalid_combination(connection):
    with pytest.raises(ProtocolError) as error:
        connection.set_remote_metadata({
            b'socket-type': b'PULL',
        })

    assert error.value.fatal
