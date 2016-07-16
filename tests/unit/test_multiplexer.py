"""
Unit tests for the multiplexer.
"""

import pytest

from mock import MagicMock

from azmq.multiplexer import Multiplexer


@pytest.mark.asyncio
async def test_add_socket_twice(event_loop):
    socket = MagicMock()
    multiplexer = Multiplexer(loop=event_loop)
    multiplexer.add_socket(socket)
    multiplexer.add_socket(socket)

    assert multiplexer._sockets == {socket}


@pytest.mark.asyncio
async def test_remove_unadded_socket(event_loop):
    socket = MagicMock()
    multiplexer = Multiplexer(loop=event_loop)
    multiplexer.remove_socket(socket)

    assert multiplexer._sockets == set()


@pytest.mark.asyncio
async def test_recv_multipart_no_sockets(event_loop):
    multiplexer = Multiplexer(loop=event_loop)
    result = await multiplexer.recv_multipart()

    assert result == []
