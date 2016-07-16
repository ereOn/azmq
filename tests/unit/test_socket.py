"""
Unit tests for containers.
"""

from mock import MagicMock

import asyncio
import pytest

from azmq.context import Context
from azmq.socket import Socket
from azmq.errors import UnsupportedSchemeError


@pytest.yield_fixture
def context(event_loop):
    context = Context(loop=event_loop)
    yield context
    context.close()


def test_socket_invalid_socket_type(context):
    with pytest.raises(ValueError):
        Socket(context=context, socket_type=b'FOO')


def test_socket_invalid_connect(context):
    socket = Socket(context=context, socket_type=b'REQ')

    with pytest.raises(UnsupportedSchemeError):
        socket.connect('foo://bar')


def test_socket_invalid_bind(context):
    socket = Socket(context=context, socket_type=b'REQ')

    with pytest.raises(UnsupportedSchemeError):
        socket.bind('foo://bar')


@pytest.mark.asyncio
async def test_socket_bind_on_connection_failure(context):
    async with Socket(context=context, socket_type=b'REQ') as socket:
        socket.bind('tcp://0.0.0.0:3333', on_connection_failure=MagicMock())


def test_socket_generate_identity(context):
    socket = Socket(context=context, socket_type=b'REQ')
    socket._base_identity = 2 ** 32 - 1
    result = socket.generate_identity()
    assert result == b'\x00\xff\xff\xff\xff'
    result = socket.generate_identity()
    assert result == b'\x00\x00\x00\x00\x00'
    result = socket.generate_identity()
    assert result == b'\x00\x00\x00\x00\x01'


@pytest.mark.asyncio
async def test_router_socket_send_no_connection(context):
    async with Socket(context=context, socket_type=b'ROUTER') as socket:
        await socket.send_multipart([b'hey'])


@pytest.mark.asyncio
async def test_sub_socket_send(context):
    async with Socket(context=context, socket_type=b'SUB') as socket:
        with pytest.raises(AssertionError):
            await socket.send_multipart([b'hey'])


@pytest.mark.asyncio
async def test_pub_socket_recv(context):
    async with Socket(context=context, socket_type=b'PUB') as socket:
        with pytest.raises(AssertionError):
            await socket.recv_multipart()


@pytest.mark.asyncio
async def test_xsub_socket_send_empty(context):
    async with Socket(context=context, socket_type=b'XSUB') as socket:
        await socket.send_multipart([])


@pytest.mark.asyncio
async def test_xsub_socket_send_empty_subscription(context):
    async with Socket(context=context, socket_type=b'XSUB') as socket:
        await socket.send_multipart([b''])


@pytest.mark.asyncio
async def test_xsub_socket_send_invalid_subscription(context):
    async with Socket(context=context, socket_type=b'XSUB') as socket:
        await socket.send_multipart([b'\2foo'])


@pytest.mark.asyncio
async def test_router_socket_subscribe(context):
    async with Socket(context=context, socket_type=b'ROUTER') as socket:
        with pytest.raises(AssertionError):
            await socket.subscribe(b'topic')


@pytest.mark.asyncio
async def test_router_socket_unsubscribe(context):
    async with Socket(context=context, socket_type=b'ROUTER') as socket:
        with pytest.raises(AssertionError):
            await socket.unsubscribe(b'topic')
