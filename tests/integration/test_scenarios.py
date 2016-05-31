"""
Inproc integration tests.
"""

import asyncio
import pytest

import azmq

from azmq.multiplexer import Multiplexer


use_all_transports = pytest.mark.parametrize('endpoint', [
    'inproc://mypath',
    'tcp://127.0.0.1:3333',
])
"""
A decorator that causes the test scenario to run once with each of the support
transports.
"""


def onesec(awaitable):
    """
    Causes a normally blocking call to timeout after a while.

    :param awaitable: The awaitable to wrap.
    :returns: A decorated awaitable that times out.
    """
    return asyncio.wait_for(awaitable, 1)


@use_all_transports
@pytest.mark.asyncio
async def test_req_rep(event_loop, endpoint):
    async with azmq.Context() as context:
        req_socket = context.socket(azmq.REQ)
        rep_socket = context.socket(azmq.REP)

        try:
            req_socket.bind(endpoint)
            rep_socket.connect(endpoint)

            await req_socket.send_multipart([b'my', b'request'])
            message = await rep_socket.recv_multipart()
            assert message == [b'my', b'request']
            await rep_socket.send_multipart([b'my', b'response'])
            message = await req_socket.recv_multipart()
            assert message == [b'my', b'response']

        finally:
            req_socket.close()
            rep_socket.close()


@use_all_transports
@pytest.mark.asyncio
async def test_push_pull(event_loop, endpoint):
    async with azmq.Context() as context:
        push_socket = context.socket(azmq.PUSH)
        pull_socket_1 = context.socket(azmq.PULL)
        pull_socket_2 = context.socket(azmq.PULL)

        try:
            push_socket.bind(endpoint)
            pull_socket_1.connect(endpoint)
            pull_socket_2.connect(endpoint)

            multiplexer = Multiplexer()
            multiplexer.add_socket(pull_socket_1)
            multiplexer.add_socket(pull_socket_2)

            await onesec(push_socket.send_multipart([b'a', b'1']))
            await onesec(push_socket.send_multipart([b'b', b'2']))

            messages = []

            while len(messages) < 2:
                results = await onesec(multiplexer.recv_multipart())
                messages.extend(x for _, x in results)

            assert messages == [[b'a', b'1'], [b'b', b'2']]

        finally:
            push_socket.close()
            pull_socket_1.close()
            pull_socket_2.close()


@use_all_transports
@pytest.mark.asyncio
async def test_push_pull_slow_bind(event_loop, endpoint):
    async with azmq.Context() as context:
        push_socket = context.socket(azmq.PUSH)
        pull_socket = context.socket(azmq.PULL)

        try:
            # The PUSH socket connects before the PULL sockets binds and sends
            # a message right away.
            push_socket.connect(endpoint)
            await onesec(push_socket.send_multipart([b'hello']))

            # The PULL sockets finally binds, and should receive the message,
            # even late.
            pull_socket.bind(endpoint)
            message = await onesec(pull_socket.recv_multipart())
            assert message == [b'hello']

        finally:
            push_socket.close()
            pull_socket.close()


@use_all_transports
@pytest.mark.asyncio
async def test_push_pull_slow_connect(event_loop, endpoint):
    async with azmq.Context() as context:
        push_socket = context.socket(azmq.PUSH)
        pull_socket = context.socket(azmq.PULL)

        try:
            # The PUSH socket binds before the PULL sockets binds and sends
            # a message right away. It should block in this case.
            push_socket.bind(endpoint)

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(
                    push_socket.send_multipart([b'hello']),
                    0,
                )

            # The PULL sockets finally binds, and should receive no message.
            pull_socket.connect(endpoint)

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(pull_socket.recv_multipart(), 0)

        finally:
            push_socket.close()
            pull_socket.close()