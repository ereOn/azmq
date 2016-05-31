"""
Inproc integration tests.
"""

import asyncio
import pytest

import azmq


@pytest.mark.asyncio
async def test_req_rep(event_loop):
    async with azmq.Context() as context:
        req_socket = context.socket(azmq.REQ)
        rep_socket = context.socket(azmq.REP)

        try:
            req_socket.bind('inproc://mypath')
            rep_socket.connect('inproc://mypath')

            await req_socket.send_multipart([b'my', b'request'])
            message = await rep_socket.recv_multipart()
            assert message == [b'my', b'request']
            await rep_socket.send_multipart([b'my', b'response'])
            message = await req_socket.recv_multipart()
            assert message == [b'my', b'response']

        finally:
            req_socket.close()
            rep_socket.close()


@pytest.mark.asyncio
async def test_push_pull(event_loop):
    async with azmq.Context() as context:
        push_socket = context.socket(azmq.PUSH)
        pull_socket_1 = context.socket(azmq.PULL)
        pull_socket_2 = context.socket(azmq.PULL)

        try:
            push_socket.bind('inproc://mypath')
            pull_socket_1.connect('inproc://mypath')
            pull_socket_2.connect('inproc://mypath')

            await push_socket.send_multipart([b'a', b'1'])
            await push_socket.send_multipart([b'b', b'2'])

            messages = await asyncio.gather(
                pull_socket_1.recv_multipart(),
                pull_socket_2.recv_multipart(),
            )
            assert set(map(tuple, messages)) == {
                (b'a', b'1'),
                (b'b', b'2'),
            }

        finally:
            push_socket.close()
            pull_socket_1.close()
            pull_socket_2.close()
