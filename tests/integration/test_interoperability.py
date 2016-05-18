"""
Tests interoperability with pyzmq (which is powered by the C-native libzmq
library).
"""

import asyncio
import concurrent
import pytest
import sys
import zmq
import threading

from logging import getLogger

import azmq

logger = getLogger()


@pytest.yield_fixture
def event_loop():
    if sys.platform == 'win32':
        loop = asyncio.ProactorEventLoop()
    else:
        loop = asyncio.SelectorEventLoop()

    asyncio.set_event_loop(loop)

    yield loop

    loop.run_until_complete(asyncio.wait(asyncio.Task.all_tasks()))


@pytest.mark.asyncio
async def test_tcp_client_rep_socket(event_loop):
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.bind('tcp://127.0.0.1:3333')

    # This is temporary. We should test the higher-level implementation
    # instead.

    async with azmq.Context(loop=event_loop) as context:
        socket = context.socket(azmq.REP)
        socket.connect('tcp://127.0.0.1:3333')
        thread = threading.Thread(
            target=sock.send_multipart,
            args=[[b'hello', b'world']],
        )
        thread.start()
        logger.info("Received: %r", await asyncio.wait_for(socket.recv_multipart(), 1))
        thread.join()
        logger.info("Wait is over.")

    assert 0
