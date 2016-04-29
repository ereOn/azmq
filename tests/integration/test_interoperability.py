"""
Tests interoperability with pyzmq (which is powered by the C-native libzmq
library).
"""

import asyncio
import concurrent
import pytest
import sys
import zmq


# This is temporary. We should test the higher-level implementation instead.
import azmq


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
    sock = ctx.socket(zmq.REP)
    sock.bind('tcp://127.0.0.1:3333')
    # This is temporary. We should test the higher-level implementation
    # instead.

    async with azmq.Context(loop=event_loop) as context:
        socket = context.socket(azmq.REQ)
        socket.connect('tcp://127.0.0.1:3333')
        await asyncio.sleep(1)

    assert 0
