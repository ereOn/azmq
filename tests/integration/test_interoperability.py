"""
Tests interoperability with pyzmq (which is powered by the C-native libzmq
library).
"""

import asyncio
import pytest
import sys
import zmq

from contextlib import contextmanager
from logging import getLogger
from threading import Thread

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


@pytest.yield_fixture
def pyzmq_context():
    context = zmq.Context()
    yield context
    context.destroy()


@pytest.yield_fixture
def req_socket(pyzmq_context):
    socket = pyzmq_context.socket(zmq.REQ)
    yield socket
    socket.close()


@pytest.yield_fixture
def rep_socket(pyzmq_context):
    socket = pyzmq_context.socket(zmq.REP)
    yield socket
    socket.close()


@pytest.fixture
def connect_or_bind(link):
    assert link in {'bind', 'connect'}, "Invalid link type: %s" % repr(link)

    def func(socket, target, reverse=False):
        if (link == 'bind') != reverse:
            socket.bind(target)
        elif (link == 'connect') != reverse:
            socket.connect(target)

    return func


@contextmanager
def run_in_background(target, *args, **kwargs):
    thread = Thread(target=target, args=args, kwargs=kwargs)
    thread.start()

    try:
        yield
    finally:
        thread.join(5)
        assert not thread.isAlive()


@pytest.mark.parametrize("link", [
    'bind',
    'connect',
])
@pytest.mark.asyncio
async def test_tcp_req_socket(event_loop, rep_socket, connect_or_bind):
    connect_or_bind(rep_socket, 'tcp://127.0.0.1:3333', reverse=True)

    def run():
        frames = rep_socket.recv_multipart()
        assert frames == [b'my', b'question']
        rep_socket.send_multipart([b'your', b'answer'])

    with run_in_background(run):
        async with azmq.Context(loop=event_loop) as context:
            socket = context.socket(azmq.REQ)
            connect_or_bind(socket, 'tcp://127.0.0.1:3333')
            await socket.send_multipart([b'my', b'question'])
            frames = await asyncio.wait_for(socket.recv_multipart(), 1)
            assert frames == [b'your', b'answer']


@pytest.mark.parametrize("link", [
    'bind',
    'connect',
])
@pytest.mark.asyncio
async def test_tcp_rep_socket(event_loop, req_socket, connect_or_bind):
    connect_or_bind(req_socket, 'tcp://127.0.0.1:3333', reverse=True)

    def run():
        req_socket.send_multipart([b'my', b'question'])
        frames = req_socket.recv_multipart()
        assert frames == [b'your', b'answer']

    with run_in_background(run):
        async with azmq.Context(loop=event_loop) as context:
            socket = context.socket(azmq.REP)
            connect_or_bind(socket, 'tcp://127.0.0.1:3333')
            frames = await asyncio.wait_for(socket.recv_multipart(), 1)
            assert frames == [b'my', b'question']
            await socket.send_multipart([b'your', b'answer'])
