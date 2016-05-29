"""
Tests interoperability with pyzmq (which is powered by the C-native libzmq
library).
"""

import asyncio
import pytest
import zmq

from contextlib import (
    ExitStack,
    contextmanager,
)
from concurrent.futures import Future
from logging import getLogger
from threading import (
    Event,
    Thread,
)

import azmq

logger = getLogger()


@pytest.yield_fixture
def pyzmq_context():
    context = zmq.Context()
    yield context
    context.term()


@pytest.yield_fixture
def socket_factory(pyzmq_context):
    with ExitStack() as stack:
        class SocketFactory(object):
            def create(self, type_):
                socket = pyzmq_context.socket(type_)
                stack.callback(socket.close)
                return socket

        yield SocketFactory()


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
    event = Event()
    future = Future()

    def extended_target(*args, **kwargs):
        try:
            future.set_result(target(*args, **kwargs))
        except Exception as ex:
            future.set_exception(ex)
        finally:
            event.set()

    thread = Thread(target=extended_target, args=args, kwargs=kwargs)
    thread.start()
    exception = None

    try:
        yield event
    except Exception as ex:
        exception = ex
    finally:
        if not exception:
            future.result(timeout=5)
        else:
            raise

        thread.join(timeout=5)
        assert not thread.isAlive()


@pytest.mark.parametrize("link", [
    'bind',
    'connect',
])
@pytest.mark.asyncio
async def test_tcp_req_socket(event_loop, socket_factory, connect_or_bind):
    rep_socket = socket_factory.create(zmq.REP)
    connect_or_bind(rep_socket, 'tcp://127.0.0.1:3333', reverse=True)

    def run():
        frames = rep_socket.recv_multipart()
        assert frames == [b'my', b'question']
        rep_socket.send_multipart([b'your', b'answer'])

    with run_in_background(run):
        async with azmq.Context(loop=event_loop) as context:
            socket = context.socket(azmq.REQ)
            connect_or_bind(socket, 'tcp://127.0.0.1:3333')
            await asyncio.wait_for(
                socket.send_multipart([b'my', b'question']),
                1,
            )
            frames = await asyncio.wait_for(socket.recv_multipart(), 1)
            assert frames == [b'your', b'answer']


@pytest.mark.parametrize("link", [
    'bind',
    'connect',
])
@pytest.mark.asyncio
async def test_tcp_rep_socket(event_loop, socket_factory, connect_or_bind):
    req_socket = socket_factory.create(zmq.REQ)
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
            await asyncio.wait_for(
                socket.send_multipart([b'your', b'answer']),
                1,
            )


@pytest.mark.parametrize("link", [
    'bind',
    'connect',
])
@pytest.mark.asyncio
async def test_tcp_dealer_socket(event_loop, socket_factory, connect_or_bind):
    rep_socket = socket_factory.create(zmq.REP)
    connect_or_bind(rep_socket, 'tcp://127.0.0.1:3333', reverse=True)

    def run():
        frames = rep_socket.recv_multipart()
        assert frames == [b'my', b'question']
        rep_socket.send_multipart([b'your', b'answer'])

    with run_in_background(run):
        async with azmq.Context(loop=event_loop) as context:
            socket = context.socket(azmq.DEALER)
            connect_or_bind(socket, 'tcp://127.0.0.1:3333')
            await asyncio.wait_for(
                socket.send_multipart([b'', b'my', b'question']),
                1,
            )
            frames = await asyncio.wait_for(socket.recv_multipart(), 1)
            assert frames == [b'', b'your', b'answer']


@pytest.mark.parametrize("link", [
    'bind',
    'connect',
])
@pytest.mark.asyncio
async def test_tcp_router_socket(event_loop, socket_factory, connect_or_bind):
    req_socket = socket_factory.create(zmq.REQ)
    req_socket.identity = b'abcd'
    connect_or_bind(req_socket, 'tcp://127.0.0.1:3333', reverse=True)

    def run():
        req_socket.send_multipart([b'my', b'question'])
        frames = req_socket.recv_multipart()
        assert frames == [b'your', b'answer']

    with run_in_background(run):
        async with azmq.Context(loop=event_loop) as context:
            socket = context.socket(azmq.ROUTER)
            connect_or_bind(socket, 'tcp://127.0.0.1:3333')
            frames = await asyncio.wait_for(socket.recv_multipart(), 1)
            identity = frames.pop(0)
            assert identity == req_socket.identity
            assert frames == [b'', b'my', b'question']
            await asyncio.wait_for(
                socket.send_multipart([identity, b'', b'your', b'answer']),
                1,
            )


@pytest.mark.parametrize("link", [
    'bind',
    'connect',
])
@pytest.mark.asyncio
async def test_tcp_pub_socket(event_loop, socket_factory, connect_or_bind):
    sub_socket = socket_factory.create(zmq.SUB)
    sub_socket.setsockopt(zmq.SUBSCRIBE, b'a')
    connect_or_bind(sub_socket, 'tcp://127.0.0.1:3333', reverse=True)

    def run():
        frames = sub_socket.recv_multipart()
        assert frames == [b'a', b'message']

    with run_in_background(run) as thread_done_event:
        async with azmq.Context(loop=event_loop) as context:
            socket = context.socket(azmq.PUB)
            connect_or_bind(socket, 'tcp://127.0.0.1:3333')

            while not thread_done_event.is_set():
                await socket.send_multipart([b'a', b'message'])
                await socket.send_multipart([b'b', b'wrong'])


@pytest.mark.parametrize("link", [
    'bind',
    'connect',
])
@pytest.mark.asyncio
async def test_tcp_xpub_socket(event_loop, socket_factory, connect_or_bind):
    sub_socket = socket_factory.create(zmq.SUB)
    sub_socket.setsockopt(zmq.SUBSCRIBE, b'a')
    connect_or_bind(sub_socket, 'tcp://127.0.0.1:3333', reverse=True)

    def run():
        frames = sub_socket.recv_multipart()
        assert frames == [b'a', b'message']

    with run_in_background(run) as thread_done_event:
        async with azmq.Context(loop=event_loop) as context:
            socket = context.socket(azmq.XPUB)
            connect_or_bind(socket, 'tcp://127.0.0.1:3333')

            frames = await asyncio.wait_for(socket.recv_multipart(), 1)
            assert frames == [b'\1a']

            while not thread_done_event.is_set():
                await socket.send_multipart([b'a', b'message'])
                await socket.send_multipart([b'b', b'wrong'])

            sub_socket.close()
            frames = await asyncio.wait_for(socket.recv_multipart(), 1)
            assert frames == [b'\0a']


@pytest.mark.parametrize("link", [
    'bind',
    'connect',
])
@pytest.mark.asyncio
async def test_tcp_sub_socket(event_loop, socket_factory, connect_or_bind):
    xpub_socket = socket_factory.create(zmq.XPUB)
    connect_or_bind(xpub_socket, 'tcp://127.0.0.1:3333', reverse=True)

    def run():
        # Wait one second for the subscription to arrive.
        assert xpub_socket.poll(1000) == zmq.POLLIN
        topic = xpub_socket.recv_multipart()
        assert topic == [b'\x01a']
        xpub_socket.send_multipart([b'a', b'message'])

        if connect_or_bind == 'connect':
            assert xpub_socket.poll(1000) == zmq.POLLIN
            topic = xpub_socket.recv_multipart()
            assert topic == [b'\x00a']

    with run_in_background(run):
        async with azmq.Context(loop=event_loop) as context:
            socket = context.socket(azmq.SUB)
            await socket.subscribe(b'a')
            connect_or_bind(socket, 'tcp://127.0.0.1:3333')

            frames = await asyncio.wait_for(socket.recv_multipart(), 1)
            assert frames == [b'a', b'message']

        logger.info("Context closed for good")


@pytest.mark.parametrize("link", [
    'bind',
    'connect',
])
@pytest.mark.asyncio
async def test_tcp_xsub_socket(event_loop, socket_factory, connect_or_bind):
    xpub_socket = socket_factory.create(zmq.XPUB)
    connect_or_bind(xpub_socket, 'tcp://127.0.0.1:3333', reverse=True)

    def run():
        # Wait one second for the subscription to arrive.
        assert xpub_socket.poll(1000) == zmq.POLLIN
        topic = xpub_socket.recv_multipart()
        assert topic == [b'\x01a']
        xpub_socket.send_multipart([b'a', b'message'])

        if connect_or_bind == 'connect':
            assert xpub_socket.poll(1000) == zmq.POLLIN
            topic = xpub_socket.recv_multipart()
            assert topic == [b'\x00a']

    with run_in_background(run):
        async with azmq.Context(loop=event_loop) as context:
            socket = context.socket(azmq.XSUB)
            await socket.send_multipart([b'\x01a'])
            connect_or_bind(socket, 'tcp://127.0.0.1:3333')

            frames = await asyncio.wait_for(socket.recv_multipart(), 1)
            assert frames == [b'a', b'message']


@pytest.mark.parametrize("link", [
    'bind',
    'connect',
])
@pytest.mark.asyncio
async def test_tcp_push_socket(event_loop, socket_factory, connect_or_bind):
    pull_socket = socket_factory.create(zmq.PULL)
    connect_or_bind(pull_socket, 'tcp://127.0.0.1:3333', reverse=True)

    def run():
        assert pull_socket.poll(1000) == zmq.POLLIN
        message = pull_socket.recv_multipart()
        assert message == [b'hello', b'world']

    with run_in_background(run):
        async with azmq.Context(loop=event_loop) as context:
            socket = context.socket(azmq.PUSH)
            connect_or_bind(socket, 'tcp://127.0.0.1:3333')
            await socket.send_multipart([b'hello', b'world'])


@pytest.mark.parametrize("link", [
    'bind',
    'connect',
])
@pytest.mark.asyncio
async def test_tcp_pull_socket(event_loop, socket_factory, connect_or_bind):
    pull_socket = socket_factory.create(zmq.PUSH)
    connect_or_bind(pull_socket, 'tcp://127.0.0.1:3333', reverse=True)

    def run():
        pull_socket.send_multipart([b'hello', b'world'])

    with run_in_background(run):
        async with azmq.Context(loop=event_loop) as context:
            socket = context.socket(azmq.PULL)
            connect_or_bind(socket, 'tcp://127.0.0.1:3333')
            message = await socket.recv_multipart()
            assert message == [b'hello', b'world']


@pytest.mark.parametrize("link", [
    'bind',
    'connect',
])
@pytest.mark.asyncio
async def test_tcp_big_messages(event_loop, socket_factory, connect_or_bind):
    rep_socket = socket_factory.create(zmq.REP)
    connect_or_bind(rep_socket, 'tcp://127.0.0.1:3333', reverse=True)

    def run():
        frames = rep_socket.recv_multipart()
        assert frames == [b'1' * 500, b'2' * 100000]
        rep_socket.send_multipart([b'3' * 500, b'4' * 100000])

    with run_in_background(run):
        async with azmq.Context(loop=event_loop) as context:
            socket = context.socket(azmq.REQ)
            connect_or_bind(socket, 'tcp://127.0.0.1:3333')
            await asyncio.wait_for(
                socket.send_multipart([b'1' * 500, b'2' * 100000]),
                1,
            )
            frames = await asyncio.wait_for(socket.recv_multipart(), 1)
            assert frames == [b'3' * 500, b'4' * 100000]
