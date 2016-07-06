"""
Inproc integration tests.
"""

import asyncio
import pytest

import azmq

from azmq.multiplexer import Multiplexer
from azmq.errors import ProtocolError


from ..conftest import use_all_transports


async def zerosec(awaitable):
    """
    Assert that an awaitable would block.

    :param awaitable: The awaitable to wrap.
    :returns: A decorated awaitable.
    """
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(awaitable, 0)


def fivesec(awaitable):
    """
    Causes a normally blocking call to timeout after a while.

    :param awaitable: The awaitable to wrap.
    :returns: A decorated awaitable that times out.
    """
    return asyncio.wait_for(awaitable, 5)


@use_all_transports
@pytest.mark.asyncio
async def test_req_rep(event_loop, endpoint):
    async with azmq.Context() as context:
        req_socket = context.socket(azmq.REQ)
        rep_socket = context.socket(azmq.REP)

        try:
            req_socket.bind(endpoint)
            rep_socket.connect(endpoint)

            await fivesec(req_socket.send_multipart([b'my', b'request']))
            message = await fivesec(rep_socket.recv_multipart())
            assert message == [b'my', b'request']
            await fivesec(rep_socket.send_multipart([b'my', b'response']))
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

            await fivesec(push_socket.send_multipart([b'a', b'1']))
            await fivesec(push_socket.send_multipart([b'b', b'2']))

            messages = []

            while len(messages) < 2:
                results = await fivesec(multiplexer.recv_multipart())
                messages.extend(tuple(x) for _, x in results)

            assert set(messages) == {(b'a', b'1'), (b'b', b'2')}

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
            await fivesec(push_socket.send_multipart([b'hello']))

            # The PULL sockets finally binds, and should receive the message,
            # even late.
            pull_socket.bind(endpoint)
            message = await fivesec(pull_socket.recv_multipart())
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
            await zerosec(push_socket.send_multipart([b'hello']))

            # The PULL sockets finally binds, and should receive no message.
            pull_socket.connect(endpoint)
            await zerosec(pull_socket.recv_multipart())

        finally:
            push_socket.close()
            pull_socket.close()


@use_all_transports
@pytest.mark.asyncio
async def test_push_pull_explicit_reconnect(event_loop, endpoint):
    async with azmq.Context() as context:
        push_socket = context.socket(azmq.PUSH)
        pull_socket = context.socket(azmq.PULL)

        try:
            push_socket.connect(endpoint)
            await fivesec(push_socket.send_multipart([b'hello']))

            # The disconnection should cause the outgoing message to be lost.
            await fivesec(push_socket.disconnect(endpoint))
            push_socket.connect(endpoint)

            pull_socket.bind(endpoint)
            await zerosec(pull_socket.recv_multipart())

            await fivesec(push_socket.send_multipart([b'hello']))
            message = await fivesec(pull_socket.recv_multipart())
            assert message == [b'hello']

        finally:
            push_socket.close()
            pull_socket.close()


@use_all_transports
@pytest.mark.asyncio
async def test_pub_sub_implicit_reconnect(event_loop, endpoint):
    async with azmq.Context() as context:
        pub_socket = context.socket(azmq.PUB)
        sub_socket = context.socket(azmq.SUB)
        await sub_socket.subscribe(b'h')

        try:
            pub_socket.bind(endpoint)
            sub_socket.connect(endpoint)

            # Let's start a task that sends messages on the pub socket.
            async def publish():
                while True:
                    await fivesec(pub_socket.send_multipart([b'hello']))

            publish_task = asyncio.ensure_future(publish())

            try:
                message = await fivesec(sub_socket.recv_multipart())
                assert message == [b'hello']

                await pub_socket.unbind(endpoint)
                pub_socket.bind(endpoint)

                message = await fivesec(sub_socket.recv_multipart())
                assert message == [b'hello']
            finally:
                publish_task.cancel()

        finally:
            pub_socket.close()
            sub_socket.close()


@use_all_transports
@pytest.mark.asyncio
async def test_pub_sub_spam(event_loop, endpoint):
    async with azmq.Context() as context:
        pub_socket = context.socket(azmq.PUB)
        sub_sockets = [
            context.socket(azmq.SUB)
            for _ in range(10)
        ]

        try:
            async def send():
                pub_socket.bind(endpoint)

                while True:
                    await fivesec(pub_socket.send_multipart([b'a', b'b']))

            async def recv(socket):
                await socket.subscribe(b'a')
                socket.connect(endpoint)
                message = await fivesec(socket.recv_multipart())
                assert message == [b'a', b'b']

            send_task = asyncio.ensure_future(send())
            recv_tasks = [
                asyncio.ensure_future(recv(socket))
                for socket in sub_sockets
            ]

            try:
                await fivesec(asyncio.gather(*recv_tasks))
            finally:
                send_task.cancel()

        finally:
            pub_socket.close()

            for socket in sub_sockets:
                socket.close()


@use_all_transports
@pytest.mark.asyncio
async def test_pub_sub_spam_subscribe_after(event_loop, endpoint):
    async with azmq.Context() as context:
        pub_socket = context.socket(azmq.PUB)
        sub_sockets = [
            context.socket(azmq.SUB)
            for _ in range(10)
        ]

        try:
            async def send():
                pub_socket.bind(endpoint)

                while True:
                    await fivesec(pub_socket.send_multipart([b'a', b'b']))

            async def recv(socket):
                socket.connect(endpoint)
                await socket.subscribe(b'a')
                message = await fivesec(socket.recv_multipart())
                assert message == [b'a', b'b']

            send_task = asyncio.ensure_future(send())
            recv_tasks = [
                asyncio.ensure_future(recv(socket))
                for socket in sub_sockets
            ]

            try:
                await fivesec(asyncio.gather(*recv_tasks))
            finally:
                send_task.cancel()

        finally:
            pub_socket.close()

            for socket in sub_sockets:
                socket.close()


@use_all_transports
@pytest.mark.asyncio
async def test_push_pull_max_outbox_size(event_loop, endpoint):
    async with azmq.Context() as context:
        push_socket = context.socket(azmq.PUSH)
        push_socket.max_outbox_size = 1
        pull_socket = context.socket(azmq.PULL)

        try:
            # The PUSH socket connects before the PULL sockets binds and sends
            # two messages right away. The first one should not block but the
            # second should as the queue size is 1 and the socket can't
            # possibly have sent the element so far.
            push_socket.connect(endpoint)
            await fivesec(push_socket.send_multipart([b'one']))
            await zerosec(push_socket.send_multipart([b'two']))

            # The PULL sockets finally binds, and should receive the first
            # message, even late.
            pull_socket.bind(endpoint)
            message = await fivesec(pull_socket.recv_multipart())
            assert message == [b'one']
            await zerosec(pull_socket.recv_multipart())

        finally:
            push_socket.close()
            pull_socket.close()


@use_all_transports
@pytest.mark.asyncio
async def test_router_identity(event_loop, endpoint):
    async with azmq.Context() as context:
        dealer_socket = context.socket(azmq.DEALER)
        router_socket = context.socket(azmq.ROUTER)

        try:
            dealer_socket.identity = b'hello'
            dealer_socket.bind(endpoint)
            router_socket.connect(endpoint)

            await fivesec(dealer_socket.send_multipart(
                [b'', b'my', b'request'],
            ))
            message = await fivesec(router_socket.recv_multipart())
            assert message == [b'hello', b'', b'my', b'request']
            await fivesec(router_socket.send_multipart(
                [b'hello', b'', b'my', b'response'],
            ))
            message = await dealer_socket.recv_multipart()
            assert message == [b'', b'my', b'response']

        finally:
            dealer_socket.close()
            router_socket.close()


@use_all_transports
@pytest.mark.asyncio
async def test_req_rep_invalid_identity(event_loop, endpoint):
    async with azmq.Context() as context:
        req_socket = context.socket(azmq.REQ)
        rep_socket = context.socket(azmq.REP)

        try:
            future = asyncio.Future(loop=event_loop)
            req_socket.connect(
                endpoint,
                on_connection_failure=future.set_result,
            )
            rep_socket.identity = b'\0invalid'
            rep_socket.bind(endpoint)

            await fivesec(future)
            assert isinstance(future.result().error, ProtocolError)
            assert future.result().error.fatal

        finally:
            req_socket.close()
            rep_socket.close()


@use_all_transports
@pytest.mark.asyncio
async def test_req_req_invalid_combination(event_loop, endpoint):
    async with azmq.Context() as context:
        req_socket = context.socket(azmq.REQ)
        req_socket_2 = context.socket(azmq.REQ)

        try:
            future = asyncio.Future(loop=event_loop)
            req_socket.connect(
                endpoint,
                on_connection_failure=future.set_result,
            )
            req_socket_2.bind(endpoint)

            await fivesec(future)
            assert isinstance(future.result().error, ProtocolError)
            assert future.result().error.fatal

        finally:
            req_socket.close()
            req_socket_2.close()


@use_all_transports
@pytest.mark.asyncio
async def test_req_rep_multiple_receivers(event_loop, endpoint):
    async with azmq.Context() as context:
        req_socket = context.socket(azmq.REQ)
        rep_socket = context.socket(azmq.REP)
        rep_socket_2 = context.socket(azmq.REP)

        try:
            req_socket.bind(endpoint)
            rep_socket.connect(endpoint)
            rep_socket_2.connect(endpoint)

            multiplexer = Multiplexer()
            multiplexer.add_socket(rep_socket)
            multiplexer.add_socket(rep_socket_2)

            await fivesec(req_socket.send_multipart([b'my', b'request']))
            (socket, message), = await fivesec(multiplexer.recv_multipart())
            assert message == [b'my', b'request']
            await fivesec(socket.send_multipart([b'my', b'response']))
            message = await req_socket.recv_multipart()
            assert message == [b'my', b'response']

        finally:
            req_socket.close()
            rep_socket.close()
            rep_socket_2.close()


@use_all_transports
@pytest.mark.asyncio
async def test_pub_sub_unsubscribe(event_loop, endpoint):
    async with azmq.Context() as context:
        pub_socket = context.socket(azmq.PUB)
        sub_socket = context.socket(azmq.SUB)
        await sub_socket.subscribe(b'h')
        await sub_socket.subscribe(b'g')

        try:
            pub_socket.bind(endpoint)
            sub_socket.connect(endpoint)

            # Let's start a task that sends messages on the pub socket.
            async def publish():
                while True:
                    await fivesec(pub_socket.send_multipart([b'hello']))

            publish_task = asyncio.ensure_future(publish())

            try:
                message = await fivesec(sub_socket.recv_multipart())
                assert message == [b'hello']
            finally:
                publish_task.cancel()

            await sub_socket.unsubscribe(b'h')

            # Let's start a task that sends messages on the pub socket.
            async def publish():
                while True:
                    await fivesec(pub_socket.send_multipart([b'hello']))
                    await fivesec(pub_socket.send_multipart([b'gello']))

            publish_task = asyncio.ensure_future(publish())

            try:
                while message == [b'hello']:
                    message = await fivesec(sub_socket.recv_multipart())

                assert message == [b'gello']
            finally:
                publish_task.cancel()

        finally:
            pub_socket.close()
            sub_socket.close()
