"""
Unit tests for common classes.
"""

import asyncio
import pytest

from mock import MagicMock

from azmq.common import (
    AsyncBox,
    AsyncPeriodicTimer,
    AsyncTimeout,
    ClosableAsyncObject,
)


class MyClass(ClosableAsyncObject):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.can_close = asyncio.Event()

    async def on_close(self, result):
        await self.can_close.wait()


@pytest.mark.asyncio
async def test_closable_async_object_as_contextmanager(event_loop):
    my_class = MyClass(loop=event_loop)

    assert not my_class.closing
    assert not my_class.closed

    with my_class as x:
        assert x is my_class
        assert not my_class.closing
        assert not my_class.closed

    assert my_class.closing
    assert not my_class.closed

    my_class.can_close.set()
    await my_class.wait_closed()

    assert my_class.closing
    assert my_class.closed


@pytest.mark.asyncio
async def test_closable_async_object_as_async_contextmanager(event_loop):
    my_class = MyClass(loop=event_loop)

    assert not my_class.closing
    assert not my_class.closed

    async with my_class as x:
        my_class.can_close.set()
        assert x is my_class
        assert not my_class.closing
        assert not my_class.closed

    assert my_class.closing
    assert my_class.closed


@pytest.mark.asyncio
async def test_async_timeout_expiration(event_loop):
    event = asyncio.Event(loop=event_loop)
    async_timeout = AsyncTimeout(callback=event.set, timeout=0)
    await event.wait()


@pytest.mark.asyncio
async def test_async_timeout_expiration_after_revive(event_loop):
    event = asyncio.Event(loop=event_loop)

    async def set_event():
        event.set()

    async_timeout = AsyncTimeout(callback=set_event, timeout=5)
    async_timeout.revive(timeout=0)
    await event.wait()


@pytest.mark.asyncio
async def test_async_timeout_exception(event_loop):
    event = asyncio.Event(loop=event_loop)
    call = MagicMock()

    async def failure():
        call()
        event.set()
        raise RuntimeError

    async with AsyncTimeout(callback=failure, timeout=0):
        await event.wait()

    call.assert_called_once_with()


@pytest.mark.asyncio
async def test_async_periodic_timer(event_loop):
    event = asyncio.Event(loop=event_loop)

    async with AsyncPeriodicTimer(callback=event.set, period=0):
        await event.wait()


@pytest.mark.asyncio
async def test_async_periodic_timer_reset(event_loop):
    event = asyncio.Event(loop=event_loop)

    async def set_event():
        event.set()

    async with AsyncPeriodicTimer(callback=set_event, period=5) as timer:
        timer.reset(period=0)
        await event.wait()


@pytest.mark.asyncio
async def test_async_periodic_timer_exception(event_loop):
    event = asyncio.Event(loop=event_loop)
    call = MagicMock()

    async def failure():
        call()
        event.set()
        raise RuntimeError

    async with AsyncPeriodicTimer(callback=failure, period=0):
        await event.wait()

    call.assert_called_once_with()


@pytest.mark.asyncio
async def test_async_box(event_loop):
    box = AsyncBox(maxsize=2, loop=event_loop)

    assert box.empty()
    assert not box.full()

    await box.write(1)

    assert not box.empty()
    assert not box.full()
    await asyncio.wait_for(box.wait_not_empty(), 1)

    await box.write(2)

    assert not box.empty()
    assert box.full()

    assert await box.read() == 1

    assert not box.empty()
    assert not box.full()

    assert box.read_nowait() == 2

    assert box.empty()
    assert not box.full()

    await box.write(3)
    box.clear()

    assert box.empty()
    assert not box.full()


@pytest.mark.asyncio
async def test_async_box_clone(event_loop):
    async with AsyncBox(maxsize=2, loop=event_loop) as box:
        await box.write(1)

    box2 = box.clone()

    assert not box2.empty()
    assert not box2.full()

    assert box2.read_nowait() == 1
