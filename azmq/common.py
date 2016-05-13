"""
Common utility classes and functions.
"""

import asyncio

from functools import wraps
from pyslot import Signal

from .log import logger


class AsyncObject(object):
    """
    Base class for objects that keep a reference to an event loop.
    """
    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()


def cancel_on_closing(coro):
    """
    Automatically cancels a coroutine when the defining instance gets closed.

    :param coro: The coroutine to cancel on closing.
    :returns: A decorated coroutine.
    """
    @wraps(coro)
    async def wrapper(self, *args, **kwargs):
        return await self.await_until_closing(coro(self, *args, **kwargs))

    return wrapper


class ClosableAsyncObject(AsyncObject):
    """
    Base class for objects that can be asynchronously closed and awaited.
    """
    def __init__(self, *args, loop=None, **kwargs):
        super().__init__(loop=loop)
        self._closed_future = asyncio.Future(loop=self.loop)
        self._closing = asyncio.Event(loop=self.loop)
        self.on_closed = Signal()
        logger.debug("%s opened.", self.__class__.__name__)
        self.on_open(*args, **kwargs)

    def __del__(self):
        """
        Closes asynchronously the object upon deletion.
        """
        self.close()

    @property
    def closing(self):
        return self._closing.is_set()

    async def wait_closing(self):
        """
        Wait for the instance to be closing.
        """
        await self._closing.wait()

    @property
    def closed(self):
        return self._closed_future.done()

    async def wait_closed(self):
        """
        Wait for the instance to be closed.
        """
        await self._closed_future
        return self._closed_future.result()

    async def await_until_closing(self, coro):
        """
        Wait for some task to complete but aborts as soon asthe instance is
        being closed.

        :param coro: The coroutine or future-like object to wait for.
        """
        wait_task = asyncio.ensure_future(self.wait_closing(), loop=self.loop)
        coro_task = asyncio.ensure_future(coro, loop=self.loop)
        done, pending = await asyncio.wait(
            [wait_task, coro_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

        # It could be that the previous instructions cancelled coro_task if it
        # wasn't done yet.
        return await coro_task

    def on_open(self):
        """
        Called whenever the instance is opened.

        Should be redefined by child-classes.
        """

    async def on_close(self, result):
        """
        Called whenever a close of the instance was requested.

        :param result: A value to use as the closing context.
        :returns: An awaitable that must only complete when the instance is
            effectively closed.

        Should be redefined by child-classes.
        """

    def __enter__(self):
        """
        Enter the context manager.

        :returns: The instance.
        """
        return self

    def __exit__(self, exc_type, exc, tb):
        """
        Exit the context manager.

        Closes the instance.
        """
        self.close()

    async def __aenter__(self):
        """
        Enter the context manager.

        :returns: The instance.
        """
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """
        Exit the context manager.

        Closes the instance and waits for it to be closed.
        """
        self.close()
        await self.wait_closed()

    def _set_closed(self, future):
        """
        Indicate that the instance is effectively closed.

        :param future: The close future.
        """
        logger.debug("%s closed.", self.__class__.__name__)
        self.on_closed.emit(self)
        self._closed_future.set_result(future.result())

    def close(self, result=True):
        """
        Close the instance.
        """
        if not self.closed and not self.closing:
            logger.debug("%s closing...", self.__class__.__name__)
            self._closing.set()
            future = asyncio.ensure_future(self.on_close(result))
            future.add_done_callback(self._set_closed)


class CompositeClosableAsyncObject(ClosableAsyncObject):
    """
    Base class for objects that can be asynchronously closed and awaited and
    have ownership of other closable objects.
    """
    @property
    def children(self):
        return self._children

    def on_open(self):
        self._children = set()

    async def on_close(self, result):
        await asyncio.gather(
            *[
                self.on_close_child(child, result)
                for child in self._children
            ]
        )
        del self._children
        return result

    async def on_close_child(self, child, result):
        """
        Called whenever a child instance must be closed.

        :returns: An awaitable that must only complete when the specified child
            instance is effectively closed.

        May be redefined by child-classes. The default implementation calls
            `close` followed by `wait_closed` on the specified child instance.
        """
        child.close(result)
        await child.wait_closed()

    def register_child(self, child):
        """
        Register a new child that will be closed whenever the current instance
        closes.

        :param child: The child instance.
        """
        if self.closing:
            child.close()
        else:
            self._children.add(child)
            child.on_closed.connect(self.unregister_child)

    def unregister_child(self, child):
        """
        Unregister an existing child that is no longer to be owned by the
        current instance.

        :param child: The child instance.
        """
        self._children.remove(child)
        child.on_closed.connect(self.unregister_child)


class AsyncTaskObject(ClosableAsyncObject):
    """
    Base class for objects whose lifetime is bound to a single running task.
    """
    def on_open(self):
        self.run_task = asyncio.ensure_future(
            self.run(),
            loop=self.loop,
        )

    async def on_close(self, result):
        self.run_task.cancel()
        await self.run_task
        return result

    async def run(self):
        try:
            await self.on_run()
        finally:
            self.close()

    async def on_run(self):
        """
        The task's main code. Upon completion of this task, the instance will
        be automatically closed.

        Should be redefined by child-classes.
        """

class AsyncTimeout(AsyncTaskObject):
    """
    A resetable asynchronous timeout.
    """
    def on_open(self, coro, timeout):
        """
        Initialize a new timeout.

        :param coro: The coroutine to execute when the timeout reaches the end
            of its life.
        :param timeout: The maximum time to wait for, in seconds.
        """
        super().on_open()
        self.coro = coro
        self.timeout = timeout
        self.revive_event = asyncio.Event(loop=self.loop)

    async def on_run(self):
        try:
            while True:
                await asyncio.wait_for(
                    self.revive_event.wait(),
                    timeout=self.timeout,
                    loop=self.loop,
                )
                self.revive_event.clear()
        except asyncio.TimeoutError:
            await self.coro()

    def revive(self):
        self.revive_event.set()


class AsyncPeriodicTimer(AsyncTaskObject):
    """
    An asynchronous periodic timer.
    """
    def on_open(self, coro, period):
        """
        Initialize a new timer.

        :param coro: The coroutine to execute on each tick.
        :param period: The interval of time between two ticks.
        """
        super().on_open()
        self.coro = coro
        self.period = period
        self.reset_event = asyncio.Event(loop=self.loop)

    async def on_run(self):
        while True:
            try:
                await asyncio.wait_for(
                    self.reset_event.wait(),
                    timeout=self.period,
                    loop=self.loop,
                )
            except asyncio.TimeoutError:
                await self.coro()
            else:
                self.reset_event.clear()

    def reset(self):
        """
        Reset the internal timer, effectively causing the next tick to happen
        in `self.period` seconds.
        """
        self.reset_event.set()
