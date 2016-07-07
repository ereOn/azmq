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
        logger.debug("%s[%s] opened.", self.__class__.__name__, id(self))
        self.on_open(*args, **kwargs)

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
        # Prevents the future from being cancelled in case the wait itself gets
        # cancelled.
        await asyncio.shield(self._closed_future)
        return self._closed_future.result()

    async def await_until_closing(self, coro):
        """
        Wait for some task to complete but aborts as soon asthe instance is
        being closed.

        :param coro: The coroutine or future-like object to wait for.
        """
        wait_task = asyncio.ensure_future(self.wait_closing(), loop=self.loop)
        coro_task = asyncio.ensure_future(coro, loop=self.loop)

        try:
            done, pending = await asyncio.wait(
                [wait_task, coro_task],
                return_when=asyncio.FIRST_COMPLETED,
                loop=self.loop,
            )

        finally:
            wait_task.cancel()
            coro_task.cancel()

        # It could be that the previous instructions cancelled coro_task if it
        # wasn't done yet.
        return await coro_task

    def on_open(self):
        """
        Called whenever the instance is opened.

        Should be redefined by child-classes.
        """

    async def on_close(self):
        """
        Called whenever a close of the instance was requested.

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
        logger.debug("%s[%s] closed.", self.__class__.__name__, id(self))
        self.on_closed.emit(self)
        self._closed_future.set_result(future.result())

    def close(self):
        """
        Close the instance.
        """
        if not self.closed and not self.closing:
            logger.debug(
                "%s[%s] closing...",
                self.__class__.__name__,
                id(self),
            )
            self._closing.set()
            future = asyncio.ensure_future(self.on_close(), loop=self.loop)
            future.add_done_callback(self._set_closed)


class CompositeClosableAsyncObject(ClosableAsyncObject):
    """
    Base class for objects that can be asynchronously closed and awaited and
    have ownership of other closable objects.
    """
    def on_open(self):
        self._children = set()

    async def on_close(self):
        await asyncio.gather(
            *[
                self.on_close_child(child)
                for child in self._children
            ],
            loop=self.loop,
        )
        del self._children

    async def on_close_child(self, child):
        """
        Called whenever a child instance must be closed.

        :returns: An awaitable that must only complete when the specified child
            instance is effectively closed.

        May be redefined by child-classes. The default implementation calls
            `close` followed by `wait_closed` on the specified child instance.
        """
        child.close()
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
        child.on_closed.disconnect(self.unregister_child)


class AsyncTaskObject(ClosableAsyncObject):
    """
    Base class for objects whose lifetime is bound to a single running task.
    """
    def on_open(self):
        self.run_task = asyncio.ensure_future(
            self.run(),
            loop=self.loop,
        )

    async def on_close(self):
        self.run_task.cancel()

        await asyncio.wait([self.run_task], loop=self.loop)

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
    def on_open(self, callback, timeout):
        """
        Initialize a new timeout.

        :param callback: The  callback to execute when the timeout reaches the
            end of its life. May be a coroutine function.
        :param timeout: The maximum time to wait for, in seconds.
        """
        super().on_open()
        self.callback = callback
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
            try:
                if asyncio.iscoroutinefunction(self.callback):
                    await self.callback()
                else:
                    self.callback()
            except Exception:
                logger.exception("Error in timeout callback execution.")

    def revive(self, timeout=None):
        """
        Revive the timeout.

        :param timeout: If not `None`, specifies a new timeout value to use.
        """
        if timeout is not None:
            self.timeout = timeout

        self.revive_event.set()


class AsyncPeriodicTimer(AsyncTaskObject):
    """
    An asynchronous periodic timer.
    """
    def on_open(self, callback, period):
        """
        Initialize a new timer.

        :param callback: The function or coroutine function to call on each
            tick.
        :param period: The interval of time between two ticks.
        """
        super().on_open()
        self.callback = callback
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
                try:
                    if asyncio.iscoroutinefunction(self.callback):
                        await self.callback()
                    else:
                        self.callback()
                except Exception:
                    logger.exception("Error in timer callback execution.")
            else:
                self.reset_event.clear()

    def reset(self, period=None):
        """
        Reset the internal timer, effectively causing the next tick to happen
        in `self.period` seconds.

        :param period: If not `None`, specifies a new period to use.
        """
        if period is not None:
            self.period = period

        self.reset_event.set()


class AsyncBox(ClosableAsyncObject):
    def on_open(self, maxsize=0):
        self._maxsize = maxsize
        self._queue = asyncio.Queue(maxsize=maxsize, loop=self.loop)
        self._can_read = asyncio.Event(loop=self.loop)
        self._can_write = asyncio.Event(loop=self.loop)
        self._can_write.set()

    @cancel_on_closing
    async def read(self):
        """
        Read from the box in a blocking manner.

        :returns: An item from the box.
        """
        result = await self._queue.get()

        self._can_write.set()

        if self._queue.empty():
            self._can_read.clear()

        return result

    def read_nowait(self):
        """
        Read from the box in a non-blocking manner.

        If the box is empty, an exception is thrown. You should always check
        for emptiness with `empty` or `wait_not_empty` before calling this
        method.

        :returns: An item from the box.
        """
        result = self._queue.get_nowait()

        self._can_write.set()

        if self._queue.empty():
            self._can_read.clear()

        return result

    @cancel_on_closing
    async def wait_not_empty(self):
        """
        Blocks until the queue is not empty.
        """
        await self._can_read.wait()

    @cancel_on_closing
    async def wait_not_full(self):
        """
        Blocks until the queue is not full.
        """
        await self._can_write.wait()

    def full(self):
        return self._queue.full()

    def empty(self):
        return self._queue.empty()

    @cancel_on_closing
    async def write(self, item):
        """
        Write an item in the queue.

        :param item: The item.
        """
        await self._queue.put(item)
        self._can_read.set()

        if self._queue.full():
            self._can_write.clear()

    def write_nowait(self, item):
        """
        Write in the box in a non-blocking manner.

        If the box is full, an exception is thrown. You should always check
        for fullness with `full` or `wait_not_full` before calling this method.

        :param item: An item.
        """
        self._queue.put_nowait(item)
        self._can_read.set()

        if self._queue.full():
            self._can_write.clear()

    def clear(self):
        """
        Clear the box.
        """
        while not self._queue.empty():
            self._queue.get_nowait()

        self._can_read.clear()
        self._can_write.set()

    def clone(self):
        """
        Clone the box.

        :returns: A new box with the same item queue.

        The cloned box is not closed, no matter the initial state of the
        original instance.
        """
        result = AsyncBox(maxsize=self._maxsize, loop=self.loop)
        result._queue = self._queue
        result._can_read = self._can_read
        result._can_write = self._can_write

        return result
