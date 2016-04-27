"""
Common utility classes and functions.
"""

import asyncio

from .log import logger


class AsyncObject(object):
    """
    Base class for objects that keep a reference to an event loop.
    """
    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()


class ClosableAsyncObject(AsyncObject):
    """
    Base class for objects that can be asynchronously closed and awaited.
    """
    def __init__(self, *args, loop=None, **kwargs):
        super().__init__(loop=loop)
        self._closed_future = asyncio.Future(loop=self.loop)
        self.closing = False
        logger.debug("%s opened.", self.__class__.__name__)
        self.on_open(*args, **kwargs)

    @property
    def closed(self):
        return self._closed_future.done()

    async def wait_closed(self):
        """
        Wait for the instance to be closed.
        """
        await self._closed_future

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

    def on_closed(self):
        """
        Called whenever the instance was effectively closed.

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

    def _set_closed(self, *args, **kwargs):
        """
        Indicate that the instance is effectively closed.

        Can be used as a future callback as it takes and ignores and arguments
        passed to it.
        """
        logger.debug("%s closed.", self.__class__.__name__)
        self.on_closed()
        self._closed_future.set_result(None)

    def close(self):
        """
        Close the instance.
        """
        if not self.closed and not self.closing:
            logger.debug("%s closing...", self.__class__.__name__)
            self.closing = True
            future = asyncio.ensure_future(self.on_close())
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

    async def on_close(self):
        await asyncio.gather(
            *[
                self.on_close_child(child)
                for child in self._children
            ]
        )

    def on_closed(self):
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
        self._children.add(child)

    def unregister_child(self, child):
        """
        Unregister an existing child that is no longer to be owned by the
        current instance.

        :param child: The child instance.
        """
        self._children.remove(child)
