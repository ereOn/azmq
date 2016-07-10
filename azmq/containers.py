"""
Container utility classes.
"""

import asyncio

from itertools import (
    chain,
    islice,
)

from .common import AsyncObject


class AsyncList(list, AsyncObject):
    """
    A list-like object that is asyncio-compatible.
    """
    def __init__(self, *items, **kwargs):
        """
        Instanciate a new `AsyncList` list with optional items.

        :param items: If provided, an iterable of items to fill the list with.
        :param loop: The event loop the instance is associated with. Default
            to the thread-local current asyncio event-loop.
        """
        list.__init__(self, *items)
        AsyncObject.__init__(self, **kwargs)
        self._empty = asyncio.Event(loop=self.loop)
        self._not_empty = asyncio.Event(loop=self.loop)
        self._refresh()

    def _refresh(self):
        if self:
            self._empty.clear()
            self._not_empty.set()
        else:
            self._empty.set()
            self._not_empty.clear()

    def append(self, *args, **kwargs):
        super().append(*args, **kwargs)
        self._refresh()

    def remove(self, *args, **kwargs):
        super().remove(*args, **kwargs)
        self._refresh()

    def extend(self, *args, **kwargs):
        super().extend(*args, **kwargs)
        self._refresh()

    def clear(self, *args, **kwargs):
        super().clear(*args, **kwargs)
        self._refresh()

    def __setitem__(self, *args, **kwargs):
        super().__setitem__(*args, **kwargs)
        self._refresh()

    def pop(self, *args, **kwargs):
        try:
            return super().pop(*args, **kwargs)
        finally:
            self._refresh()

    def insert(self, *args, **kwargs):
        super().insert(*args, **kwargs)
        self._refresh()

    async def wait_not_empty(self):
        """
        Wait for the list to become non-empty.
        """
        await self._not_empty.wait()

    async def wait_empty(self):
        """
        Wait for the list to become empty.
        """
        await self._empty.wait()

    def create_proxy(self):
        """
        Create a fair list proxy to the current instance.

        :returns: A `FairListProxy` instance.
        """
        return FairListProxy(self)


class FairListProxy(object):
    """
    A read-only iterable proxy class that provides fair access to the proxied
    list each time it is iterated over.
    """

    def __init__(self, list_):
        """
        Instanciate a new fair list-proxy.

        :param list_: The list to proxy.
        """
        super().__init__()
        self._list = list_
        self._index = 0

    def __bool__(self):
        return bool(self._list)

    def __len__(self):
        return len(self._list)

    def shift(self, count=1):
        """
        Shift the view a specified number of times.

        :param count: The count of times to shift the view.
        """
        if self:
            self._index = (self._index + count) % len(self)
        else:
            self._index = 0

    def __iter__(self):
        index = self._index
        self.shift()

        return chain(
            islice(self._list, index, len(self._list)),
            islice(self._list, 0, index),
        )

    async def wait_not_empty(self):
        """
        Wait for the list to become non-empty.
        """
        await self._list.wait_not_empty()

    async def wait_empty(self):
        """
        Wait for the list to become empty.
        """
        await self._list.wait_empty()
