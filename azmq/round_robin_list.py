"""
A list-like container that allows fair access to its elements.
"""

import asyncio

from itertools import (
    chain,
    islice,
)

from .common import AsyncObject


class RoundRobinList(AsyncObject):
    def __init__(self, values=None, **kwargs):
        super().__init__(**kwargs)
        self._values = list(values or [])
        self._current = 0
        self._not_empty = asyncio.Event(loop=self.loop)

    def __bool__(self):
        return bool(self._values)

    def __iter__(self):
        return islice(
            chain(iter(self._values), iter(self._values)),
            self._current,
            self._current + len(self._values),
        )

    async def wait_not_empty(self):
        await self._not_empty.wait()

    async def get_next(self):
        await self.wait_not_empty()
        return self._values[self.next()]

    def next(self):
        if self._current < len(self._values) - 1:
            self._current += 1
        else:
            self._current = 0

        return self._current

    def append(self, value):
        self._values.append(value)
        self._not_empty.set()

    def remove(self, value):
        index = self._values.index(value)

        # If we remove an element located prior to the current one, we move the
        # current index back one position to avoid skipping one element.
        if index < self._current:
            self._current -= 1

        self._values.pop(index)

        if not self._values:
            self._not_empty.clear()
