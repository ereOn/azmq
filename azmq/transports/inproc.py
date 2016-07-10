"""
Inproc transport classes.
"""

import asyncio

from ..common import (
    AsyncBox,
    ClosableAsyncObject,
    CompositeClosableAsyncObject,
    cancel_on_closing,
)


class Channel(ClosableAsyncObject):
    def on_open(self, path):
        super().on_open()
        self._path = path
        self._linked_channel = None
        self._inbox = AsyncBox(loop=self.loop)

    async def on_close(self):
        self._inbox.close()

        if self._linked_channel:
            self._linked_channel.close()
            self._linked_channel._linked_channel = None
            self._linked_channel = None

        await self._inbox.wait_closed()

    @property
    def path(self):
        return self._path

    def __repr__(self):
        return 'Channel(path=%r)' % self._path

    def link(self, channel):
        self._linked_channel = channel
        channel._linked_channel = self

    @cancel_on_closing
    async def read(self):
        return await self._inbox.read()

    @cancel_on_closing
    async def write(self, item):
        if self._linked_channel:
            await self._linked_channel._inbox.write(item)


class InprocServer(CompositeClosableAsyncObject):
    def on_open(self, handler):
        super().on_open()
        self._handler = handler
        self._tasks = []

    async def on_close(self):
        if self._tasks:
            await asyncio.wait(self._tasks[:])

        await super().on_close()

    @cancel_on_closing
    async def create_channel(self, path):
        left = Channel(path=path, loop=self.loop)
        self.register_child(left)
        right = Channel(path=path, loop=self.loop)
        self.register_child(right)
        left.link(right)

        task = asyncio.ensure_future(self._handler(right))
        self._tasks.append(task)

        @task.add_done_callback
        def remove_task(future):
            self._tasks.remove(task)

        return left
