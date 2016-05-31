"""
Inproc transport classes.
"""

import asyncio

from ..common import (
    AsyncInbox,
    ClosableAsyncObject,
    CompositeClosableAsyncObject,
    cancel_on_closing,
)


class Channel(ClosableAsyncObject):
    def on_open(self):
        super().on_open()
        self._linked_channel = None
        self._inbox = AsyncInbox(loop=self.loop)

    async def on_close(self, result):
        self._inbox.close()

        if self._linked_channel:
            self._linked_channel.close()
            self._linked_channel._linked_channel = None
            self._linked_channel = None

        await self._inbox.wait_closed()

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

    async def on_close(self, result):
        if self._tasks:
            await asyncio.wait(self._tasks[:])

        await super().on_close(result)

    def create_channel(self):
        if self.closing:
            raise asyncio.CancelledError()

        left = Channel(loop=self.loop)
        self.register_child(left)
        right = Channel(loop=self.loop)
        self.register_child(right)
        left.link(right)

        task = asyncio.ensure_future(self._handler(right))
        self._tasks.append(task)

        def remove_task(future):
            self._tasks.remove(task)

        task.add_done_callback(remove_task)
        return left
