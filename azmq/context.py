"""
ZMQ context class implementation.
"""

import asyncio

from .common import (
    AsyncInbox,
    ClosableAsyncObject,
    CompositeClosableAsyncObject,
    cancel_on_closing,
)
from .errors import InprocPathBound
from .socket import Socket


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
        self._channel_pairs = []

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

        self._channel_pairs.append((left, right))
        task = asyncio.ensure_future(self._handler(right))
        self._tasks.append(task)

        def remove_task(future):
            self._tasks.remove(task)

        task.add_done_callback(remove_task)
        return left


class Context(CompositeClosableAsyncObject):
    """
    A ZMQ context.

    This class is **NOT** thread-safe.
    """
    def on_open(self):
        super().on_open()

        self._inproc_servers = {}

    async def on_close(self, result):
        for server_future in self._inproc_servers.values():
            if not server_future.cancelled():
                if not server_future.done():
                    server_future.cancel()

        self._inproc_servers.clear()

        await super().on_close(result)

    def socket(self, type):
        """
        Create and register a new socket.

        :param type: The type of the socket.
        :param loop: An optional event loop to associate the socket with.

        This is the preferred method to create new sockets.
        """
        socket = Socket(type=type, context=self, loop=self.loop)
        self.register_child(socket)
        return socket

    @cancel_on_closing
    async def _start_inproc_server(self, handler, path):
        server_future = self._inproc_servers.get(path)

        if not server_future:
            server_future = self._inproc_servers[path] = \
                asyncio.Future(loop=self.loop)
        elif server_future.done():
            raise InprocPathBound(path=path)

        server = InprocServer(loop=self.loop, handler=handler)
        self.register_child(server)
        server_future.set_result(server)
        return server

    @cancel_on_closing
    async def _open_inproc_connection(self, path):
        server_future = self._inproc_servers.get(path)

        if not server_future:
            server_future = self._inproc_servers[path] = \
                asyncio.Future(loop=self.loop)

        await server_future
        server = server_future.result()

        return server.create_channel()
