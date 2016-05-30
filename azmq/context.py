"""
ZMQ context class implementation.
"""

import asyncio

from .common import (
    CompositeClosableAsyncObject,
    cancel_on_closing,
)
from .errors import InprocPathBound
from .socket import Socket


class InprocServer(CompositeClosableAsyncObject):
    def on_open(self):
        super().on_open()


class Context(CompositeClosableAsyncObject):
    """
    A ZMQ context.

    This class is **NOT** thread-safe.
    """
    def on_open(self):
        super().on_open()

        self._inproc_servers = {}

    async def on_close(self, result):
        tasks = []

        for server_future in self._inproc_servers.values():
            if not server_future.cancelled():
                if server_future.done():
                    server = server_future.result()
                    server.close()
                    tasks.append(asyncio.ensure_future(
                        server.wait_closed(),
                        loop=self.loop,
                    ))
                else:
                    server_future.cancel()

        self._inproc_servers.clear()

        if tasks:
            await asyncio.wait(tasks, loop=self.loop)

        await super().on_close(result)

    async def create_channel(self):
        pass

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
    async def _start_inproc_server(self, path):
        server_future = self._inproc_servers.get(path)

        if not server_future:
            server_future = self._inproc_servers[path] = \
                asyncio.Future(loop=self.loop)
        elif server_future.done():
            raise InprocPathBound(path=path)

        server = InprocServer()
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

        return await server.create_channel()
