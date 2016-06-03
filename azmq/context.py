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
from .transports.inproc import InprocServer


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
        server = None

        while not server or server.closing:
            if not server_future or server_future.cancelled():
                server_future = self._inproc_servers[path] = \
                    asyncio.Future(loop=self.loop)

            await asyncio.shield(server_future)
            server = server_future.result()
            server_future = None

        return server.create_channel()
