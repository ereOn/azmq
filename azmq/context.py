"""
ZMQ context class implementation.
"""

import asyncio

from .common import (
    CompositeClosableAsyncObject,
    cancel_on_closing,
)
from .log import logger
from .errors import InprocPathBound
from .socket import Socket
from .transports.inproc import InprocServer
from .zap import ZAPClient


class Context(CompositeClosableAsyncObject):
    """
    A ZMQ context.

    This class is **NOT** thread-safe.
    """
    def on_open(self):
        super().on_open()

        self._inproc_servers = {}
        self._zap_authenticator = None
        self.zap_client = None

    async def on_close(self, result):
        for server_future in self._inproc_servers.values():
            if not server_future.cancelled():
                if not server_future.done():
                    server_future.cancel()

        self._inproc_servers.clear()

        await super().on_close(result)

    def socket(self, socket_type, identity=None, mechanism=None):
        """
        Create and register a new socket.

        :param socket_type: The type of the socket.
        :param loop: An optional event loop to associate the socket with.

        This is the preferred method to create new sockets.
        """
        socket = Socket(
            context=self,
            socket_type=socket_type,
            identity=identity,
            mechanism=mechanism,
            loop=self.loop,
        )
        self.register_child(socket)
        return socket

    def set_zap_authenticator(self, zap_authenticator):
        """
        Setup a ZAP authenticator.

        :param zap_authenticator: A ZAP authenticator instance to use. The
            context takes ownership of the specified instance. It will close it
            automatically when it stops. If `None` is specified, any previously
            owner instance is disowned and returned. It becomes the caller's
            responsibility to close it.
        :returns: The previous ZAP authenticator instance.
        """
        result = self._zap_authenticator

        if result:
            self.unregister_child(result)

        self._zap_authenticator = zap_authenticator
        self.register_child(zap_authenticator)

        if self.zap_client:
            self.zap_client.close()

        if self._zap_authenticator:
            self.zap_client = ZAPClient(context=self)
            self.register_child(self.zap_client)
        else:
            self.zap_client = None

        return result

    @cancel_on_closing
    async def _start_inproc_server(self, handler, path):
        server_future = self._inproc_servers.get(path)

        if not server_future:
            server_future = self._inproc_servers[path] = \
                asyncio.Future(loop=self.loop)
        elif server_future.done():
            server = server_future.result()

            if server.closing:
                await server.wait_closed()
                server_future = self._inproc_servers[path] = \
                    asyncio.Future(loop=self.loop)
            else:
                raise InprocPathBound(path=path)

        server = InprocServer(loop=self.loop, handler=handler)
        logger.debug("Starting inproc server on %r.", path)
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

        return server.create_channel(path)
