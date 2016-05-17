"""
A ZMQ socket class implementation.
"""

import asyncio

from urllib.parse import urlsplit
from itertools import chain

from .common import (
    CompositeClosableAsyncObject,
    cancel_on_closing,
)
from .constants import (
    REQ,
)
from .errors import UnsupportedSchemeError
from .engines.tcp import TCPClientEngine
from .log import logger
from .round_robin_list import RoundRobinList


class Socket(CompositeClosableAsyncObject):
    """
    A ZMQ socket.

    This class is **NOT** thread-safe.
    """
    def on_open(self, type, context):
        super().on_open()

        self.type = type
        self.context = context
        self.identity = b''
        self.engines = {}
        self.connections = RoundRobinList()

        if self.type == REQ:
            self._connection = None
            self._recv_func = self._recv_req
            self._send_func = self._send_req
        else:
            raise RuntimeError("Unsupported socket type: %r" % self.type)

    @property
    def attributes(self):
        attributes = {
            b'Socket-Type': self.type,
        }

        if self.identity:
            attributes[b'Identity'] = self.identity

        return attributes

    def connect(self, endpoint):
        url = urlsplit(endpoint)

        if url.scheme == 'tcp':
            engine = TCPClientEngine(
                host=url.hostname,
                port=url.port,
                attributes=self.attributes,
            )
            engine.on_connection_ready.connect(self.register_connection)
            engine.on_connection_lost.connect(self.unregister_connection)
        else:
            raise UnsupportedSchemeError(scheme=url.scheme)

        self.engines[url] = engine
        self.register_child(engine)

    def disconnect(self, endpoint):
        url = urlsplit(endpoint)

        engine = self.engines.pop(url)
        engine.close()

    def register_connection(self, connection):
        logger.debug("Registering new active connection: %s", connection)
        self.connections.append(connection)

    def unregister_connection(self, connection):
        logger.debug("Unregistering active connection: %s", connection)
        self.connections.remove(connection)

    async def _send_req(self, frames):
        assert self._connection is None
        self._connection = await self.connections.next()
        await self._connection.outbox.put([b''] + frames)

    async def _recv_req(self, frames):
        # TODO: The current implementation prevents waiting before we send.
        # This sucks.
        assert self._connection
        await self.connections.wait_not_empty()
        done, pending = await asyncio.wait(
            [connection.inbox.get() for connection in self.connections],
            return_when=asyncio.FIRST_COMPLETED,
        )

        it = chain(done, pending)
        result = await next(it)

        for task in it:
            task.cancel()

        return result

    @cancel_on_closing
    async def send_multipart(self, frames):
        return await self._send_func(frames=frames)

    @cancel_on_closing
    async def recv_multipart(self):
        return await self._recv_func()
