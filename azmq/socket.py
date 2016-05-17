"""
A ZMQ socket class implementation.
"""

import asyncio

from urllib.parse import urlsplit
from itertools import chain
from contextlib import ExitStack

from .common import (
    CompositeClosableAsyncObject,
    cancel_on_closing,
)
from .constants import (
    REQ,
)
from .errors import (
    UnsupportedSchemeError,
    InvalidOperation,
)
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
            # This future holds the last connection we sent a request to (or
            # none, if no request was sent yet). This allows to start receiving
            # before we send.
            self._current_connection = asyncio.Future(loop=self.loop)
            self.recv_multipart = self._recv_req
            self.send_multipart = self._send_req
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

    @cancel_on_closing
    async def _send_req(self, frames):
        if self._current_connection.done():
            raise InvalidOperation(
                "Cannot send twice in a row from a REQ socket. Please recv "
                "from it first",
            )

        connection = await self.connections.next()
        await connection.outbox.put([b''] + frames)
        self._current_connection.set_result(connection)

    @cancel_on_closing
    async def _recv_req(self, frames):
        await self._current_connection
        connection = self._current_connection.result()

        # Let's allow writes back as soon as we know on which connection to
        # receive.
        self._current_connection = asyncio.Future(loop=self.loop)

        # As per 28/REQREP, a REQ socket SHALL discard silently any messages
        # received from other peers when processing incoming messages on the
        # current connection.
        with ExitStack() as stack:
            for conn in self.connections:
                if conn is not connection:
                    stack.enter_context(conn.discard_incoming_messages())

            return await connection.inbox.get()
