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
    REP,
    REQ,
)
from .errors import (
    UnsupportedSchemeError,
    InvalidOperation,
)
from .engines.tcp import (
    TCPClientEngine,
    TCPServerEngine,
)
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
        self.outgoing_engines = {}
        self.incoming_engines = {}
        self.connections = RoundRobinList()

        if self.type == REQ:
            # This future holds the last connection we sent a request to (or
            # none, if no request was sent yet). This allows to start receiving
            # before we send.
            self._current_connection = asyncio.Future(loop=self.loop)
            self.recv_multipart = self._recv_req
            self.send_multipart = self._send_req
        elif self.type == REP:
            # This future holds the last connection we received a request from
            # (or none, if no request was received yet). This allows to start
            # receiving before we send.
            self._current_connection = asyncio.Future(loop=self.loop)
            self.recv_multipart = self._recv_rep
            self.send_multipart = self._send_rep
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

        self.outgoing_engines[url] = engine
        self.register_child(engine)

    def disconnect(self, endpoint):
        url = urlsplit(endpoint)

        engine = self.outgoing_engines.pop(url)
        engine.close()

    def bind(self, endpoint):
        url = urlsplit(endpoint)

        if url.scheme == 'tcp':
            engine = TCPServerEngine(
                host=url.hostname,
                port=url.port,
                attributes=self.attributes,
            )
            engine.on_connection_ready.connect(self.register_connection)
            engine.on_connection_lost.connect(self.unregister_connection)
        else:
            raise UnsupportedSchemeError(scheme=url.scheme)

        self.incoming_engines[url] = engine
        self.register_child(engine)

    def unbind(self, endpoint):
        url = urlsplit(endpoint)

        engine = self.incoming_engines.pop(url)
        engine.close()

    def register_connection(self, connection):
        logger.debug("Registering new active connection: %s", connection)
        self.connections.append(connection)

    def unregister_connection(self, connection):
        logger.debug("Unregistering active connection: %s", connection)
        self.connections.remove(connection)

    async def _fair_recv(self):
        """
        Receive from all the existing connections, rotating the list of
        connections every time.

        :returns: A pair of connection, frames.
        """
        await self.connections.wait_not_empty()

        # This offsets the list, which helps us provide fair-queuing.
        self.connections.next()
        connections = list(self.connections)

        read_tasks = [
            asyncio.ensure_future(connection.read_frames(), loop=self.loop)
            for connection in connections
        ]
        done, pending = await asyncio.wait(
            read_tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )

        connection, read_task = next(
            (conn, read_tasks[index])
            for index, conn in enumerate(connections)
            if read_tasks[index] in done
        )

        for task in chain(done, pending):
            if task is not read_task:
                task.cancel()

        return connection, await read_task

    @cancel_on_closing
    async def _send_req(self, frames):
        if self._current_connection.done():
            raise InvalidOperation(
                "Cannot send twice in a row from a REQ socket. Please recv "
                "from it first",
            )

        connection = await self.connections.get_next()
        await connection.write_frames([b''] + frames)
        self._current_connection.set_result(connection)

    @cancel_on_closing
    async def _recv_req(self):
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

            frames = await connection.read_frames()

            # We need to get rid of the empty delimiter.
            if frames[0] != b'':
                logger.warning(
                    "Received unexpected reply (%r) in REQ socket. Closing "
                    "connection. The recv() call will NEVER return !",
                    frames,
                )
                connection.close()

                # This may seem weird but we must treat these errors as if the
                # peer did never reply, which means blocking forever (at least
                # until the socket is closed).
                #
                # ZMQ best-practices dictate the user should recreate the socket
                # anyway in case of timeouts and there is no other sensible
                # course of action: we can't return anything meaningful and
                # throwing an error puts the burden on the user by forcing
                # him/her to handle two possible outcomes.
                forever = asyncio.Future(loop=self.loop)
                await forever

            frames.pop(0)
            return frames

    @cancel_on_closing
    async def _send_rep(self, frames):
        await self._current_connection
        connection, envelope = self._current_connection.result()

        # Let's allow reads back as soon as we know on which connection to
        # receive.
        self._current_connection = asyncio.Future(loop=self.loop)

        await connection.write_frames(envelope + frames)

    @cancel_on_closing
    async def _recv_rep(self):
        if self._current_connection.done():
            raise InvalidOperation(
                "Cannot receive twice in a row from a REP socket. Please send "
                "from it first",
            )

        connection, frames = await self._fair_recv()
        delimiter_index = frames.index(b'')
        envelope = frames[:delimiter_index + 1]
        message = frames[delimiter_index + 1:]
        self._current_connection.set_result((connection, envelope))
        return message
