"""
TCP engines.
"""

import asyncio
import socket

from ..connections.stream import StreamConnection
from ..log import logger

from .base import BaseEngine


class TCPClientEngine(BaseEngine):
    def on_open(self, *, host, port, **kwargs):
        super().on_open(**kwargs)

        self.host = host
        self.port = port

    async def open_connection(self):
        try:
            reader, writer = await asyncio.open_connection(
                host=self.host,
                port=self.port,
                loop=self.loop,
            )

        except OSError as ex:
            logger.debug(
                "Connection attempt to %s:%s failed (%s). Retrying...",
                self.host,
                self.port,
                ex,
            )
        else:
            writer.transport.get_extra_info('socket').setsockopt(
                socket.IPPROTO_TCP,
                socket.TCP_NODELAY,
                1,
            )

            address, port = writer.get_extra_info('peername')
            logger.debug(
                "Connection to %s:%s established.",
                address,
                port,
            )

            try:
                async with StreamConnection(
                    reader=reader,
                    writer=writer,
                    address=address,
                    zap_client=self.zap_client,
                    socket_type=self.socket_type,
                    identity=self.identity,
                    mechanism=self.mechanism,
                    on_ready=self.on_connection_ready.emit,
                    on_lost=self.on_connection_lost.emit,
                    on_failure=self.on_connection_failure,
                    loop=self.loop,
                ) as connection:
                    self.register_child(connection)
                    return await connection.wait_closed()
            finally:
                logger.debug(
                    "Connection to %s:%s closed.",
                    address,
                    port,
                )


class TCPServerEngine(BaseEngine):
    def on_open(self, *, host, port, **kwargs):
        super().on_open(**kwargs)

        self.host = host
        self.port = port

    async def open_connection(self):
        try:
            server = await asyncio.start_server(
                self.handle_connection,
                host=self.host,
                port=self.port,
                loop=self.loop,
            )

            try:
                await server.wait_closed()
            finally:
                server.close()
                await server.wait_closed()

        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Unable to start TCP server on %s:%s.",
                self.host,
                self.port,
            )

    async def handle_connection(self, reader, writer):
        address, port = writer.get_extra_info('peername')
        peername = '%s:%s' % (address, port)

        logger.debug("Connection from %s established.", peername)
        writer.transport.get_extra_info('socket').setsockopt(
            socket.IPPROTO_TCP,
            socket.TCP_NODELAY,
            1,
        )

        async with StreamConnection(
            reader=reader,
            writer=writer,
            address=address,
            zap_client=self.zap_client,
            socket_type=self.socket_type,
            identity=self.identity,
            mechanism=self.mechanism,
            on_ready=self.on_connection_ready.emit,
            on_lost=self.on_connection_lost.emit,
            on_failure=self.on_connection_failure,
            loop=self.loop,
        ) as connection:
            self.register_child(connection)
            await connection.wait_closed()

        logger.debug("Connection from %s lost.", peername)
