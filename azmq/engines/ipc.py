"""
IPC engines.
"""

import asyncio
import os

from ..connections.stream import StreamConnection
from ..log import logger

from .base import BaseEngine


class IPCClientEngine(BaseEngine):
    def on_open(self, *, path, **kwargs):
        super().on_open(**kwargs)

        self.path = path

    async def open_connection(self):
        try:
            reader, writer = await asyncio.open_unix_connection(
                path=self.path,
            )

        except OSError as ex:
            logger.debug(
                "Connection attempt to %s failed (%s). Retrying...",
                self.path,
                ex,
            )
        else:
            logger.debug("Connection to %s established.", self.path)

            async with StreamConnection(
                reader=reader,
                writer=writer,
                address=self.path,
                zap_client=self.zap_client,
                socket_type=self.socket_type,
                identity=self.identity,
                mechanism=self.mechanism,
                on_ready=self.on_connection_ready.emit,
                on_lost=self.on_connection_lost.emit,
            ) as connection:
                self.register_child(connection)
                await connection.wait_closed()

            logger.debug("Connection to %s closed.", self.path)


class IPCServerEngine(BaseEngine):
    def on_open(self, *, path, **kwargs):
        super().on_open(**kwargs)

        self.path = path

    async def open_connection(self):
        try:
            # Remove any stale UNIX socket.
            try:
                os.unlink(self.path)
            except FileNotFoundError:
                pass

            server = await asyncio.start_unix_server(
                self.handle_connection,
                path=self.path,
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
                "Unable to start UNIX server on %s.",
                self.path,
            )

    async def handle_connection(self, reader, writer):
        logger.debug("Connection from %s established.", self.path)

        async with StreamConnection(
            reader=reader,
            writer=writer,
            address=self.path,
            zap_client=self.zap_client,
            socket_type=self.socket_type,
            identity=self.identity,
            mechanism=self.mechanism,
            on_ready=self.on_connection_ready.emit,
            on_lost=self.on_connection_lost.emit,
        ) as connection:
            self.register_child(connection)
            await connection.wait_closed()

        logger.debug("Connection from %s lost.", self.path)
