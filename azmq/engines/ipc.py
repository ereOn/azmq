"""
IPC engines.
"""

import asyncio
import os
import sys

from ..connections.stream import StreamConnection
from ..log import logger

from .base import BaseEngine

if sys.platform == 'win32':
    from .win32 import start_pipe_server as start_ipc_server
    from .win32 import open_pipe_connection as open_ipc_connection
else:
    from asyncio import start_unix_server

    async def start_ipc_server(*args, **kwargs):
        try:
            os.unlink(kwargs['path'])
        except FileNotFoundError:
            pass

        return await start_unix_server(*args, **kwargs)

    from asyncio import open_unix_connection as open_ipc_connection


class IPCClientEngine(BaseEngine):
    def on_open(self, *, path, **kwargs):
        super().on_open(**kwargs)

        self.path = path

    async def open_connection(self):
        reader, writer = await open_ipc_connection(
            path=self.path,
            loop=self.loop,
        )

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
            on_failure=self.on_connection_failure,
            loop=self.loop,
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
            server = await start_ipc_server(
                self.handle_connection,
                path=self.path,
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
            on_failure=self.on_connection_failure,
            loop=self.loop,
        ) as connection:
            self.register_child(connection)
            await connection.wait_closed()

        logger.debug("Connection from %s lost.", self.path)
