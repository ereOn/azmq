"""
TCP engines.
"""

import asyncio

from ..connections.inproc import InprocConnection
from ..log import logger

from .base import BaseEngine


class InprocClientEngine(BaseEngine):
    def on_open(self, *, context, path, **kwargs):
        super().on_open(**kwargs)

        self.context = context
        self.path = path

    async def open_connection(self):
        try:
            channel = await self.await_until_closing(
                self.context._open_inproc_connection(
                    path=self.path,
                ),
            )
        except Exception as ex:
            logger.debug("Connection to %s failed: %s", self.path, ex)
        else:
            logger.debug(
                "Connection to %s established.",
                self.path,
            )

            try:
                async with channel:
                    async with InprocConnection(
                        channel=channel,
                        socket_type=self.socket_type,
                        identity=self.identity,
                        mechanism=self.mechanism,
                        on_ready=self.on_connection_ready.emit,
                        on_lost=self.on_connection_lost.emit,
                        on_failure=self.on_connection_failure,
                    ) as connection:
                        self.register_child(connection)
                        return await connection.wait_closed()
            finally:
                logger.debug(
                    "Connection to %s closed.",
                    self.path,
                )


class InprocServerEngine(BaseEngine):
    def on_open(self, context, path):
        super().on_open()

        self.context = context
        self.path = path

    async def open_connection(self):
        try:
            server = await self.context._start_inproc_server(
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
                "Unable to start INPROC server on %s.",
                self.path,
            )

    async def handle_connection(self, channel):
        logger.debug("Connection from %s established.", channel.path)

        async with InprocConnection(
            channel=channel,
            socket_type=self.socket_type,
            identity=self.identity,
            mechanism=self.mechanism,
            on_ready=self.on_connection_ready.emit,
            on_lost=self.on_connection_lost.emit,
            on_failure=self.on_connection_failure,
        ) as connection:
            self.register_child(connection)
            await connection.wait_closed()

        logger.debug("Connection from %s lost.", channel.path)
