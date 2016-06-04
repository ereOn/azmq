"""
TCP engines.
"""

import asyncio

from ..connections.inproc import InprocConnection
from ..log import logger

from .base import BaseEngine


class InprocClientEngine(BaseEngine):
    def on_open(self, context, path):
        super().on_open()

        self.context = context
        self.path = path
        self.run_task = asyncio.ensure_future(self.run())

    async def run(self):
        while not self.closing:
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

                async with InprocConnection(
                    channel=channel,
                    socket_type=self.socket_type,
                    identity=self.identity,
                    mechanism=self.mechanism,
                    on_ready=self.on_connection_ready.emit,
                    on_lost=self.on_connection_lost.emit,
                ) as connection:
                    self.register_child(connection)
                    await connection.wait_closed()

                logger.debug(
                    "Connection to %s closed.",
                    self.path,
                )


class InprocServerEngine(BaseEngine):
    def on_open(self, context, path):
        super().on_open()

        self.context = context
        self.path = path
        self.run_task = asyncio.ensure_future(self.run())

    async def run(self):
        server = await self.context._start_inproc_server(
            self.handle_connection,
            path=self.path,
        )

        try:
            await self.wait_closing()
        finally:
            server.close()
            await server.wait_closed()

    async def handle_connection(self, channel):
        logger.debug("Connection from %s established.", channel)

        async with InprocConnection(
            channel=channel,
            socket_type=self.socket_type,
            identity=self.identity,
            mechanism=self.mechanism,
            on_ready=self.on_connection_ready.emit,
            on_lost=self.on_connection_lost.emit,
        ) as connection:
            self.register_child(connection)
            await connection.wait_closed()

        logger.debug("Connection from %s lost.", channel)
