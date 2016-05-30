"""
TCP engines.
"""

import asyncio

from ..connection import InprocConnection
from ..log import logger

from .base import BaseEngine


class InprocClientEngine(BaseEngine):
    def on_open(self, context, path, attributes):
        super().on_open()

        self.context = context
        self.attributes = attributes
        self.run_task = asyncio.ensure_future(self.run())

    async def run(self):
        while not self.closing:
            channel = await self.context._open_inproc_connection(
                path=self.path,
            )

            logger.debug(
                "Connection to %s:%s established.",
                self.host,
                self.port,
            )

            async with InprocConnection(
                channel=channel,
                attributes=self.attributes,
                on_ready=self.on_connection_ready.emit,
                on_lost=self.on_connection_lost.emit,
            ) as connection:
                self.register_child(connection)
                await connection.wait_closed()

            logger.debug(
                "Connection to %s closed.",
                self.path,
            )

            if not self.closing:
                await asyncio.sleep(0.5)


class InprocServerEngine(BaseEngine):
    def on_open(self, context, path, attributes):
        super().on_open()

        self.context = context
        self.path = path
        self.attributes = attributes
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
            attributes=self.attributes,
            on_ready=self.on_connection_ready.emit,
            on_lost=self.on_connection_lost.emit,
        ) as connection:
            self.register_child(connection)
            await connection.wait_closed()

        logger.debug("Connection from %s lost.", channel)
