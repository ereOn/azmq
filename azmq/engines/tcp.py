"""
TCP engines.
"""

import asyncio

from ..connections.stream import StreamConnection
from ..log import logger

from .base import BaseEngine


class TCPClientEngine(BaseEngine):
    def on_open(self, host, port, attributes):
        super().on_open()

        self.host = host
        self.port = port
        self.attributes = attributes
        self.run_task = asyncio.ensure_future(self.run())

    async def run(self):
        while not self.closing:
            try:
                reader, writer = await asyncio.open_connection(
                    host=self.host,
                    port=self.port,
                )

            except OSError as ex:
                logger.debug(
                    "Connection attempt to %s:%s failed (%s). Retrying...",
                    self.host,
                    self.port,
                    ex,
                )
            else:
                logger.debug(
                    "Connection to %s:%s established.",
                    self.host,
                    self.port,
                )

                async with StreamConnection(
                    reader=reader,
                    writer=writer,
                    attributes=self.attributes,
                    on_ready=self.on_connection_ready.emit,
                    on_lost=self.on_connection_lost.emit,
                ) as connection:
                    self.register_child(connection)
                    await connection.wait_closed()

                logger.debug(
                    "Connection to %s:%s closed.",
                    self.host,
                    self.port,
                )

            if not self.closing:
                await asyncio.sleep(0.5)


class TCPServerEngine(BaseEngine):
    def on_open(self, host, port, attributes):
        super().on_open()

        self.host = host
        self.port = port
        self.attributes = attributes
        self.run_task = asyncio.ensure_future(self.run())

    async def run(self):
        server = await asyncio.start_server(
            self.handle_connection,
            host=self.host,
            port=self.port,
        )

        try:
            await self.wait_closing()
        finally:
            server.close()
            await server.wait_closed()

    async def handle_connection(self, reader, writer):
        peername = ':'.join(map(str, writer.get_extra_info('peername')))

        logger.debug("Connection from %s established.", peername)

        async with StreamConnection(
            reader=reader,
            writer=writer,
            attributes=self.attributes,
            on_ready=self.on_connection_ready.emit,
            on_lost=self.on_connection_lost.emit,
        ) as connection:
            self.register_child(connection)
            await connection.wait_closed()

        logger.debug("Connection from %s lost.", peername)
