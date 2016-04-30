"""
TCP engines.
"""

import asyncio

from ..common import (
    ClosableAsyncObject,
    CompositeClosableAsyncObject,
)
from ..connection import Connection
from ..log import logger

from .base import BaseEngine


class TCPClientEngine(BaseEngine, CompositeClosableAsyncObject):
    def on_open(self, host, port, attributes):
        super().on_open()

        self.host = host
        self.port = port
        self.attributes = attributes
        self.run_task = asyncio.ensure_future(self.run())

    async def on_close(self, result):
        await super().on_close(result)

        try:
            await self.run_task
        except:
            pass

        return result

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

                async with Connection(
                    reader=reader,
                    writer=writer,
                    attributes=self.attributes,
                ) as connection:
                    self.register_child(connection)

                    await asyncio.wait(
                        [
                            connection.wait_ready(),
                            connection.wait_closed(),
                        ],
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    if connection.ready:
                        pass

                    if (
                        await connection.wait_closed() != \
                        Connection.CLOSE_RETRY
                    ) or self.closing:
                        logger.debug(
                            "Connection to %s:%s closed definitely.",
                            self.host,
                            self.port,
                        )
                        break
                    else:
                        logger.debug(
                            "Connection to %s:%s closed. Retrying...",
                            self.host,
                            self.port,
                        )

            await asyncio.sleep(0.5)
