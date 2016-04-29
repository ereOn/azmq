"""
TCP engines.
"""

import asyncio

from ..common import (
    ClosableAsyncObject,
    CompositeClosableAsyncObject,
)
from ..log import logger

from .base import BaseEngine


class Connection(ClosableAsyncObject):
    """
    Implements a ZMTP connection.

    If closed with the `True` result, instructs the managing engine to retry
    the connection.
    """
    def __init__(self, reader, writer):
        super().__init__()
        self.reader = reader
        self.writer = writer

    async def on_close(self, result):
        self.writer.close()
        return result


class TCPClientEngine(BaseEngine, CompositeClosableAsyncObject):
    def on_open(self, host, port):
        super().on_open()

        self.host = host
        self.port = port
        self.run_task = asyncio.ensure_future(self.run())

    async def on_close(self, result):
        await super().on_close(result)
        await self.run_task
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

                async with Connection(reader=reader, writer=writer) as \
                        connection:
                    self.register_child(connection)

                    if not await connection.wait_closed() or self.closing:
                        logger.debug(
                            "Connection to %s:%s closed.",
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
