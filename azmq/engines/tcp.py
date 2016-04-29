"""
TCP engines.
"""

import asyncio

from ..common import (
    AsyncTaskObject,
    CompositeClosableAsyncObject,
)
from ..log import logger

from .base import BaseEngine


class Protocol(CompositeClosableAsyncObject):
    def __init__(self, reader, writer):
        super().__init__()
        self.reader = reader
        self.writer = writer

    async def on_close(self):
        self.writer.close()


class TCPClientEngine(BaseEngine, AsyncTaskObject):
    def __init__(self, host, port):
        super().__init__()
        self.host = host
        self.port = port
        # TODO: We will want to save a reference to the associated protocol to
        #make sure we close it when the engine closes.

    async def on_run(self):
        while True:
            try:
                reader, writer = await asyncio.open_connection(
                    host=self.host,
                    port=self.port,
                )
                protocol = Protocol(reader=reader, writer=writer)
                self.on_protocol_created.emit(protocol)

            except OSError as ex:
                logger.debug(
                    "Connection attempt to %s:%s failed (%s). Retrying...",
                    self.host,
                    self.port,
                    ex,
                )
            else:
                await protocol.wait_closed()
                protocol = None

                logger.debug(
                    "Connection to %s:%s closed. Retrying...",
                    self.host,
                    self.port,
                )


            await asyncio.sleep(0.5)
