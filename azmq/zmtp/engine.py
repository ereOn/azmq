"""
ZMTP engine.
"""

import asyncio

from random import random

from .log import logger
from .messaging import (
    write_greeting,
    write_short_greeting,
)


class BaseEngine(object):
    """
    The base class for engines.
    """
    def __init__(self, socket, url):
        self.socket = socket
        self.url = url
        self.loop = socket.loop
        self.closed_future = asyncio.Future(loop=self.loop)
        self.closing = False
        self.futures = set()

        self.socket.register_engine(self)

        logger.debug("Engine opened.")

    @property
    def closed(self):
        return self.closed_future.done()

    async def wait_closed(self):
        """
        Wait for the engine to be closed.
        """
        await self.closed_future

    def close(self):
        """
        Close the engine.
        """
        if not self.closed and not self.closing:
            logger.debug("Engine closing...")
            self.closing = True

            for future in self.futures:
                future.cancel()

            def set_closed(_):
                logger.debug("Engine closed.")
                self.closed_future.set_result(True)
                self.socket.unregister_engine(self)

            asyncio.ensure_future(
                asyncio.gather(*self.futures),
            ).add_done_callback(set_closed)


class Protocol(asyncio.Protocol):
    def __init__(self):
        self.transport = None
        self.first = True

    @property
    def peername(self):
        host, port = self.transport.get_extra_info('peername')
        return 'tcp://%s:%s' % (host, port)

    def connection_made(self, transport):
        self.transport = transport
        logger.debug("Connection to %r established.", self.peername)

    def connection_lost(self, exc):
        logger.debug("Connection to %r lost.", self.peername)

    def data_received(self, data):
        logger.debug("Received data: %r.", data)

        if self.first:
            self.first = False
            write_greeting(self.transport, (3, 1), 'NULL', False)

    def eof_received(self):
        logger.debug("Received eof.")

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass


class TCPClientEngine(BaseEngine):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._async_connect()

    def _async_connect(self):
        future = asyncio.ensure_future(
            self.loop.create_connection(
                Protocol,
                host=self.url.hostname,
                port=self.url.port,
            ),
            loop=self.loop,
        )
        future.add_done_callback(self._handle_async_connect)
        self.futures.add(future)

    def _handle_async_connect(self, future):
        self.futures.remove(future)

        try:
            self.transport, self.protocol = future.result()
        except OSError as ex:
            logger.debug(
                "Connection attempt to %s failed (%s). Retrying...",
                self.url,
                ex,
            )
            self.loop.call_later(0.5, self._async_connect)
