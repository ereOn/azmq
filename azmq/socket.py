"""
A ZMQ socket class implementation.
"""

import asyncio

from urllib.parse import urlsplit

from .errors import UnsupportedSchemeError
from .log import logger
from .zmtp.engine import TCPClientEngine


class Socket(object):
    """
    A ZMQ socket.

    This class is **NOT** thread-safe.
    """
    def __init__(self, type, context, loop=None):
        self.context = context
        self.loop = loop or asyncio.get_event_loop()
        self.type = type
        self.identity = b''
        self.closed_future = asyncio.Future(loop=self.loop)
        self.closing = False
        self.engines = set()

        self.context.register_socket(self)

        logger.debug("Socket opened.")

    @property
    def closed(self):
        return self.closed_future.done()

    async def wait_closed(self):
        """
        Wait for the socket to be closed.
        """
        await self.closed_future

    def close(self):
        """
        Close the socket.
        """
        if not self.closed and not self.closing:
            logger.debug("Socket closing...")
            self.closing = True
            futures = []

            for engine in self.engines:
                engine.close()
                futures.append(engine.wait_closed())

            def set_closed(_):
                logger.debug("Socket closed.")
                self.closed_future.set_result(True)
                self.context.unregister_socket(self)

            asyncio.ensure_future(
                asyncio.gather(*futures),
            ).add_done_callback(set_closed)

    def register_engine(self, engine):
        """
        Register an existing engine.

        :param engine: The engine to register.

        ..warning::
            It is the caller's responsibility to ensure that the `engine` was
            not registered already in this socket or another one.
        """
        self.engines.add(engine)

    def unregister_engine(self, engine):
        """
        Unregister an existing engine.

        You should never need to call that method explicitely as it is done for
        you automatically when closing the engine.

        :param engine: The engine to unregister.

        ..warning::
            It is the caller's responsibility to ensure that the `engine` was
            not registered already in this socket or another one.
        """
        self.engines.remove(engine)

    def connect(self, endpoint):
        url = urlsplit(endpoint)

        if url.scheme == 'tcp':
            engine = TCPClientEngine(
                socket=self,
                url=url,
            )
        else:
            raise UnsupportedSchemeError(scheme=url.scheme)

    def disconnect(self, endpoint):
        url = urlsplit(endpoint)

        engine = next(engine for engine in self.engines if engine.url == url)
        engine.close()
