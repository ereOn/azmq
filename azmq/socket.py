"""
A ZMQ socket class implementation.
"""

import asyncio

from urllib.parse import urlsplit

from .common import ClosableAsyncObject
from .errors import UnsupportedSchemeError
from .log import logger
from .zmtp.engine import TCPClientEngine


class Socket(ClosableAsyncObject):
    """
    A ZMQ socket.

    This class is **NOT** thread-safe.
    """
    def on_open(self, type, context):
        self.type = type
        self.context = context
        self.identity = b''
        self.engines = set()
        self.context.register_socket(self)

        logger.debug("Socket opened.")

    async def on_close(self):
        logger.debug("Socket closing...")
        futures = []

        for engine in self.engines:
            engine.close()
            futures.append(engine.wait_closed())

        await asyncio.gather(*futures)

    def on_closed(self):
        logger.debug("Socket closed.")

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
