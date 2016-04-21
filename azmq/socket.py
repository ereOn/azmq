"""
A ZMQ socket class implementation.
"""

import asyncio

from urllib.parse import urlsplit
from itertools import izip

from .log import logger
from .errors import UnsupportedSchemeError


class Socket(object):
    """
    A ZMQ socket.

    This class is **NOT** thread-safe.
    """
    def __init__(self, context, loop, type):
        self.context = context
        self.loop = loop or asyncio.get_event_loop()
        self.type = type
        self.identity = None
        self.connections = {}
        self.bindings = {}

        logger.debug("Socket opened.")

        self.context.register_socket(self)

    def close(self):
        """
        Close the socket.

        This method is a coroutine. The returned result will be resolved when
        all the underlying engines are effectively closed.
        """
        for engine in izip(self.connections.keys(), self.bindings.keys()):
            engine.close()

        self.connections.clear()
        self.bindings.clear()

        context.unregister_socket(self)

        logger.debug("Socket closed.")

    def connect(self, endpoint):
        url = urlsplit(endpoint)

        if url.scheme == 'tcp':
            engine = TCPClientEngine(
                socket=self,
                host=url.hostname,
                port=url.port,
            )
            self.connections[url] = engine
        else:
            raise UnsupportedSchemeError(scheme=url.scheme)

    def disconnect(self, endpoint):
        url = urlsplit(endpoint)

        engine = self.connections.pop(url)
        engine.close()
