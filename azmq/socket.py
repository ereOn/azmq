"""
A ZMQ socket class implementation.
"""

import asyncio

from urllib.parse import urlsplit

from .common import CompositeClosableAsyncObject
from .errors import UnsupportedSchemeError
from .engines.tcp import TCPClientEngine


class Socket(CompositeClosableAsyncObject):
    """
    A ZMQ socket.

    This class is **NOT** thread-safe.
    """
    def on_open(self, type, context):
        super().on_open()

        self.type = type
        self.context = context
        self.identity = b''
        self.engines = {}
        self.connections = set()

    @property
    def attributes(self):
        attributes = {
            b'Socket-Type': self.type,
        }

        if self.identity:
            attributes[b'Identity'] = self.identity

        return attributes

    def connect(self, endpoint):
        url = urlsplit(endpoint)

        if url.scheme == 'tcp':
            engine = TCPClientEngine(
                host=url.hostname,
                port=url.port,
                attributes=self.attributes,
            )
            engine.on_connection_ready.connect(self.register_connection)
        else:
            raise UnsupportedSchemeError(scheme=url.scheme)

        self.engines[url] = engine
        self.register_child(engine)

    def disconnect(self, endpoint):
        url = urlsplit(endpoint)

        engine = self.engines.pop(url)
        engine.close()

    def register_connection(self, connection):
        self.connections.add(connection)
