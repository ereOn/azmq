"""
A ZMQ socket class implementation.
"""

import asyncio

from urllib.parse import urlsplit

from .common import CompositeClosableAsyncObject
from .errors import UnsupportedSchemeError
from .zmtp.engine import TCPClientEngine


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
        self.context.register_child(self)

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

        engine = next(engine for engine in self.children if engine.url == url)
        engine.close()
