"""
ZMQ context class implementation.
"""

import asyncio

from .common import CompositeClosableAsyncObject
from .socket import Socket


class Context(CompositeClosableAsyncObject):
    """
    A ZMQ context.

    This class is **NOT** thread-safe.
    """
    def socket(self, type):
        """
        Create and register a new socket.

        :param type: The type of the socket.
        :param loop: An optional event loop to associate the socket with.

        This is the preferred method to create new sockets.
        """
        return Socket(type=type, context=self, loop=self.loop)
