"""
ZMQ context class implementation.
"""

import asyncio

from .common import ClosableAsyncObject
from .log import logger
from .socket import Socket


class Context(ClosableAsyncObject):
    """
    A ZMQ context.

    This class is **NOT** thread-safe.
    """
    def on_open(self):
        self.sockets = set()

        logger.debug("Context opened.")

    async def on_close(self):
        logger.debug("Context closing.")
        futures = []

        for socket in self.sockets:
            socket.close()
            futures.append(socket.wait_closed())

        await asyncio.gather(*futures)

    def on_closed(self):
        logger.debug("Context closed.")

    def socket(self, type):
        """
        Create and register a new socket.

        :param type: The type of the socket.
        :param loop: An optional event loop to associate the socket with.

        This is the preferred method to create new sockets.
        """
        return Socket(type=type, context=self, loop=self.loop)

    def register_socket(self, socket):
        """
        Register an existing socket.

        You should never need to call that method explicitely as it is done for
        you automatically when calling `socket`.

        :param socket: The socket to register.

        ..warning::
            It is the caller's responsibility to ensure that the `socket` was
            not registered already in this context or another one.
        """
        self.sockets.add(socket)

    def unregister_socket(self, socket):
        """
        Unregister an existing socket.

        You should never need to call that method explicitely as it is done for
        you automatically when closing the socket.

        :param socket: The socket to unregister.

        ..warning::
            It is the caller's responsibility to ensure that the `socket` was
            not registered already in this context or another one.
        """
        self.sockets.remove(socket)
