"""
ZMQ context class implementation.
"""

import asyncio

from .log import logger
from .socket import Socket


class Context(object):
    """
    A ZMQ context.

    This class is **NOT** thread-safe.
    """
    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.closed_future = asyncio.Future(loop=self.loop)
        self.closing = False
        self.sockets = set()

        logger.debug("Context opened.")

    @property
    def closed(self):
        return self.closed_future.done()

    async def wait_closed(self):
        """
        Wait for the context to be closed.
        """
        await self.closed_future

    def close(self):
        """
        Close the context and all its associated sockets.
        """
        if not self.closed and not self.closing:
            logger.debug("Context closing...")
            self.closing = True
            futures = []

            for socket in self.sockets:
                socket.close()
                futures.append(socket.wait_closed())

            def set_closed(_):
                logger.debug("Context closed.")
                self.closed_future.set_result(True)

            asyncio.ensure_future(
                asyncio.gather(*futures),
            ).add_done_callback(set_closed)

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
