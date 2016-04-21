"""
ZMQ context class implementation.
"""

from threading import Lock

from .log import logger
from .socket import Socket


class Context(object):
    """
    A ZMQ context.

    This class and all of its methods are thread-safe.
    """
    def __init__(self):
        self.lock = Lock()
        self.sockets = set()

        logger.debug("Context opened.")

    def close(self):
        """
        Close the context and all its associated sockets.
        """
        with self.lock:
            for socket in self.sockets:
                socket.loop.call_soon_threadsafe(socket.close)

        logger.debug("Context closed.")

    def socket(self, type, loop=None):
        """
        Create and register a new socket.

        :param type: The type of the socket.
        :param loop: An optional event loop to associate the socket with.

        This is the preferred method to create new sockets.
        """
        return Socket(context=self, loop=loop, type=type)

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
        with self.lock:
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
        with self.lock:
            self.sockets.remove(socket)
