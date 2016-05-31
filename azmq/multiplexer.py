"""
A class to ease dealing with multiple sockets at once.
"""

import asyncio

from .common import (
    ClosableAsyncObject,
    cancel_on_closing,
)


class Multiplexer(ClosableAsyncObject):
    def on_open(self):
        super().on_open()
        self._sockets = set()

    def add_socket(self, socket):
        """
        Add a socket to the multiplexer.

        :param socket: The socket. If it was added already, it won't be added a
            second time.
        """
        if socket not in self._sockets:
            self._sockets.add(socket)
            socket.on_closed.connect(self.remove_socket)

    def remove_socket(self, socket):
        """
        Remove a socket from the multiplexer.

        :param socket: The socket. If it was removed already or if it wasn't
            added, the call does nothing.
        """
        if socket in self._sockets:
            socket.on_closed.disconnect(self.remove_socket)
            self._sockets.remove(socket)

    @cancel_on_closing
    async def recv_multipart(self):
        """
        Read from all the associated sockets.

        :returns: A list of tuples (socket, frames) for each socket that
            returned a result.
        """
        if not self._sockets:
            return []

        results = []

        async def recv_and_store(socket):
            frames = await socket.recv_multipart()
            results.append((socket, frames))

        tasks = [
            asyncio.ensure_future(recv_and_store(socket), loop=self.loop)
            for socket in self._sockets
        ]

        try:
            await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED,
                loop=self.loop,
            )
        finally:
            for task in tasks:
                task.cancel()

        return results
