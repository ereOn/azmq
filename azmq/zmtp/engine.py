"""
ZMTP engine.
"""

import asyncio

from enum import Enum
from io import BytesIO
from random import random

from .log import logger
from .messaging import (
    dump_ready_command,
    load_ready_command,
    read_command,
    read_first_greeting,
    read_second_greeting,
    write_command,
    write_first_greeting,
    write_second_greeting,
)
from .errors import (
    UnexpectedCommand,
    UnsupportedMechanism,
)


class State(Enum):
    version_negotiation = 1
    authentication = 2


class TCPClient(object):
    """
    A generic TCP client.
    """

    def __init__(self, engine, reader, writer):
        self.engine = engine
        self.loop = engine.loop
        self.reader = reader
        self.writer = writer
        self.state = State.version_negotiation
        self.closed_future = asyncio.Future(loop=self.loop)
        self.closing = False
        self.run_task = asyncio.ensure_future(self.run(), loop=self.loop)
        self.run_task.add_done_callback(self.on_run_done)

        logger.debug("TCP client opened.")

    @property
    def closed(self):
        return self.closed_future.done()

    async def wait_closed(self):
        """
        Wait for the engine to be closed.
        """
        await self.closed_future

    def close(self):
        """
        Close the client.
        """
        if not self.closed and not self.closing:
            logger.debug("TCP client closing...")
            self.closing = True

            if self.run_task:
                self.run_task.cancel()

    async def run(self):
        """
        The messaging loop.
        """
        write_first_greeting(self.writer, major_version=3)
        await read_first_greeting(self.reader, 3)

        write_second_greeting(
            self.writer,
            minor_version=1,
            mechanism=b'NULL',
            as_server=False,
        )
        minor_version, mechanism, as_server = await read_second_greeting(
            self.reader,
        )

        if mechanism == b'NULL':
            write_command(self.writer, b'READY', dump_ready_command({
                b'Identity': self.engine.socket.identity,
                b'Socket-Type': self.engine.socket.type,
            }))
            command_name, command_data = await read_command(self.reader)

            if command_name != b'READY':
                raise UnexpectedCommand(
                    "Unexpected command: %s" % command_name,
                )

            ready_values = load_ready_command(command_data)
            print(ready_values)
        else:
            raise UnsupportedMechanism("Unsupported mechanism: %s" % mechanism)

    async def write(self, remote_socket_type, remote_identity):
        while True:
            frames = await self.write_queue.get()

    def on_run_done(self, future):
        self.run_task = None
        self.writer.close()

        if future.cancelled():
            logger.debug("TCP client closed after cancellation.")
        else:
            logger.debug("TCP client closed (%s).", future.exception())

        self.closed_future.set_result(True)
        self.engine.on_connection_lost()
