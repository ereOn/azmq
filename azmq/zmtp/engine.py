"""
ZMTP engine.
"""

import asyncio

from random import random

from .log import logger
from .messaging import (
    write_greeting,
    write_short_greeting,
)

class Protocol(asyncio.Protocol):
    def __init__(self):
        self.transport = None
        self.first = True

    @property
    def peername(self):
        host, port = self.transport.get_extra_info('peername')
        return 'tcp://%s:%s' % (host, port)

    def connection_made(self, transport):
        self.transport = transport
        logger.debug("Connection to %r established.", self.peername)

    def connection_lost(self, exc):
        logger.debug("Connection to %r lost.", self.peername)

    def data_received(self, data):
        logger.debug("Received data: %r.", data)

        if self.first:
            self.first = False
            write_greeting(self.transport, (3, 1), 'NULL', False)

    def eof_received(self):
        logger.debug("Received eof.")

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass


class TCPClientEngine(object):
    def __init__(self, socket, host, port):
        self.socket = socket
        self.loop = socket.context.loop
        self.host = host
        self.port = port
        self.futures = set()
        self.initialize_connection()
        self.transport = None
        self.protocol = None

        self.retry_delay = 0.1
        self.min_retry_delay_factor = 1.1
        self.max_retry_delay_factor = 1.5
        self.max_retry_delay = 30.0

    def async_call(self, coro, callback):
        future = asyncio.ensure_future(coro, loop=self.loop)
        future.add_done_callback(self.futures.remove)
        future.add_done_callback(callback)
        self.futures.add(future)

    def increase_delay(self):
        self.retry_delay *= (
            self.min_retry_delay_factor + random() * (
                self.max_retry_delay_factor - self.min_retry_delay_factor
            )
        )

        if self.retry_delay > self.max_retry_delay:
            self.retry_delay = self.max_retry_delay

        return self.retry_delay

    def initialize_connection(self, future=None):
        self.async_call(
            self.loop.create_connection(
                Protocol,
                host=self.host,
                port=self.port,
            ),
            self.handle_connection,
        )

    def handle_connection(self, future):
        try:
            self.transport, self.protocol = future.result()
            self.retry_delay = 0.0
        except OSError as ex:
            retry_delay = self.increase_delay()

            logger.debug(
                "Could not establish connection to tcp://%s:%s (%s). Retrying "
                "in %0.2f second(s).",
                self.host,
                self.port,
                ex,
                retry_delay,
            )
            self.async_call(
                asyncio.sleep(retry_delay),
                self.initialize_connection,
            )

    def close(self):
        for future in self.futures:
            # We need to remove the done callback on the set or it will be
            # modified mid-iteration.
            future.remove_done_callback(self.futures.remove)
            future.cancel()

        self.futures.clear()

        if self.transport:
            #TODO: Call abort if LINGER=0 ?
            self.transport.close()


