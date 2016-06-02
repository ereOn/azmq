"""
Implements a ZMTP connection.
"""

import asyncio
import random
import socket
import struct

from ..constants import (
    PUB,
    XPUB,
    LEGAL_COMBINATIONS,
)
from ..errors import ProtocolError
from ..log import logger
from ..messaging import (
    Frame,
    dump_ready_command,
    load_ping_command,
    load_pong_command,
    load_ready_command,
    read_command,
    read_first_greeting,
    read_second_greeting,
    read_traffic,
    write_command,
    write_first_greeting,
    write_frames,
    write_ping_command,
    write_pong_command,
    write_second_greeting,
)

from ..common import (
    AsyncPeriodicTimer,
    AsyncTimeout,
)

from .base import BaseConnection


class StreamConnection(BaseConnection):
    """
    Implements a ZMTP connection that works on a pair of streams.
    """
    def __init__(
        self,
        reader,
        writer,
        attributes,
        on_ready,
        on_lost,
        **kwargs
    ):
        super().__init__(
            attributes=attributes,
            on_ready=on_ready,
            on_lost=on_lost,
            **kwargs,
        )
        self.reader = reader
        self.writer = writer
        self.writer.transport.get_extra_info('socket').setsockopt(
            socket.IPPROTO_TCP,
            socket.TCP_NODELAY,
            1,
        )
        self.version = None
        self.ping_period = 5
        self.ping_timeout = self.ping_period * 3
        self._send_ping_timer = None
        self._expect_ping_timeout = None
        self._base_ping_context = random.getrandbits(32)
        self._pending_ping_contexts = set()
        self._max_pending_pings = 3

    async def on_close(self, result):
        try:
            return await super().on_close(result)
        finally:
            self.writer.close()

    async def run(self):
        try:
            await self.on_run()
        except ProtocolError as ex:
            logger.debug("Protocol error (%s). Terminating connection.", ex)
        except asyncio.IncompleteReadError:
            logger.debug("Remote end was closed. Terminating connection.")
        except Exception:
            logger.exception("Unexpected error. Terminating connection.")
        finally:
            self.close()

    async def on_run(self):
        write_first_greeting(self.writer, major_version=3)
        major_version = await read_first_greeting(self.reader)

        if major_version < 3:
            logger.warning(
                "Unsupported peer's major version (%s). Disconnecting.",
                major_version,
            )
            return

        write_second_greeting(
            self.writer,
            minor_version=1,
            mechanism=b'NULL',
            as_server=False,
        )
        minor_version, mechanism, as_server = await read_second_greeting(
            self.reader,
        )
        logger.debug(
            "Peer is using version %s.%s with the '%s' authentication "
            "mechanism.",
            major_version,
            minor_version,
            mechanism.decode(),
        )
        self.version = (major_version, minor_version)

        if mechanism == b'NULL':
            write_command(
                self.writer,
                b'READY',
                dump_ready_command({
                    b'Socket-Type': self.local_socket_type,
                    b'Identity': self.local_identity,
                }),
            )
            command = await read_command(self.reader)

            if command.name != b'READY':
                logger.warning("Unexpected command: %s.", command.name)
                return

            peer_attributes = load_ready_command(command.data)
            logger.debug("Peer attributes: %s", peer_attributes)

            if (
                self.local_socket_type,
                peer_attributes[b'socket-type'],
            ) not in LEGAL_COMBINATIONS:
                logger.warning(
                    "Incompatible socket types (%s <-> %s). Killing the "
                    "connection.",
                    self.local_socket_type.decode(),
                    peer_attributes[b'socket-type'].decode(),
                )
                return

            self.identity = peer_attributes.get(b'identity', None)

            if self.identity:
                # Peer-specified identities can't start with b'\x00'.
                if not self.identity[0]:
                    logger.warning(
                        "Peer specified an invalid identity (%r). Killing the "
                        "connection.",
                        self.identity,
                    )
                    return
        else:
            logger.warning("Unsupported mechanism: %s.", mechanism)
            return

        logger.debug("Connection is now ready to read and write.")
        self.on_ready(self)
        assert self.inbox or self.outbox, (
            "on_ready callback must either set an inbox or an outbox."
        )

        if self.version >= (3, 1):
            # Make sure we send pings regularly.
            self._send_ping_timer = AsyncPeriodicTimer(
                coro=self._send_ping,
                period=self.ping_period,
            )
            self.register_child(self._send_ping_timer)

            # Make sure we expect pings regularly.
            self._expect_ping_timeout = AsyncTimeout(
                self.close,
                timeout=self.ping_timeout,
            )
            self.register_child(self._expect_ping_timeout)

        try:
            tasks = []

            if self.inbox:
                read_task = asyncio.ensure_future(self.read(), loop=self.loop)
                tasks.append(read_task)

            if self.outbox:
                write_task = asyncio.ensure_future(
                    self.write(),
                    loop=self.loop,
                )
                tasks.append(write_task)

            try:
                await self.await_until_closing(
                    asyncio.gather(*tasks, loop=self.loop),
                )
            except asyncio.CancelledError:
                logger.debug(
                    "Read/write cycle interrupted. Connection will die soon.",
                )
            finally:
                for task in tasks:
                    task.cancel()

                await asyncio.wait(tasks, loop=self.loop)
        finally:
            self.on_lost(self)

        # Flush out the unset outgoing messages before we exit.
        while not self.outbox.empty():
            write_frames(self.writer, self.outbox.read_nowait())

    async def read(self):
        frames = []

        while not self.closing:
            traffic = await read_traffic(self.reader)

            if self._expect_ping_timeout:
                self._expect_ping_timeout.revive()

            if isinstance(traffic, Frame):
                # ZMTP <=3.0 will send frames for subscriptions and
                # unsubscriptions.
                if self.local_socket_type in {PUB, XPUB}:
                    type_ = traffic.body[0]

                    if type_ == 1:
                        await self.subscribe(traffic.body[1:])
                    elif type_ == 0:
                        await self.unsubscribe(traffic.body[1:])
                else:
                    frames.append(traffic)

                    if traffic.last:
                        if not self._discard_incoming_messages:
                            await self.inbox.write(frames)

                        frames = []
            else:
                if self.version >= (3, 1):
                    if traffic.name == b'SUBSCRIBE':
                        await self.subscribe(traffic.data)
                    elif traffic.name == b'CANCEL':
                        await self.unsubscribe(traffic.data)
                    elif traffic.name == b'PING':
                        ttl, ping_ctx = load_ping_command(traffic.data)
                        logger.debug("Received PING (%r, %s).", ping_ctx, ttl)
                        self._expect_ping_timeout.revive()
                        self._send_pong(ping_ctx)
                    elif traffic.name == b'PONG':
                        ping_ctx = load_pong_command(traffic.data)
                        logger.debug("Received PONG (%r).", ping_ctx)
                        self._pending_ping_contexts.remove(ping_ctx)

    async def write(self):
        outbox = self.outbox

        while not self.closing:
            frames = await outbox.read()
            write_frames(self.writer, frames)

            if self._send_ping_timer:
                self._send_ping_timer.reset()

    def _generate_ping_context(self):
        """
        Generate a unique but random ping context.
        """
        ping_context = struct.pack('!I', self._base_ping_context)
        self._base_ping_context += 1

        if self._base_ping_context >= 2 ** 32:
            self._base_ping_context = 0

        return ping_context

    async def _send_ping(self):
        if len(self._pending_ping_contexts) < self._max_pending_pings:
            ping_ctx = self._generate_ping_context()
            logger.debug(
                "Sending PING (%r, %s).",
                ping_ctx,
                self.ping_timeout,
            )
            self._pending_ping_contexts.add(ping_ctx)
            write_ping_command(
                self.writer,
                self.ping_timeout,
                ping_ctx,
            )

    def _send_pong(self, ping_ctx):
        logger.debug("Sending PONG (%r).", ping_ctx)
        write_pong_command(self.writer, ping_ctx)
