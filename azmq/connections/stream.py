"""
Implements a ZMTP connection.
"""

import asyncio
import random
import struct

from ..constants import (
    PUB,
    XPUB,
)
from ..errors import ProtocolError
from ..log import logger

from ..common import (
    AsyncPeriodicTimer,
    AsyncTimeout,
)

from .base import BaseConnection


class StreamConnection(BaseConnection):
    """
    Implements a ZMTP connection that works on a pair of streams.
    """
    def __init__(self, *, reader, writer, address, zap_client, **kwargs):
        super().__init__(**kwargs)
        self.reader = reader
        self.writer = writer
        self.address = address
        self.zap_client = zap_client
        self.version = None
        self.ping_period = 5
        self.ping_timeout = self.ping_period * 3
        self._send_ping_timer = None
        self._expect_ping_timeout = None
        self._base_ping_context = random.getrandbits(32)
        self._pending_ping_contexts = set()
        self._max_pending_pings = 3
        self._nonce = 0

    async def on_close(self, result):
        try:
            return await super().on_close(result)
        finally:
            self.writer.close()

    async def run(self):
        try:
            await self.on_run()
        except asyncio.CancelledError:
            logger.debug("Connection was closed.")
        except ProtocolError as ex:
            logger.debug("Protocol error (%s). Terminating connection.", ex)
        except asyncio.IncompleteReadError:
            logger.debug("Remote end was closed. Terminating connection.")
        except Exception:
            logger.exception("Unexpected error. Terminating connection.")
        finally:
            self.close()

    async def on_run(self):
        self.version = await self._greeting(
            reader=self.reader,
            writer=self.writer,
            version=(3, 1),
            mechanism=self.mechanism.name,
            as_server=self.mechanism.as_server,
        )

        # At worst, negotiation should end when the connection is closed.
        metadata, user_id, auth_metadata = await self.await_until_closing(
            self.mechanism.negotiate(
                reader=self.reader,
                writer=self.writer,
                metadata=self.get_metadata(),
                address=self.address,
                zap_client=self.zap_client,
            ),
        )
        self.set_remote_metadata(metadata)
        self.remote_user_id = user_id
        self.remote_auth_metadata = auth_metadata

        logger.debug("Connection is now ready to read and write.")
        self.on_ready(self)
        assert self.inbox or self.outbox, (
            "on_ready callback must either set an inbox or an outbox."
        )

        if self.version >= (3, 1):
            # Make sure we send pings regularly.
            self._send_ping_timer = AsyncPeriodicTimer(
                callback=self._send_ping,
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
            self.mechanism.write(self.writer, self.outbox.read_nowait())

    async def read(self):
        frames = []
        inbox = self.inbox
        read = self.mechanism.read

        while not self.closing:
            frame, last = await read(
                reader=self.reader,
                on_command=self._process_command,
            )

            if self._expect_ping_timeout:
                self._expect_ping_timeout.revive()

            # ZMTP <=3.0 will send frames for subscriptions and
            # unsubscriptions.
            if self.socket_type in {PUB, XPUB}:
                type_ = frame[0]

                if type_ == 1:
                    await self.subscribe(frame[1:])
                elif type_ == 0:
                    await self.unsubscribe(frame[1:])
            else:
                frames.append(frame)

                if last:
                    if not self._discard_incoming_messages:
                        await inbox.write(frames)

                    frames = []

    async def write(self):
        outbox = self.outbox
        write = self.mechanism.write

        while not self.closing:
            frames = await outbox.read()
            write(writer=self.writer, frames=frames)

            if self._send_ping_timer:
                self._send_ping_timer.reset()

    # Private methods.

    @staticmethod
    def _write_first_greeting(writer, major_version):
        signature = b'\xff\x00\x00\x00\x00\x00\x00\x00\x00\x7f'
        writer.write(signature)
        writer.write(bytes([major_version]))

    @staticmethod
    async def _read_first_greeting(reader):
        data = await reader.readexactly(10)

        if data[0] != 0xff:
            raise ProtocolError("Invalid signature", fatal=True)

        if data[-1] != 0x7f:
            raise ProtocolError("Invalid signature", fatal=True)

        major_version = struct.unpack('B', await reader.readexactly(1))[0]

        if major_version < 3:
            raise ProtocolError(
                "Unsupported peer major version (%s)." % major_version,
                fatal=True,
            )

        return major_version

    @staticmethod
    def _write_second_greeting(writer, minor_version, mechanism, as_server):
        filler = b'\x00' * 31
        writer.write(bytes([minor_version]))
        writer.write(mechanism[:20].ljust(20, b'\x00'))
        writer.write(b'\x01' if as_server else b'\x00')
        writer.write(filler)

    @staticmethod
    async def _read_second_greeting(reader):
        version = struct.unpack('B', await reader.readexactly(1))[0]
        mechanism = (await reader.readexactly(20)).rstrip(b'\x00')
        as_server = bool(struct.unpack('B', await reader.readexactly(1))[0])

        # Next is the filler. We don't care about it.
        await reader.readexactly(31)

        return version, mechanism, as_server

    @classmethod
    async def _greeting(cls, writer, reader, version, mechanism, as_server):
        logger.debug(
            "Using version %s.%s with the '%s' authentication "
            "mechanism as %s.",
            version[0],
            version[1],
            mechanism.decode(),
            'server' if as_server else 'client',
        )
        cls._write_first_greeting(
            writer=writer,
            major_version=version[0],
        )
        remote_major_version = await cls._read_first_greeting(reader=reader)
        cls._write_second_greeting(
            writer=writer,
            minor_version=version[1],
            mechanism=mechanism,
            as_server=as_server,
        )
        (
            remote_minor_version,
            remote_mechanism,
            remote_as_server,
        ) = await cls._read_second_greeting(reader=reader)

        logger.debug(
            "Peer is using version %s.%s with the '%s' authentication "
            "mechanism as %s.",
            remote_major_version,
            remote_minor_version,
            remote_mechanism.decode(),
            'server' if remote_as_server else 'client',
        )

        if remote_mechanism != mechanism:
            raise ProtocolError(
                "Incompatible mechanisms: %s == %s" % (
                    remote_mechanism,
                    mechanism,
                ),
                fatal=True,
            )

        remote_version = (remote_major_version, remote_minor_version)

        return min(version, remote_version)

    async def _process_command(self, name, data):
        if self.version >= (3, 1):
            if name == b'SUBSCRIBE':
                await self.subscribe(data)
            elif name == b'CANCEL':
                await self.unsubscribe(data)
            elif name == b'PING':
                self._process_ping_command(data)
            elif name == b'PONG':
                self._process_pong_command(data)

    def _generate_ping_context(self):
        ping_ctx = struct.pack('!I', self._base_ping_context)
        self._base_ping_context += 1

        if self._base_ping_context >= 2 ** 32:
            self._base_ping_context = 0

        return ping_ctx

    async def _send_ping(self):
        if len(self._pending_ping_contexts) < self._max_pending_pings:
            ping_ctx = self._generate_ping_context()
            logger.debug(
                "Sending PING (%r, %s).",
                ping_ctx,
                self.ping_timeout,
            )
            self._pending_ping_contexts.add(ping_ctx)
            self.mechanism.write_command(
                self.writer,
                b'PING',
                struct.pack('!H', int(self.ping_timeout * 10)),
                ping_ctx,
            )

    def _send_pong(self, ping_ctx):
        logger.debug("Sending PONG (%r).", ping_ctx)
        self.mechanism.write_command(self.writer, b'PONG', ping_ctx)

    def _process_ping_command(self, data):
        ttl = struct.unpack('!H', data[:2])[0]
        ping_ctx = data[2:]
        logger.debug("Received PING (%r, %s).", ping_ctx, ttl)
        self._expect_ping_timeout.revive()
        self._send_pong(ping_ctx)

    def _process_pong_command(self, ping_ctx):
        logger.debug("Received PONG (%r).", ping_ctx)
        self._pending_ping_contexts.remove(ping_ctx)
