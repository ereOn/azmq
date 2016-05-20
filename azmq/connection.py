"""
Implements a ZMTP connection.
"""

import asyncio

from contextlib import contextmanager

from .common import (
    ClosableAsyncObject,
    cancel_on_closing,
)
from .constants import (
    ROUTER,
    LEGAL_COMBINATIONS,
)
from .errors import ProtocolError
from .log import logger
from .messaging import (
    Frame,
    dump_ready_command,
    load_ready_command,
    read_command,
    read_first_greeting,
    read_second_greeting,
    read_traffic,
    write_command,
    write_first_greeting,
    write_frames,
    write_second_greeting,
)


class Connection(ClosableAsyncObject):
    CLOSE_RETRY = object()

    class Retry(RuntimeError):
        pass

    """
    Implements a ZMTP connection.

    If closed with `CLOSE_RETRY`, give a hint to the managing engine to retry
    the connection, if possible.
    """
    def __init__(self, reader, writer, attributes, **kwargs):
        super().__init__(**kwargs)
        self.reader = reader
        self.writer = writer
        self.socket_type = attributes['socket_type']
        self.local_identity = attributes.get('identity', b'')
        self.identity = None
        self._ready_future = asyncio.Future(loop=self.loop)
        self.run_task = asyncio.ensure_future(self.run(), loop=self.loop)
        self._inbox = asyncio.Queue(
            maxsize=attributes['max_inbox_size'],
            loop=self.loop,
        )
        self._outbox = asyncio.Queue(
            maxsize=attributes['max_outbox_size'],
            loop=self.loop,
        )
        self._can_read = asyncio.Event(loop=self.loop)
        self._can_read.set()
        self._can_write = asyncio.Event(loop=self.loop)
        self._can_write.set()
        self._discard_incoming_messages = False
        self._discard_outgoing_messages = False

    @property
    def ready(self):
        return self._ready_future.done()

    async def wait_ready(self):
        await self._ready_future

    def can_read(self):
        return self._can_read.is_set()

    def can_write(self):
        return self._can_write.is_set()

    async def wait_can_read(self):
        await self._can_read.wait()

    async def wait_can_write(self):
        await self._can_write.wait()

    def close_read(self):
        self._discard_incoming_messages = True
        self.clear_inbox()

    def close_write(self):
        self._discard_outgoing_messages = True
        self.clear_outbox()

    def clear_inbox(self):
        while not self._inbox.empty():
            self._inbox.get_nowait()

        self._can_read.clear()

    def clear_outbox(self):
        while not self._outbox.empty():
            self._outbox.get_nowait()

        self._can_write.clear()

    async def on_close(self, result):
        self.writer.close()
        await self.run_task
        return result

    @cancel_on_closing
    async def read_frames(self):
        result = await self._inbox.get()

        if self._inbox.empty():
            self._can_read.clear()

        return result

    @cancel_on_closing
    async def write_frames(self, frames):
        # Writing on a closing connection silently discards the messages.
        if not self.closing:
            await self._outbox.put(frames)

            if self._outbox.full():
                self._can_write.clear()

    @contextmanager
    def discard_incoming_messages(self, clear=True):
        """
        Discard all incoming messages for the time of the context manager.

        :param clear: A flag that, if set, also clears already received (but
            not read) incoming messages from the inbox. Default is `True`.
        """
        if clear:
            # Flush any received message so far.
            self.clear_inbox()

        # This allows nesting of discard_incoming_messages() calls.
        previous = self._discard_incoming_messages
        self._discard_incoming_messages = True

        try:
            yield
        finally:
            self._discard_incoming_messages = previous

    async def run(self):
        try:
            await self.on_run()
        except self.Retry:
            self.close(self.CLOSE_RETRY)
        except ProtocolError as ex:
            logger.info("Protocol error (%s). Terminating connection.", ex)
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

        if mechanism == b'NULL':
            write_command(
                self.writer,
                b'READY',
                dump_ready_command({
                    b'Socket-Type': self.socket_type,
                    b'Identity': self.local_identity,
                }),
            )
            command = await read_command(self.reader)

            if command.name != b'READY':
                logger.warning("Unexpected command: %s.", command.name)
                return

            peer_attributes = load_ready_command(command.data)
            logger.debug("Peer attributes: %s", peer_attributes)

            if (self.socket_type, peer_attributes[b'socket-type']) not in \
                    LEGAL_COMBINATIONS:
                logger.warning(
                    "Incompatible socket types (%s <-> %s). Killing the "
                    "connection.",
                    self.socket_type.decode(),
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
        self._ready_future.set_result(None)

        read_task = asyncio.ensure_future(self.read(), loop=self.loop)
        write_task = asyncio.ensure_future(self.write(), loop=self.loop)

        try:
            await self.await_until_closing(
                asyncio.gather(read_task, write_task, loop=self.loop),
            )
        except asyncio.CancelledError:
            logger.debug(
                "Read/write cycle interrupted. Connection will die soon.",
            )

    async def read(self):
        frames = []

        while True:
            traffic = await read_traffic(self.reader)

            if isinstance(traffic, Frame):
                frames.append(traffic)

                if traffic.last:
                    if not self._discard_incoming_messages:
                        await self._inbox.put(frames)
                        self._can_read.set()

                    frames = []
            else:
                # TODO: Handle potential commands.
                pass

    async def write(self):
        while True:
            frames = await self._outbox.get()

            if not self._discard_outgoing_messages:
                self._can_write.set()
                write_frames(self.writer, frames)
