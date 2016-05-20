"""
Implements a ZMTP connection.
"""

import asyncio

from contextlib import contextmanager

from .common import (
    ClosableAsyncObject,
    cancel_on_closing,
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

    If closed with `CLOSE_RETRY`, instructs the managing engine to retry the
    connection.
    """
    def __init__(self, reader, writer, attributes, **kwargs):
        super().__init__(**kwargs)
        self.reader = reader
        self.writer = writer
        self.socket_type = attributes['socket_type']
        self.identity = attributes.get('identity', b'')
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
        self._can_send = asyncio.Event(loop=self.loop)
        self._can_send.set()
        self._discard_incoming_messages = False

    @property
    def ready(self):
        return self._ready_future.done()

    async def wait_ready(self):
        await self._ready_future

    def can_send(self):
        return self._can_send.is_set()

    async def wait_can_send(self):
        await self._can_send.wait()

    async def on_close(self, result):
        self.writer.close()
        await self.run_task
        return result

    @cancel_on_closing
    async def read_frames(self):
        result = await self._inbox.get()
        return result

    @cancel_on_closing
    async def write_frames(self, frames):
        # Writing on a closing connection silently discards the messages.
        if not self.closing:
            await self._outbox.put(frames)

            if self._outbox.full():
                self._can_send.clear()

    @contextmanager
    def discard_incoming_messages(self, clear=True):
        """
        Discard all incoming messages for the time of the context manager.

        :param clear: A flag that, if set, also clears already received (but
            not read) incoming messages from the inbox. Default is `True`.
        """
        if clear:
            # Flush any received message so far.
            while not self._inbox.empty():
                self._inbox.get_nowait()

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
                    b'Identity': self.identity,
                }),
            )
            command = await read_command(self.reader)

            if command.name != b'READY':
                logger.warning("Unexpected command: %s.", command.name)
                return

            peer_attributes = load_ready_command(command.data)
            logger.debug("Peer attributes: %s", peer_attributes)
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

                    frames = []
            else:
                # TODO: Handle potential commands.
                pass

    async def write(self):
        while True:
            frames = await self._outbox.get()
            self._can_send.set()
            write_frames(self.writer, frames)
