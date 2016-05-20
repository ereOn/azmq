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
    PUB,
    XPUB,
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
        self.local_socket_type = attributes['socket_type']
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
        self._cant_read = asyncio.Event(loop=self.loop)
        self._cant_read.set()
        self._can_write = asyncio.Event(loop=self.loop)
        self._can_write.set()
        self._discard_incoming_messages = False
        self._discard_outgoing_messages = False
        self.subscriptions = []

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
        self._cant_read.set()

    def clear_outbox(self):
        while not self._outbox.empty():
            self._outbox.get_nowait()

        self._can_write.set()

    async def on_close(self, result):
        self.writer.close()
        await self.run_task
        return result

    @cancel_on_closing
    async def read_frames(self):
        result = await self._inbox.get()

        if self._inbox.empty():
            self._can_read.clear()
            self._cant_read.set()

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
            logger.debug("Protocol error (%s). Terminating connection.", ex)
        except asyncio.IncompleteReadError:
            logger.debug("Remote end was closed. Terminating connection.")
        except Exception:
            logger.exception("Unexpected error. Terminating connection.")
        finally:
            self.close()

            if self.local_socket_type == XPUB:
                # XPUB sockets must inform the application of the
                # unsubscription before they go to limbo.

                for topic in self.subscriptions[:]:
                    await self.unsubscribe(topic)

                # Wait for the inbox to be flushed by the application.
                await self._cant_read.wait()

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
        finally:
            read_task.cancel()
            write_task.cancel()

            await asyncio.wait([read_task, write_task], loop=self.loop)

    async def subscribe(self, topic):
        self.subscriptions.append(topic)
        logger.debug("Peer subscribed to topic %r.", topic)

        # XPUB sockets must inform the application of the subscription.
        if self.local_socket_type == XPUB:
            await self._inbox.put([b'\x01' + topic])
            self._can_read.set()
            self._cant_read.clear()

    async def unsubscribe(self, topic):
        try:
            self.subscriptions.remove(topic)
            logger.debug("Peer unsubscribed from topic %r.", topic)
        except ValueError:
            pass
        else:
            # XPUB sockets must inform the application of the unsubscription.
            if self.local_socket_type == XPUB:
                await self._inbox.put([b'\x00' + topic])
                self._can_read.set()
                self._cant_read.clear()

    async def read(self):
        frames = []

        while not self.closing:
            traffic = await read_traffic(self.reader)

            if isinstance(traffic, Frame):
                # ZMTP <=2.0 will send frames for subscriptions and
                # unsubscriptions.
                if traffic.body and self.local_socket_type in {PUB, XPUB}:
                    type_ = traffic.body[0]

                    if type_ == 1:
                        await self.subscribe(traffic.body[1:])
                    elif type_ == 0:
                        await self.unsubscribe(traffic.body[1:])
                else:
                    frames.append(traffic)

                    if traffic.last:
                        if not self._discard_incoming_messages:
                            await self._inbox.put(frames)
                            self._can_read.set()
                            self._cant_read.clear()

                        frames = []
            else:
                if traffic.name == b'SUBSCRIBE':
                    await self.subscribe(traffic.data)
                elif traffic.name == b'CANCEL':
                    await self.unsubscribe(traffic.data)

    async def write(self):
        while not self.closing:
            frames = await self._outbox.get()

            if not self._discard_outgoing_messages:
                self._can_write.set()
                write_frames(self.writer, frames)
