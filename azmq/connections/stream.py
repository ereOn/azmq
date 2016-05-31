"""
Implements a ZMTP connection.
"""

import asyncio

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
                            await self.inbox.write(frames)

                        frames = []
            else:
                if traffic.name == b'SUBSCRIBE':
                    await self.subscribe(traffic.data)
                elif traffic.name == b'CANCEL':
                    await self.unsubscribe(traffic.data)

    async def write(self):
        outbox = self.outbox

        while not self.closing:
            frames = await outbox.read()
            write_frames(self.writer, frames)
