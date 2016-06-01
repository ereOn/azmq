"""
A ZMTP connection over the inproc transport.
"""

import asyncio

from ..constants import (
    PUB,
    XPUB,
    LEGAL_COMBINATIONS,
)
from ..log import logger
from .base import BaseConnection


class InprocConnection(BaseConnection):
    """
    Implements a ZMTP connection that works in-process.
    """
    def __init__(
        self,
        channel,
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
        self.channel = channel

    async def run(self):
        try:
            await self.on_run()
        except Exception:
            logger.exception("Unexpected error. Terminating connection.")
        finally:
            self.close()

    async def on_run(self):
        await self.channel.write(b'NULL')
        mechanism = await self.channel.read()

        if mechanism == b'NULL':
            await self.channel.write(
                {
                    b'socket-type': self.local_socket_type,
                    b'identity': self.local_identity,
                },
            )
            peer_attributes = await self.channel.read()
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
            await self.channel.write(self.outbox.read_nowait())

    async def read(self):
        while not self.closing:
            frames = await self.channel.read()

            if self.local_socket_type in {PUB, XPUB}:
                type_ = frames[0][0]

                if type_ == 1:
                    await self.subscribe(frames[0][1:])
                elif type_ == 0:
                    await self.unsubscribe(frames[0][1:])
            else:
                if not self._discard_incoming_messages:
                    await self.inbox.write(frames)

    async def write(self):
        outbox = self.outbox

        while not self.closing:
            frames = await outbox.read()
            await self.channel.write(frames)
