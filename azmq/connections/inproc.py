"""
A ZMTP connection over the inproc transport.
"""

import asyncio

from ..constants import (
    PUB,
    XPUB,
)
from ..errors import ProtocolError
from ..log import logger
from .base import BaseConnection


class InprocConnection(BaseConnection):
    """
    Implements a ZMTP connection that works in-process.
    """
    def __init__(
        self,
        channel,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.channel = channel
        self.register_child(self.channel)

    async def on_close(self):
        self.channel.close()
        return await super().on_close()

    async def on_run(self):
        await self.channel.write(self.get_metadata())
        metadata = await self.channel.read()
        self.set_remote_metadata(metadata)

        logger.debug("Connection is now ready to read and write.")
        self.on_ready(self)

        assert self.inbox or self.outbox, (
            "on_ready callback must either set an inbox or an outbox."
        )

        try:
            tasks = []
            read_task = asyncio.ensure_future(self.read(), loop=self.loop)
            tasks.append(read_task)
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
        while True:
            frames = await self.channel.read()

            if self.socket_type in {PUB, XPUB}:
                type_ = frames[0][0]

                if type_ == 1:
                    await self.subscribe(frames[0][1:])
                elif type_ == 0:
                    await self.unsubscribe(frames[0][1:])
                else:
                    raise ProtocolError(
                        "Unexpected subscription message type (%s)." % type_,
                    )
            elif not self._discard_incoming_messages:
                await self.inbox.write(frames)

    async def write(self):
        outbox = self.outbox

        while True:
            frames = await outbox.read()
            await self.channel.write(frames)
