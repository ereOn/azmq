"""
Base engine class.
"""

import asyncio

from pyslot import Signal

from ..common import (
    CompositeClosableAsyncObject,
    cancel_on_closing,
)


class BaseEngine(CompositeClosableAsyncObject):
    def __init__(
        self,
        *,
        socket_type,
        identity,
        mechanism,
        zap_client,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.socket_type = socket_type
        self.identity = identity
        self.mechanism = mechanism
        self.zap_client = zap_client
        self.on_connection_ready = Signal()
        self.on_connection_lost = Signal()

    def on_open(self, **kwargs):
        super().on_open(**kwargs)

        self.run_task = asyncio.ensure_future(self.run())

    @cancel_on_closing
    async def run(self):
        while not self.closing:
            try:
                await self.open_connection()
            except Exception:
                pass

            # TODO: Implement exponentional back-off.
            await asyncio.sleep(0.1)
