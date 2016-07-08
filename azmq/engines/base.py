"""
Base engine class.
"""

import asyncio

from pyslot import Signal

from ..common import (
    CompositeClosableAsyncObject,
    cancel_on_closing,
)
from ..errors import ProtocolError
from ..log import logger


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
        self.on_connection_failure = Signal()
        self.max_backoff_duration = 300  # 5 minutes.
        self.min_backoff_duration = 0.001
        self.current_backoff_duration = self.min_backoff_duration

    def on_open(self, **kwargs):
        super().on_open(**kwargs)

        self.run_task = asyncio.ensure_future(self.run(), loop=self.loop)

    @cancel_on_closing
    async def run(self):
        while not self.closing:
            try:
                result = await self.open_connection()

                if isinstance(result, ProtocolError) and result.fatal:
                    logger.warning("Fatal error: %s. Not restarting.", result)
                    break

            except asyncio.CancelledError:
                break

            except Exception as ex:
                logger.warning("Connection error: %r.", ex)
            else:
                self.current_backoff_duration = self.min_backoff_duration

            await asyncio.sleep(self.current_backoff_duration, loop=self.loop)

            self.current_backoff_duration = min(
                self.max_backoff_duration,
                self.current_backoff_duration * 2,
            )
