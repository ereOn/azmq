"""
Base engine class.
"""

from pyslot import Signal

from ..common import CompositeClosableAsyncObject


class BaseEngine(CompositeClosableAsyncObject):
    def __init__(self, *, socket_type, identity, mechanism, **kwargs):
        super().__init__(**kwargs)
        self.socket_type = socket_type
        self.identity = identity
        self.mechanism = mechanism
        self.on_connection_ready = Signal()
        self.on_connection_lost = Signal()

    async def on_close(self, result):
        await super().on_close(result)

        try:
            await self.run_task
        except:
            pass

        return result
