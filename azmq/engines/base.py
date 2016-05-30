"""
Base engine class.
"""

from pyslot import Signal


class BaseEngine(object):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.on_connection_ready = Signal()
        self.on_connection_lost = Signal()

    async def on_close(self, result):
        await super().on_close(result)

        try:
            await self.run_task
        except:
            pass

        return result
