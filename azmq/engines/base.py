"""
Base engine class.
"""

from pyslot import Signal


class BaseEngine(object):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.on_connection_ready = Signal()
        self.on_connection_lost = Signal()
