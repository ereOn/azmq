"""
Base engine class.
"""

from pyslot import Signal


class BaseEngine(object):
    def __init__(self):
        super().__init__()
        self.on_protocol_created = Signal()
