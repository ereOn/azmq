"""
Exceptions classes.
"""


class AZMQError(RuntimeError):
    def __init__(self, msg="Unspecified AZMQ error"):
        super().__init__(msg)


class UnsupportedSchemeError(AZMQError):
    def __init__(self, scheme):
        super().__init__("Unsupported scheme '%s'" % scheme)
        self.scheme = scheme


class ProtocolError(AZMQError):
    pass
