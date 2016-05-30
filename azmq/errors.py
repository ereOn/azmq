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


class InvalidOperation(AZMQError):
    def __init__(self, msg):
        super().__init__(msg)


class InprocPathBound(AZMQError):
    def __init__(self, path):
        super().__init__("The inproc path %r is already bound" % path)
        self.path = path
