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
    def __init__(self, msg, fatal=False):
        super().__init__(msg)
        self.fatal = fatal


class InvalidOperation(AZMQError):
    def __init__(self, msg):
        super().__init__(msg)


class InprocPathBound(AZMQError):
    def __init__(self, path):
        super().__init__("The inproc path %r is already bound" % path)
        self.path = path


class ZAPError(AZMQError):
    def __init__(self, text, code):
        super().__init__("ZAP error: %s (%s)" % (text, code))
        self.text = text
        self.code = code


class ZAPTemporaryError(ZAPError):
    def __init__(self, text, code=300):
        super().__init__(text, code)


class ZAPAuthenticationFailure(ZAPError):
    def __init__(self, text, code=400):
        super().__init__(text, code)


class ZAPInternalError(ZAPError):
    def __init__(self, text, code=500):
        super().__init__(text, code)
