"""
Error and exceptions.
"""


class ZMTPFrameInvalid(RuntimeError):
    """
    A ZMTP-received frame could not be decoded.
    """
    pass
