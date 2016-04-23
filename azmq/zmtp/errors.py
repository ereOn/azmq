"""
Error and exceptions.
"""


class ZMTPFrameInvalid(RuntimeError):
    """
    A ZMTP-received frame could not be decoded.
    """


class UnsupportedMechanism(RuntimeError):
    """
    The mechanism is not supported.
    """


class UnexpectedCommand(RuntimeError):
    """
    A received command is unexpected.
    """
