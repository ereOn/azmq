"""
Message parsing/producing utilities.
"""

import struct

from .errors import ZMTPFrameInvalid


SIGNATURE = b'\xff\x00\x00\x00\x00\x00\x00\x00\x00\x7f'
FILLER = b'\x00' * 31


def write_short_greeting(buffer, major_version):
    """
    Write a short greeting to the specified buffer.

    :param buffer: The buffer to write to.
    :param major_version: An integer that represents the major version number.
    """
    buffer.write(SIGNATURE)
    buffer.write(bytes([major_version]))


def write_greeting(buffer, version, mechanism, as_server):
    """
    Write a complete greeting to the specified buffer.

    :param buffer: The buffer to write to.
    :param version: A 2-tuple of (major, minor).
        :param mechanism: A mechanism string. Will be UTF-8 encoded and
        truncated to 20 bytes.
    :param as_server: A flag that indicates whether this client is acting as a
        server or a client for the current session.
    """
    buffer.write(SIGNATURE)
    buffer.write(bytes(version))
    buffer.write(mechanism.encode('utf-8')[:20].ljust(20, b'\x00'))
    buffer.write(b'\x01' if as_server else b'\x00')
    buffer.write(FILLER)


def _read(buffer, count):
    data = buffer.read(count)

    if len(data) < count:
        raise ZMTPFrameInvalid

    return data


def _check_read(buffer, expected):
    data = _read(buffer, len(expected))

    if data != expected:
        raise ZMTPFrameInvalid


def read_short_greeting(buffer):
    """
    Attempt to read a short greeting from the specified buffer.

    :param buffer: The buffer to read from.
    :returns: The major version number, as an integer.
    """
    _check_read(buffer, SIGNATURE)
    return struct.unpack('B', _read(buffer, 1))[0]


def read_greeting(buffer):
    """
    Attempt to read a greeting from the specified buffer.

    :param buffer: The buffer to read from.
    :returns: The 2-tuple version, the mecanism string and the `as-server`
        flag.
    """
    _check_read(buffer, SIGNATURE)
    version = tuple(struct.unpack('BB', _read(buffer, 2)))
    mechanism = _read(buffer, 20).rstrip(b'\x00').decode('utf-8')
    as_server = bool(struct.unpack('B', _read(buffer, 1))[0])
    _read(buffer, len(FILLER))

    return version, mechanism, as_server
