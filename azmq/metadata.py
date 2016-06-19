"""
Metadata-related functions.
"""

import struct

from .errors import ProtocolError


def metadata_to_buffers(metadata):
    """
    Transform a dict of metadata into a sequence of buffers.

    :param metadata: The metadata, as a dict.
    :returns: A list of buffers.
    """
    results = []

    for key, value in metadata.items():
        assert len(key) < 256
        assert len(value) < 2 ** 32
        results.extend([
            struct.pack('!B', len(key)),
            key,
            struct.pack('!I', len(value)),
            value,
        ])

    return results


def buffer_to_metadata(buffer):
    """
    Transform a buffer to a metadata dictionary.

    :param buffer: The buffer, as received in a READY command.
    :returns: A metadata dictionary, with its keys normalized (in
        lowercase).
    """
    offset = 0
    size = len(buffer)
    metadata = {}

    while offset < size:
        name_size = struct.unpack_from('B', buffer, offset)[0]
        offset += 1

        if name_size + 4 > size:
            raise ProtocolError(
                "Invalid name size in metadata",
                fatal=True,
            )

        name = buffer[offset:offset + name_size]
        offset += name_size

        value_size = struct.unpack_from('!I', buffer, offset)[0]
        offset += 4

        if value_size > size:
            raise ProtocolError(
                "Invalid value size in metadata",
                fatal=True,
            )

        value = buffer[offset:offset + value_size]
        offset += value_size
        metadata[name.lower()] = value

    return metadata
