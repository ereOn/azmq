"""
Message parsing/producing utilities.
"""

import struct

from io import BytesIO

from .errors import ZMTPFrameInvalid


SIGNATURE = b'\xff\x00\x00\x00\x00\x00\x00\x00\x00\x7f'
FILLER = b'\x00' * 31


def write_first_greeting(buffer, major_version):
    """
    Write the first greeting to the specified buffer.

    :param buffer: The buffer to write to.
    :param minor_version: The minor version.
    """
    buffer.write(SIGNATURE)
    buffer.write(bytes([major_version]))


def write_second_greeting(buffer, minor_version, mechanism, as_server):
    """
    Write a complete greeting to the specified buffer.

    :param buffer: The buffer to write to.
    :param minor_version: The minor version.
    :param mechanism: A mechanism string. Will be UTF-8 encoded and
        truncated to 20 bytes.
    :param as_server: A flag that indicates whether this client is acting as a
        server or a client for the current session.
    """
    buffer.write(bytes([minor_version]))
    buffer.write(mechanism[:20].ljust(20, b'\x00'))
    buffer.write(b'\x01' if as_server else b'\x00')
    buffer.write(FILLER)


def write_command(buffer, name, data):
    """
    Write a command to the specified buffer.

    :param buffer: The buffer to write to.
    :param name: The command name.
    :param data: The command data.
    """
    assert len(name) < 256

    body_len = len(name) + 1 + len(data)

    if body_len < 256:
        buffer.write(struct.pack('BBB', 0x04, body_len, len(name)))
    else:
        buffer.write(struct.pack('BQB', 0x06, body_len, len(name)))

    buffer.write(name)
    buffer.write(data)


async def read_first_greeting(buffer, major_version):
    """
    Read the first greeting from the specified buffer.

    :param buffer: The buffer to read from.
    :param major_version: The minimal version to support.
    """
    data = await buffer.readexactly(len(SIGNATURE))

    if data[0] != SIGNATURE[0]:
        raise ZMTPFrameInvalid("Invalid signature")

    if data[-1] != SIGNATURE[-1]:
        raise ZMTPFrameInvalid("Invalid signature")

    version = struct.unpack('B', await buffer.readexactly(1))[0]

    if version < major_version:
        raise ZMTPFrameInvalid("Unsupported major version")


async def read_second_greeting(buffer):
    """
    Read the second greeting from the specified buffer.

    :param buffer: The buffer to read from.
    :return: The minor version, mechanism and as-server flag, as a tuple.
    """
    version = struct.unpack('B', await buffer.readexactly(1))[0]
    mechanism = (await buffer.readexactly(20)).rstrip(b'\x00')
    as_server = bool(struct.unpack('B', await buffer.readexactly(1))[0])
    await buffer.readexactly(len(FILLER))

    return version, mechanism, as_server


async def read_command(buffer):
    """
    Read a command.

    :param buffer: The buffer to read from.
    :returns: The command name and data.
    """
    command_size_type = struct.unpack('B', await buffer.readexactly(1))[0]

    if command_size_type == 0x04:
        command_size = struct.unpack('B', await buffer.readexactly(1))[0]
    elif command_size_type == 0x06:
        command_size = struct.unpack('!Q', await buffer.readexactly(8))[0]
    else:
        raise ZMTPFrameInvalid(
            "Unexpected command size type: %0x" % command_size_type,
        )

    command_name_size = struct.unpack('B', await buffer.readexactly(1))[0]
    command_name = await buffer.readexactly(command_name_size)
    command_data = await buffer.readexactly(command_size - command_name_size)
    return command_name, command_data


def dump_ready_command(values):
    """
    Dump a ready command message.

    :param values: A dictionary of values to dump.
    """
    result = BytesIO()

    for key, value in values.items():
        assert len(key) < 256
        assert len(value) < 2 ** 32
        result.write(struct.pack('B', len(key)))
        result.write(key)
        result.write(struct.pack('!I', len(value)))
        result.write(value)

    return result.getvalue()

def load_ready_command(data):
    """
    Load a ready command.

    :param data: The data, as received in a READY command.
    :returns: A dictionary of received values.
    """
    offset = 0
    size = len(data)
    result = {}

    while offset < size:
        name_size = struct.unpack('B', data[offset:offset + 1])[0]
        offset += 1

        if name_size + 4 > size:
            raise ZMTPFrameInvalid("Invalid name size in READY command")

        name = data[offset:offset + name_size]
        offset += name_size

        value_size = struct.unpack('!I', data[offset:offset + 4])[0]
        offset += 4

        if value_size > size:
            raise ZMTPFrameInvalid("Invalid value size in READY command")

        value = data[offset:offset + value_size]
        offset += value_size
        result[name] = value

    return result
