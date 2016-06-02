"""
Message parsing/producing utilities.
"""

import struct

from io import BytesIO
from itertools import islice

from .errors import ProtocolError


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


def write_command(buffer, name, *datas):
    """
    Write a command to the specified buffer.

    :param buffer: The buffer to write to.
    :param name: The command name.
    :param data: The command data.
    """
    assert len(name) < 256

    body_len = len(name) + 1 + sum(len(data) for data in datas)

    if body_len < 256:
        buffer.write(struct.pack('!BBB', 0x04, body_len, len(name)))
    else:
        buffer.write(struct.pack('!BQB', 0x06, body_len, len(name)))

    buffer.write(name)

    for data in datas:
        buffer.write(data)


def write_ping_command(buffer, ttl, context):
    """
    Write a ping to the specified buffer.

    :param buffer: The buffer to write to.
    :param ttl: The ping time to live, in seconds.
    :param context: The ping context, as arbitrary bytes.
    """
    write_command(buffer, b'PING', struct.pack('!H', int(ttl * 10)), context)


def write_pong_command(buffer, context):
    """
    Write a pong to the specified buffer.

    :param buffer: The buffer to write to.
    :param context: The ping context, as arbitrary bytes.
    """
    write_command(buffer, b'PONG', context)


def write_frame_more(buffer, frame):
    """
    Write a frame that will be followed by other frames.

    :param buffer: The buffer to write to.
    :param frame: The frame to write.
    """
    body_len = len(frame)

    if body_len < 256:
        buffer.write(struct.pack('!BB', 0x01, body_len))
    else:
        buffer.write(struct.pack('!BQ', 0x03, body_len))

    buffer.write(bytes(frame))


def write_frame_last(buffer, frame):
    """
    Write the last frame of a sequence.

    :param buffer: The buffer to write to.
    :param frame: The frame to write.
    """
    body_len = len(frame)

    if body_len < 256:
        buffer.write(struct.pack('!BB', 0x00, body_len))
    else:
        buffer.write(struct.pack('!BQ', 0x02, body_len))

    buffer.write(bytes(frame))


def write_frames(buffer, frames):
    """
    Write a sequence of frames.

    :param buffer: The buffer to write to.
    :param frame: The frames to write. Cannot be empty.
    """
    assert frames

    for frame in islice(frames, len(frames) - 1):
        write_frame_more(buffer, frame)

    write_frame_last(buffer, frames[-1])


async def read_first_greeting(buffer):
    """
    Read the first greeting from the specified buffer.

    :param buffer: The buffer to read from.
    :returns: The major version.
    """
    data = await buffer.readexactly(len(SIGNATURE))

    if data[0] != SIGNATURE[0]:
        raise ProtocolError("Invalid signature")

    if data[-1] != SIGNATURE[-1]:
        raise ProtocolError("Invalid signature")

    return struct.unpack('B', await buffer.readexactly(1))[0]


async def read_second_greeting(buffer):
    """
    Read the second greeting from the specified buffer.

    :param buffer: The buffer to read from.
    :returns: The minor version, mechanism and as-server flag, as a tuple.
    """
    version = struct.unpack('B', await buffer.readexactly(1))[0]
    mechanism = (await buffer.readexactly(20)).rstrip(b'\x00')
    as_server = bool(struct.unpack('B', await buffer.readexactly(1))[0])
    await buffer.readexactly(len(FILLER))

    return version, mechanism, as_server


class Command(object):
    __slots__ = [
        'name',
        'data',
    ]

    def __init__(self, name, data):
        self.name = name
        self.data = data

    def __bytes__(self):
        return self.data

    def __len__(self):
        return len(self.data)


class Frame(object):
    __slots__ = [
        'body',
        'last',
    ]

    def __init__(self, body, last):
        self.body = body
        self.last = last

    def __hash__(self):
        return hash(self.body)

    def __repr__(self):
        return repr(self.body)

    def __bytes__(self):
        return self.body

    def __len__(self):
        return len(self.body)

    def __eq__(self, other):
        if isinstance(other, Frame):
            return self.body == other.body and self.last == other.last
        elif isinstance(other, bytes):
            return self.body == other

        return NotImplemented

    def __ne__(self, other):
        return not self == other


async def read_traffic(buffer):
    """
    Read a command or a frame.

    :param buffer: The buffer to read from.
    :returns: The command or frame.
    """
    traffic_size_type = struct.unpack('B', await buffer.readexactly(1))[0]

    if traffic_size_type in {0x00, 0x01, 0x04}:
        traffic_size = struct.unpack('!B', await buffer.readexactly(1))[0]
    elif traffic_size_type in {0x02, 0x03, 0x06}:
        traffic_size = struct.unpack('!Q', await buffer.readexactly(8))[0]
    else:
        raise ProtocolError(
            "Unexpected traffic size type: %0x" % traffic_size_type,
        )

    if traffic_size_type in {0x00, 0x01, 0x02, 0x03}:
        frame_body = await buffer.readexactly(traffic_size)
        frame_last = traffic_size_type in {0x00, 0x02}

        return Frame(body=frame_body, last=frame_last)
    else:
        command_name_size = struct.unpack('B', await buffer.readexactly(1))[0]
        command_name = await buffer.readexactly(command_name_size)
        command_data = await buffer.readexactly(
            traffic_size - command_name_size - 1,
        )

        return Command(name=command_name, data=command_data)


async def read_command(buffer):
    """
    Read a command.

    :param buffer: The buffer to read from.
    :returns: The command.
    """
    command = await read_traffic(buffer)

    if not isinstance(command, Command):
        raise ProtocolError(
            "Expected a command but got a frame instead (%r)" % command
        )

    return command


def dump_ready_command(values):
    """
    Dump a ready command message.

    :param values: A dictionary of values to dump.
    """
    result = BytesIO()

    for key, value in values.items():
        assert len(key) < 256
        assert len(value) < 2 ** 32
        result.write(struct.pack('!B', len(key)))
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
            raise ProtocolError("Invalid name size in READY command")

        name = data[offset:offset + name_size]
        offset += name_size

        value_size = struct.unpack('!I', data[offset:offset + 4])[0]
        offset += 4

        if value_size > size:
            raise ProtocolError("Invalid value size in READY command")

        value = data[offset:offset + value_size]
        offset += value_size
        result[name.lower()] = value

    return result


def load_ping_command(data):
    """
    Load a ping command.

    :param data: The data, as received in a PING command.
    :returns: A tuple (ttl, ping context).
    """
    ttl = struct.unpack('!H', data[:2])[0]
    ping_context = data[2:]

    return ttl, ping_context


def load_pong_command(data):
    """
    Load a pong command.

    :param data: The data, as received in a PONG command.
    :returns: The ping context.
    """
    return data
