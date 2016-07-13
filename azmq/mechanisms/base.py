"""
Base class for mechanisms.
"""

import struct

from itertools import islice

from ..errors import ProtocolError
from ..metadata import (
    buffer_to_metadata,
    metadata_to_buffers,
)


class Mechanism(object):
    """
    Base class for mechanisms instances.

    Provides helper method to read and write from streams.
    """
    name = None
    as_server = False

    @classmethod
    def _metadata_to_buffers(cls, metadata):
        return metadata_to_buffers(metadata)

    @classmethod
    def _buffer_to_metadata(cls, buffer):
        return buffer_to_metadata(buffer)

    @classmethod
    def write_command(cls, writer, name, buffers=()):
        """
        Write a command to the specified writer.

        :param writer: The writer to use.
        :param name: The command name.
        :param buffers: The buffers to writer.
        """
        assert len(name) < 256

        body_len = len(name) + 1 + sum(len(buffer) for buffer in buffers)

        if body_len < 256:
            writer.write(struct.pack('!BBB', 0x04, body_len, len(name)))
        else:
            writer.write(struct.pack('!BQB', 0x06, body_len, len(name)))

        writer.write(name)

        for buffer in buffers:
            writer.write(buffer)

    @classmethod
    async def _expect_command(cls, reader, name):
        """
        Expect a command.

        :param reader: The reader to use.
        :returns: The command data.
        """
        size_type = struct.unpack('B', await reader.readexactly(1))[0]

        if size_type == 0x04:
            size = struct.unpack('!B', await reader.readexactly(1))[0]
        elif size_type == 0x06:
            size = struct.unpack('!Q', await reader.readexactly(8))[0]
        else:
            raise ProtocolError(
                "Unexpected size type: %0x" % size_type,
                fatal=True,
            )

        name_size = struct.unpack('B', await reader.readexactly(1))[0]

        if name_size != len(name):
            raise ProtocolError(
                "Unexpected command name size: %s (expecting %s)" % (
                    name_size,
                    len(name),
                ),
                fatal=True,
            )

        c_name = await reader.readexactly(name_size)

        if c_name != name:
            raise ProtocolError(
                "Unexpected command name: %s (expecting %s)" % (c_name, name),
                fatal=True,
            )

        return await reader.readexactly(size - name_size - 1)

    @staticmethod
    def _write_frame_more(writer, *buffers):
        body_len = sum(map(len, buffers))

        if body_len < 256:
            writer.write(struct.pack('!BB', 0x01, body_len))
        else:
            writer.write(struct.pack('!BQ', 0x03, body_len))

        for b in buffers:
            writer.write(b)

    @staticmethod
    def _write_frame_last(writer, *buffers):
        body_len = sum(map(len, buffers))

        if body_len < 256:
            writer.write(struct.pack('!BB', 0x00, body_len))
        else:
            writer.write(struct.pack('!BQ', 0x02, body_len))

        for b in buffers:
            writer.write(b)

    @classmethod
    def write(cls, writer, frames):
        for frame in islice(frames, len(frames) - 1):
            cls._write_frame_more(writer, frame)

        cls._write_frame_last(writer, frames[-1])

    @staticmethod
    async def read(reader, on_command):
        read = reader.readexactly

        while True:
            size_type = struct.unpack('B', await read(1))[0]

            if size_type in {0x00, 0x01, 0x04}:
                size = struct.unpack('!B', await read(1))[0]
            elif size_type in {0x02, 0x03, 0x06}:
                size = struct.unpack('!Q', await read(8))[0]
            else:
                raise ProtocolError(
                    "Unexpected traffic size type: %0x" % size_type,
                )

            if size_type in {0x00, 0x01, 0x02, 0x03}:
                frame = await reader.readexactly(size)
                last = size_type in {0x00, 0x02}

                return frame, last
            else:
                name_size = struct.unpack('B', await read(1))[0]
                name = await read(name_size)
                data = await read(size - name_size - 1)

                await on_command(name, data)
