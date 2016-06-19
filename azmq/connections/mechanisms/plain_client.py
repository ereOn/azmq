"""
The PLAIN client mechanism.
"""

import struct

from ...log import logger

from .base import Mechanism


class PlainClient(object):
    def __init__(self, username, password):
        assert len(username) < 256, "username can't exceed 255 bytes"
        assert len(password) < 256, "password can't exceed 255 bytes"

        self.username = username
        self.password = password

    def __call__(self):
        return PlainClientMechanism(
            username=self.username,
            password=self.password,
        )


class PlainClientMechanism(Mechanism):
    name = b'PLAIN'
    as_server = False

    __slots__ = [
        'username',
        'password',
    ]

    def __init__(self, username, password):
        self.username = username
        self.password = password

    def _write_plain_hello(self, writer):
        self.write_command(
            writer=writer,
            name=b'HELLO',
            buffers=[
                struct.pack('B', len(self.username)),
                self.username,
                struct.pack('B', len(self.password)),
                self.password,
            ],
        )

    @classmethod
    def _write_plain_initiate(cls, writer, metadata):
        cls.write_command(
            writer=writer,
            name=b'INITIATE',
            buffers=cls._metadata_to_buffers(metadata)
        )

    @classmethod
    async def _read_plain_ready(cls, reader):
        raw_metadata = await cls._expect_command(
            reader=reader,
            name=b'READY',
        )
        return cls._buffer_to_metadata(buffer=raw_metadata)

    async def negotiate(self, writer, reader, metadata):
        logger.debug("Negotiating PLAIN parameters as client.")

        self._write_plain_hello(writer=writer)
        await self._expect_command(reader=reader, name=b'WELCOME')
        self._write_plain_initiate(writer=writer, metadata=metadata)
        return await self._read_plain_ready(reader=reader)
