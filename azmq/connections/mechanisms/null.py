"""
The NULL mechanism.
"""

from ...log import logger

from .base import Mechanism


class Null(object):
    def __call__(self):
        return NullMechanism()


class NullMechanism(Mechanism):
    name = b'NULL'
    as_server = False

    @classmethod
    def _write_null_ready(cls, writer, metadata):
        """
        Write a NULL READY message.

        :param writer: The writer to use.
        :param metadata: The metadata dictionary.
        """
        cls.write_command(
            writer=writer,
            name=b'READY',
            buffers=cls._metadata_to_buffers(metadata)
        )

    @classmethod
    async def _read_null_ready(cls, reader):
        """
        Read a NULL READY message from the specified reader.

        :param reader: The reader to use.
        :returns: The peer's metadata dictionary.
        """
        raw_metadata = await cls._expect_command(
            reader=reader,
            name=b'READY',
        )
        return cls._buffer_to_metadata(buffer=raw_metadata)

    @classmethod
    async def negotiate(cls, writer, reader, metadata):
        logger.debug("Negotiating NULL parameters.")

        cls._write_null_ready(
            writer=writer,
            metadata=metadata,
        )
        return await cls._read_null_ready(reader=reader)
