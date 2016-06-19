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
        cls.write_command(
            writer=writer,
            name=b'READY',
            buffers=cls._metadata_to_buffers(metadata)
        )

    @classmethod
    async def _read_null_ready(cls, reader):
        raw_metadata = await cls._expect_command(
            reader=reader,
            name=b'READY',
        )
        return cls._buffer_to_metadata(buffer=raw_metadata)

    async def negotiate(self, writer, reader, metadata, address, zap_client):
        logger.debug("Negotiating NULL parameters.")

        self._write_null_ready(
            writer=writer,
            metadata=metadata,
        )
        remote_metadata = await self._read_null_ready(reader=reader)

        if zap_client:
            user_id, auth_metadata = await zap_client.authenticate(
                domain='',
                address=address,
                identity=remote_metadata.get(b'identity', b''),
                mechanism=self.name,
                credentials=[],
            )
        else:
            user_id, auth_metadata = None, None

        return remote_metadata, user_id, auth_metadata
