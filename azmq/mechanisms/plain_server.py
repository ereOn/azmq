"""
The PLAIN server mechanism.
"""

from ..log import logger

from .base import Mechanism


class PlainServer(object):
    def __call__(self):
        return PlainServerMechanism()


class PlainServerMechanism(Mechanism):
    name = b'PLAIN'
    as_server = True

    @classmethod
    async def _read_plain_hello(cls, reader):
        buffer = await cls._expect_command(
            reader=reader,
            name=b'HELLO',
        )
        username_len = buffer[0]
        username = buffer[1:username_len + 1]
        password_len = buffer[username_len + 1]
        password = buffer[username_len + 2:username_len + 2 + password_len]

        return username, password

    @classmethod
    async def _read_plain_initiate(cls, reader):
        raw_metadata = await cls._expect_command(
            reader=reader,
            name=b'INITIATE',
        )
        return cls._buffer_to_metadata(buffer=raw_metadata)

    @classmethod
    def _write_plain_ready(cls, writer, metadata):
        cls.write_command(
            writer=writer,
            name=b'READY',
            buffers=cls._metadata_to_buffers(metadata)
        )

    async def negotiate(self, writer, reader, metadata, address, zap_client):
        logger.debug("Negotiating PLAIN parameters as server.")

        # Wait for a HELLO.
        username, password = await self._read_plain_hello(reader=reader)

        self.write_command(writer=writer, name=b'WELCOME')

        remote_metadata = await self._read_plain_initiate(reader=reader)

        if zap_client:
            user_id, auth_metadata = await zap_client.authenticate(
                domain='',
                address=address,
                identity=remote_metadata.get(b'identity', b''),
                mechanism=self.name,
                credentials=[
                    username,
                    password,
                ],
            )
        else:
            user_id, auth_metadata = None, None

        self._write_plain_ready(writer=writer, metadata=metadata)

        return remote_metadata, user_id, auth_metadata
