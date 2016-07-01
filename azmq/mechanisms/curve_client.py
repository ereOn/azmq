"""
The CURVE server mechanism.
"""

import struct

from itertools import (
    chain,
    islice,
)

from ..log import logger
from ..errors import ProtocolError
from ..crypto import (
    crypto_box,
    crypto_box_afternm,
    crypto_box_beforenm,
    crypto_box_keypair,
    crypto_box_open,
    crypto_box_open_afternm,
    randombytes,
    requires_libsodium,
)

from .base import Mechanism


class CurveClient(object):
    @requires_libsodium
    def __init__(
        self,
        server_key,
        public_key=None,
        secret_key=None,
    ):
        self.set_permanent_server_key(server_key)

        if public_key or secret_key:
            self.set_permanent_keypair(
                public_key=public_key,
                secret_key=secret_key,
            )
        else:
            self.public_key = None
            self.secret_key = None

    def set_permanent_server_key(self, server_key):
        assert server_key and len(server_key) == 32, (
            "A 32 bytes server key must be specified."
        )
        self.server_key = server_key

    def set_permanent_keypair(self, public_key, secret_key):
        assert public_key and len(public_key) == 32, (
            "A 32 bytes public key must be specified."
        )
        assert secret_key and len(secret_key) == 32, (
            "A 32 bytes secret key must be specified."
        )
        self.public_key = public_key
        self.secret_key = secret_key

    def __call__(self):
        assert self.server_key, "No server key was specified."

        if not self.public_key:
            logger.info(
                "No keypair was specified for CURVE client. Generating a "
                "random one.",
            )
            self.public_key, self.secret_key = crypto_box_keypair()

        return CurveClientMechanism(
            server_key=self.server_key,
            public_key=self.public_key,
            secret_key=self.secret_key,
        )


class CurveClientMechanism(Mechanism):
    name = b'CURVE'
    as_server = False

    __slots__ = [
        'c',
        's',
        'rc',
        'cp',
        'sp',
        'rcp',
        'kp',
        'nonce',
        'rnonce',
    ]

    def __init__(self, server_key, public_key, secret_key):
        super().__init__()

        # Permanent keys.
        self.c = public_key  # The public permanent key.
        self.s = secret_key  # The secret permanent key.
        self.rc = server_key  # The remote public permanent key.

        # Transient keys.
        (
            self.cp,  # The public transient key.
            self.sp,  # The secret transient key.
        ) = crypto_box_keypair()
        self.rcp = None  # The remote public transient key.

        self.kp = None  # The precomputed transient key.

        # nonces
        self.nonce = 1
        self.rnonce = None

    def _write_curve_hello(self, writer):
        buffer = b'\1\0' + b'\0' * 72
        nonce_buffer = struct.pack('!Q', self.nonce)
        box = crypto_box(
            b'\0' * 64,
            nonce=b'CurveZMQHELLO---' + nonce_buffer,
            pk=self.rc,
            sk=self.sp,
        )

        self.write_command(
            writer=writer,
            name=b'HELLO',
            buffers=[buffer, self.cp, nonce_buffer, box],
        )

    async def _read_curve_welcome(self, reader):
        buffer = await self._expect_command(
            reader=reader,
            name=b'WELCOME',
        )

        if len(buffer) != 160:
            raise ProtocolError(
                "Invalid CURVE WELCOME message size (%s)" % len(buffer),
                fatal=True,
            )

        nonce = buffer[:16]
        box = buffer[16:]

        try:
            plain = crypto_box_open(
                box,
                nonce=b'WELCOME-' + nonce,
                pk=self.rc,
                sk=self.sp,
            )
        except ValueError:
            raise ProtocolError("Invalid CURVE WELCOME box.", fatal=True)

        remote_transient_public_key = plain[:32]
        cookie = plain[32:]

        return remote_transient_public_key, cookie

    def _write_curve_initiate(self, writer, cookie, metadata):
        vouch_nonce = randombytes(16)
        vouch_box = crypto_box(
            self.cp + self.rc,
            nonce=b'VOUCH---' + vouch_nonce,
            pk=self.rcp,
            sk=self.s,
        )
        plain = b''.join(chain(
            [
                self.c,
                vouch_nonce,
                vouch_box,
            ],
            self._metadata_to_buffers(metadata),
        ))
        nonce_buffer = struct.pack('!Q', self.nonce)
        box = crypto_box_afternm(
            plain,
            nonce=b'CurveZMQINITIATE' + nonce_buffer,
            k=self.kp,
        )

        self.write_command(
            writer=writer,
            name=b'INITIATE',
            buffers=[cookie, nonce_buffer, box],
        )

    async def _read_curve_ready(self, reader):
        buffer = await self._expect_command(
            reader=reader,
            name=b'READY',
        )
        nonce = buffer[:8]
        box = buffer[8:]
        plain = crypto_box_open_afternm(
            box,
            nonce=b'CurveZMQREADY---' + nonce,
            k=self.kp,
        )
        return self._buffer_to_metadata(plain)

    async def negotiate(self, writer, reader, metadata, address, zap_client):
        logger.debug("Negotiating CURVE parameters as client.")

        self._write_curve_hello(writer=writer)
        self.nonce += 1
        self.rcp, cookie = await self._read_curve_welcome(reader=reader)
        self.kp = crypto_box_beforenm(
            pk=self.rcp,
            sk=self.sp,
        )

        self._write_curve_initiate(
            writer=writer,
            cookie=cookie,
            metadata=metadata,
        )
        self.nonce += 1

        return await self._read_curve_ready(reader=reader), None, None

    def write(self, writer, frames):
        kp = self.kp

        def write_one(writer, frame, flag):
            nonce = struct.pack('!Q', self.nonce)
            box = crypto_box_afternm(
                flag + frame,
                nonce=b'CurveZMQMESSAGEC' + nonce,
                k=kp,
            )
            # Even though the RFC dictates those are commands, MESSAGE messages
            # are actually sent as frames in the original implementation so we
            # do the same.
            self._write_frame_last(
                writer,
                b'\7MESSAGE',
                nonce,
                box,
            )
            self.nonce += 1

        for frame in islice(frames, len(frames) - 1):
            write_one(writer, frame, b'\1')

        write_one(writer, frames[-1], b'\0')

    async def read(self, reader, on_command):
        frame, last = await super().read(reader=reader, on_command=on_command)
        nonce = frame[8:16]
        box = frame[16:]
        data = crypto_box_open_afternm(
            box,
            nonce=b'CurveZMQMESSAGES' + nonce,
            k=self.kp,
        )

        return data[1:], not data[0]
