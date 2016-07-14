"""
The CURVE server mechanism.
"""

import asyncio
import struct

from itertools import islice

from ..log import logger
from ..errors import ProtocolError
from ..crypto import (
    crypto_box,
    crypto_box_keypair,
    crypto_box_afternm,
    crypto_box_beforenm,
    crypto_box_open,
    crypto_box_open_afternm,
    crypto_secretbox,
    crypto_secretbox_open,
    randombytes,
    requires_libsodium,
)

from .base import Mechanism


class CurveServer(object):
    @requires_libsodium
    def __init__(self, public_key=None, secret_key=None):
        self.set_permanent_keypair(
            public_key=public_key,
            secret_key=secret_key,
        )

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
        return CurveServerMechanism(
            public_key=self.public_key,
            secret_key=self.secret_key,
        )


class CurveServerMechanism(Mechanism):
    name = b'CURVE'
    as_server = True

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

    def __init__(self, public_key, secret_key):
        super().__init__()

        # Permanent keys.
        self.c = public_key  # The public permanent key.
        self.s = secret_key  # The secret permanent key.
        self.rc = None  # The remote public permanent key.

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

    async def _read_curve_hello(self, reader):
        buffer = await self._expect_command(
            reader=reader,
            name=b'HELLO',
        )

        if len(buffer) != 194:
            raise ProtocolError(
                "Invalid CURVE HELLO message size (%s)" % len(buffer),
                fatal=True,
            )

        version = struct.unpack_from('BB', buffer)

        if version != (1, 0):
            raise ProtocolError(
                "Unsupported CURVE version %r." % (version,),
                fatal=True,
            )

        transient_key = buffer[74:106]  # 32 bytes.
        nonce = buffer[106:114]  # 8 bytes.
        box = buffer[114:]

        try:
            crypto_box_open(
                box,
                nonce=b'CurveZMQHELLO---' + nonce,
                pk=transient_key,
                sk=self.s,
            )
        except ValueError:
            raise ProtocolError("Invalid CURVE HELLO box.", fatal=True)

        return transient_key

    def _write_curve_welcome(self, writer):
        # Generate an unique, random nonce for the COOKIE box.
        cookie_nonce = randombytes(16)
        cookie_key = randombytes(32)
        cookie = cookie_nonce + crypto_secretbox(
            self.rcp + self.sp,
            nonce=b'COOKIE--' + cookie_nonce,
            k=cookie_key,
        )

        # Generate an unique, random nonce for the WELCOME box.
        nonce = randombytes(16)
        box = crypto_box(
            self.cp + cookie,
            nonce=b'WELCOME-' + nonce,
            pk=self.rcp,
            sk=self.s,
        )
        assert len(box) == 144

        self.write_command(
            writer=writer,
            name=b'WELCOME',
            buffers=[nonce, box],
        )

        return cookie_key

    async def _read_curve_initiate(
        self,
        reader,
        cookie_key,
    ):
        buffer = await self._expect_command(
            reader=reader,
            name=b'INITIATE',
        )

        if len(buffer) < 104:
            raise ProtocolError(
                "Invalid CURVE INITIATE message size (%s)" % len(buffer),
                fatal=True,
            )

        cookie_nonce = buffer[:16]
        cookie = buffer[16:96]
        nonce = buffer[96:104]
        box = buffer[104:]

        # Verify the cookie.
        try:
            cbuffer = crypto_secretbox_open(
                cookie,
                nonce=b'COOKIE--' + cookie_nonce,
                k=cookie_key,
            )
        except ValueError:
            raise ProtocolError("Invalid cookie.", fatal=True)

        if cbuffer[:32] != self.rcp:
            raise ProtocolError("Invalid cookie.", fatal=True)

        try:
            vbuffer = crypto_box_open_afternm(
                box,
                nonce=b'CurveZMQINITIATE' + nonce,
                k=self.kp,
            )
        except ValueError:
            raise ProtocolError("Invalid vouch.", fatal=True)

        if len(vbuffer) < 48:
            raise ProtocolError("Invalid vouch size.", fatal=True)

        self.rc = vbuffer[:32]
        vouch_nonce = vbuffer[32:48]
        vouch_box = vbuffer[48:128]
        raw_metadata = vbuffer[128:]

        try:
            plain_vouch = crypto_box_open(
                vouch_box,
                nonce=b'VOUCH---' + vouch_nonce,
                pk=self.rc,
                sk=self.sp,
            )
        except ValueError:
            raise ProtocolError("Invalid vouch.", fatal=True)

        if self.rcp + self.c != plain_vouch:
            raise ProtocolError(
                "Incorrect vouch. Aborting connection.",
                fatal=True,
            )

        return raw_metadata

    def _write_curve_ready(self, writer, metadata):
        nonce = struct.pack('!Q', self.nonce)
        box = crypto_box_afternm(
            b''.join(self._metadata_to_buffers(metadata)),
            nonce=b'CurveZMQREADY---' + nonce,
            k=self.kp,
        )
        self.write_command(
            writer=writer,
            name=b'READY',
            buffers=[nonce, box],
        )

    async def negotiate(self, writer, reader, metadata, address, zap_client):
        logger.debug("Negotiating CURVE parameters as server.")

        # Wait for a HELLO.
        self.rcp = await self._read_curve_hello(reader=reader)

        # Send back a WELCOME message. Return the generated cookie key.
        cookie_key = self._write_curve_welcome(writer=writer)

        self.kp = crypto_box_beforenm(
            pk=self.rcp,
            sk=self.sp,
        )

        try:
            raw_metadata = await asyncio.wait_for(
                self._read_curve_initiate(
                    reader=reader,
                    cookie_key=cookie_key,
                ),
                60,
            )
        except asyncio.TimeoutError:  # pragma: no cover
            raise ProtocolError(
                "Did not a receive an INITIATE command after 60 "
                "seconds. Aborting connection.",
            )

        remote_metadata = self._buffer_to_metadata(buffer=raw_metadata)

        if zap_client:
            user_id, auth_metadata = await zap_client.authenticate(
                domain='',
                address=address,
                identity=remote_metadata.get(b'identity', b''),
                mechanism=self.name,
                credentials=[
                    self.rc,  # The remote permanent public key.
                ],
            )
        else:
            user_id, auth_metadata = None, None

        self._write_curve_ready(
            writer=writer,
            metadata=metadata,
        )
        self.nonce += 1

        return remote_metadata, user_id, auth_metadata

    def write(self, writer, frames):
        kp = self.kp

        def write_one(writer, frame, flag):
            nonce = struct.pack('!Q', self.nonce)
            box = crypto_box_afternm(
                flag + frame,
                nonce=b'CurveZMQMESSAGES' + nonce,
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
            nonce=b'CurveZMQMESSAGEC' + nonce,
            k=self.kp,
        )

        return data[1:], not data[0]
