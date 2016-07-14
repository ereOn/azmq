"""
Unit tests for the curve server mechanism class.
"""

import pytest

from azmq.crypto import (
    crypto_box,
    crypto_box_afternm,
    crypto_box_beforenm,
    crypto_secretbox,
    curve_gen_keypair,
)
from azmq.mechanisms.curve_server import CurveServer
from azmq.errors import ProtocolError


@pytest.fixture
def mechanism():
    public_key, secret_key = curve_gen_keypair()
    mechanism = CurveServer(
        public_key=public_key,
        secret_key=secret_key,
    )()
    mechanism.rc, mechanism.rs = curve_gen_keypair()
    mechanism.rcp, mechanism.rsp = curve_gen_keypair()
    mechanism.kp = crypto_box_beforenm(
        pk=mechanism.rcp,
        sk=mechanism.sp,
    )
    mechanism.rkp = crypto_box_beforenm(
        pk=mechanism.cp,
        sk=mechanism.rsp,
    )
    return mechanism


@pytest.mark.asyncio
async def test_read_curve_hello_invalid(reader, mechanism):
    reader.write(b'\x04\x06\x05HELLO')
    reader.seek(0)

    with pytest.raises(ProtocolError):
        await mechanism._read_curve_hello(reader=reader)


@pytest.mark.asyncio
async def test_read_curve_hello_invalid_version(reader, mechanism):
    reader.write(b'\x04\xc8\x05HELLO' + b'\0' * 194)
    reader.seek(0)

    with pytest.raises(ProtocolError):
        await mechanism._read_curve_hello(reader=reader)


@pytest.mark.asyncio
async def test_read_curve_hello_invalid_box(reader, mechanism):
    reader.write(b'\x04\xc8\x05HELLO\1\0' + b'\0' * 192)
    reader.seek(0)

    with pytest.raises(ProtocolError):
        await mechanism._read_curve_hello(reader=reader)


@pytest.mark.asyncio
async def test_read_curve_initiate_invalid(reader, mechanism):
    reader.write(b'\x04\x09\x08INITIATE')
    reader.seek(0)

    with pytest.raises(ProtocolError):
        await mechanism._read_curve_initiate(
            reader=reader,
            cookie_key=b'\0' * 32,
        )


@pytest.mark.asyncio
async def test_read_curve_initiate_invalid_cookie(reader, mechanism):
    reader.write(b'\x04\x71\x08INITIATE' + b'\0' * 104)
    reader.seek(0)

    with pytest.raises(ProtocolError):
        await mechanism._read_curve_initiate(
            reader=reader,
            cookie_key=b'\0' * 32,
        )


@pytest.mark.asyncio
async def test_read_curve_initiate_mismatching_cookie(reader, mechanism):
    cookie_nonce = b'\0' * 16
    cookie_key = b'\0' * 32
    cookie = crypto_secretbox(
        b'\0' * 32 + mechanism.sp,
        nonce=b'COOKIE--' + cookie_nonce,
        k=cookie_key,
    )
    nonce = b'\0' * 8
    reader.write(b'\x04\x71\x08INITIATE' + cookie_nonce + cookie + nonce)
    reader.seek(0)

    with pytest.raises(ProtocolError):
        await mechanism._read_curve_initiate(
            reader=reader,
            cookie_key=cookie_key,
        )


@pytest.mark.asyncio
async def test_read_curve_initiate_invalid_vouch_box(reader, mechanism):
    cookie_nonce = b'\0' * 16
    cookie_key = b'\0' * 32
    cookie = crypto_secretbox(
        mechanism.rcp + mechanism.sp,
        nonce=b'COOKIE--' + cookie_nonce,
        k=cookie_key,
    )
    nonce = b'\0' * 8
    reader.write(b'\x04\x71\x08INITIATE' + cookie_nonce + cookie + nonce)
    reader.seek(0)

    with pytest.raises(ProtocolError):
        await mechanism._read_curve_initiate(
            reader=reader,
            cookie_key=cookie_key,
        )


@pytest.mark.asyncio
async def test_read_curve_initiate_invalid_vouch_size(reader, mechanism):
    cookie_nonce = b'\0' * 16
    cookie_key = b'\0' * 32
    cookie = crypto_secretbox(
        mechanism.rcp + mechanism.sp,
        nonce=b'COOKIE--' + cookie_nonce,
        k=cookie_key,
    )
    nonce = b'\0' * 8
    plain = b'\0' * 47
    box = crypto_box_afternm(
        plain,
        nonce=b'CurveZMQINITIATE' + nonce,
        k=mechanism.rkp,
    )
    reader.write(b'\x04\xb1\x08INITIATE' + cookie_nonce + cookie + nonce + box)
    reader.seek(0)

    with pytest.raises(ProtocolError):
        await mechanism._read_curve_initiate(
            reader=reader,
            cookie_key=cookie_key,
        )


@pytest.mark.asyncio
async def test_read_curve_initiate_invalid_vouch(reader, mechanism):
    cookie_nonce = b'\0' * 16
    cookie_key = b'\0' * 32
    cookie = crypto_secretbox(
        mechanism.rcp + mechanism.sp,
        nonce=b'COOKIE--' + cookie_nonce,
        k=cookie_key,
    )
    nonce = b'\0' * 8
    plain = b'\0' * 48
    box = crypto_box_afternm(
        plain,
        nonce=b'CurveZMQINITIATE' + nonce,
        k=mechanism.rkp,
    )
    reader.write(b'\x04\xb1\x08INITIATE' + cookie_nonce + cookie + nonce + box)
    reader.seek(0)

    with pytest.raises(ProtocolError):
        await mechanism._read_curve_initiate(
            reader=reader,
            cookie_key=cookie_key,
        )


@pytest.mark.asyncio
async def test_read_curve_initiate_mismatching_vouch(reader, mechanism):
    cookie_nonce = b'\0' * 16
    cookie_key = b'\0' * 32
    cookie = crypto_secretbox(
        mechanism.rcp + mechanism.sp,
        nonce=b'COOKIE--' + cookie_nonce,
        k=cookie_key,
    )
    vouch_nonce = b'\0' * 16
    vouch_box = crypto_box(
        b'0' * 64,
        # mechanism.rcp + mechanism.c,
        nonce=b'VOUCH---' + vouch_nonce,
        pk=mechanism.cp,
        sk=mechanism.rs,
    )
    plain = mechanism.rc + vouch_nonce + vouch_box
    nonce = b'\0' * 8
    box = crypto_box_afternm(
        plain,
        nonce=b'CurveZMQINITIATE' + nonce,
        k=mechanism.rkp,
    )
    reader.write(
        b'\x06\x00\x00\x00\x00\x00\x00\xff\x02\x08INITIATE' + cookie_nonce +
        cookie + nonce + box,
    )
    reader.seek(0)

    with pytest.raises(ProtocolError):
        await mechanism._read_curve_initiate(
            reader=reader,
            cookie_key=cookie_key,
        )
