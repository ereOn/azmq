"""
Unit tests for the base mechanism class.
"""

import pytest

from azmq.mechanisms.base import Mechanism
from azmq.errors import ProtocolError


@pytest.mark.asyncio
async def test_expect_command(reader):
    reader.write(b'\x04\x09\x03FOOhello')
    reader.seek(0)

    result = await Mechanism._expect_command(reader=reader, name=b'FOO')
    assert result == b'hello'


@pytest.mark.asyncio
async def test_expect_command_large(reader):
    reader.write(b'\x06\x00\x00\x00\x00\x00\x00\x00\x09\x03FOOhello')
    reader.seek(0)

    result = await Mechanism._expect_command(reader=reader, name=b'FOO')
    assert result == b'hello'


@pytest.mark.asyncio
async def test_expect_command_invalid_size_type(reader):
    reader.write(b'\x03')
    reader.seek(0)

    with pytest.raises(ProtocolError):
        await Mechanism._expect_command(reader=reader, name=b'FOO')


@pytest.mark.asyncio
async def test_expect_command_invalid_name_size(reader):
    reader.write(b'\x04\x09\x04HELOhello')
    reader.seek(0)

    with pytest.raises(ProtocolError):
        await Mechanism._expect_command(reader=reader, name=b'FOO')


@pytest.mark.asyncio
async def test_expect_command_invalid_name(reader):
    reader.write(b'\x04\x08\x03BARhello')
    reader.seek(0)

    with pytest.raises(ProtocolError):
        await Mechanism._expect_command(reader=reader, name=b'FOO')


@pytest.mark.asyncio
async def test_read_frame(reader):
    reader.write(b'\x00\x03foo')
    reader.seek(0)

    async def on_command(name, data):
        assert False

    result = await Mechanism.read(reader=reader, on_command=on_command)
    assert result == (b'foo', True)


@pytest.mark.asyncio
async def test_read_frame_large(reader):
    reader.write(b'\x02\x00\x00\x00\x00\x00\x00\x00\x03foo')
    reader.seek(0)

    async def on_command(name, data):
        assert False

    result = await Mechanism.read(reader=reader, on_command=on_command)
    assert result == (b'foo', True)


@pytest.mark.asyncio
async def test_read_command(reader):
    reader.write(b'\x04\x09\x03BARhello\x00\x03foo')
    reader.seek(0)

    async def on_command(name, data):
        assert name == b'BAR'
        assert data == b'hello'

    result = await Mechanism.read(reader=reader, on_command=on_command)
    assert result == (b'foo', True)


@pytest.mark.asyncio
async def test_read_invalid_size_type(reader):
    reader.write(b'\x09')
    reader.seek(0)

    async def on_command(name, data):
        assert False

    with pytest.raises(ProtocolError):
        await Mechanism.read(reader=reader, on_command=on_command)
