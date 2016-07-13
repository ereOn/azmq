"""
Unit tests for inproc transport.
"""

from azmq.transports.inproc import Channel


def test_channel_repr():
    channel = Channel(path='foo')
    assert repr(channel) == "Channel(path='foo')"
