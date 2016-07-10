"""
Unit tests for contexts.
"""

import pytest

from mock import MagicMock

from azmq.context import Context
from azmq.errors import InprocPathBound


def test_set_zap_authenticator(event_loop):
    context = Context(loop=event_loop)
    zap_authenticator = MagicMock()
    context.set_zap_authenticator(zap_authenticator)
    assert context._zap_authenticator is zap_authenticator
    result = context.set_zap_authenticator(None)
    assert result is zap_authenticator
    assert context._zap_authenticator is None


@pytest.mark.asyncio
async def test_start_inproc_server_twice(event_loop):
    context = Context(loop=event_loop)
    handler = MagicMock()
    await context._start_inproc_server(handler, 'boo')

    with pytest.raises(InprocPathBound) as error:
        await context._start_inproc_server(handler, 'boo')

    assert error.value.path == 'boo'
