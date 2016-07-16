"""
Common fixtures.
"""

import asyncio
import pytest
import sys

from io import BytesIO

from azmq.crypto import HAS_LIBSODIUM


@pytest.yield_fixture
def event_loop():
    if sys.platform == 'win32':
        loop = asyncio.ProactorEventLoop()
    else:
        loop = asyncio.SelectorEventLoop()

    asyncio.set_event_loop(loop)

    yield loop

    tasks = [task for task in asyncio.Task.all_tasks() if not task.done()]

    if tasks:
        loop.run_until_complete(asyncio.wait_for(asyncio.wait(tasks), 5))


ENDPOINTS = [
    'inproc://mypath',
    'tcp://127.0.0.1:3333',
]

if sys.platform == 'win32':
    ENDPOINTS.append(r'ipc:////./pipe/echo')
else:
    ENDPOINTS.append('ipc:///tmp/mypath')

use_all_transports = pytest.mark.parametrize('endpoint', ENDPOINTS)
"""
A decorator that causes the test scenario to run once with each of the support
transports.
"""

requires_libsodium = pytest.mark.skipif(
    not HAS_LIBSODIUM,
    reason="No libsodium support installed.",
)


@pytest.fixture
def reader():
    reader = BytesIO()

    async def readexactly(count):
        return reader.read(count)

    reader.readexactly = readexactly
    return reader


@pytest.fixture
def writer():
    writer = BytesIO()

    return writer
