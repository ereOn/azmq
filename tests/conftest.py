"""
Common fixtures.
"""

import asyncio
import pytest
import sys

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

    loop.close()


use_all_transports = pytest.mark.parametrize('endpoint', [
    'inproc://mypath',
    'tcp://127.0.0.1:3333',
])
"""
A decorator that causes the test scenario to run once with each of the support
transports.
"""

requires_libsodium = pytest.mark.skipif(
    not HAS_LIBSODIUM,
    reason="No libsodium support installed.",
)
