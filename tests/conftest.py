"""
Common fixtures.
"""

import asyncio
import pytest
import sys


@pytest.yield_fixture
def event_loop():
    if sys.platform == 'win32':
        loop = asyncio.ProactorEventLoop()
    else:
        loop = asyncio.SelectorEventLoop()

    asyncio.set_event_loop(loop)

    yield loop

    tasks = asyncio.Task.all_tasks()

    if tasks:
        loop.run_until_complete(asyncio.wait(tasks))
