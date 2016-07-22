"""
Unit tests for containers.
"""

import asyncio
import pytest

from azmq.containers import AsyncList


async def assert_empty(l):
    await asyncio.wait_for(l.wait_empty(), 0)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(l.wait_not_empty(), 0)


async def assert_non_empty(l):
    await asyncio.wait_for(l.wait_not_empty(), 0)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(l.wait_empty(), 0)


def test_asynclist_list_likeness():
    al = AsyncList()
    al.append(4)
    al.extend([5, 6])
    assert al == [4, 5, 6]
    assert len(al) == 3
    assert al == AsyncList([4, 5, 6])


@pytest.mark.asyncio
async def test_asynclist_wait(event_loop):
    al = AsyncList()
    await assert_empty(al)

    al.append(5)
    await assert_non_empty(al)

    al.remove(5)
    await assert_empty(al)

    al.extend([4, 6])
    await assert_non_empty(al)

    al.remove(4)
    await assert_non_empty(al)

    al.clear()
    await assert_empty(al)

    al[:] = [1]
    await assert_non_empty(al)

    al[:] = []
    await assert_empty(al)

    al.insert(0, 4)
    await assert_non_empty(al)

    al.pop(0)
    await assert_empty(al)

    # call_soon() would run the call right away since we are in a coroutine.
    # This is not documented but it seems to happen consistently.
    event_loop.call_later(0, al.append, 2)
    await asyncio.wait_for(al.wait_change(), 0.5)


@pytest.mark.asyncio
async def test_fairlistproxy(event_loop):
    al = AsyncList()
    flp = al.create_proxy()
    assert not flp
    assert len(flp) == 0
    await assert_empty(flp)

    al.extend(['a', 'b', 'c', 'd'])
    assert flp
    assert len(flp) == len(al)
    await assert_non_empty(flp)

    assert list(flp) == ['a', 'b', 'c', 'd']
    assert list(flp) == ['b', 'c', 'd', 'a']
    al.remove('c')
    assert list(flp) == ['d', 'a', 'b']
    al.remove('d')
    assert list(flp) == ['a', 'b']
    al.remove('b')
    assert list(flp) == ['a']
    al.remove('a')
    assert list(flp) == []

    al.extend(['a', 'b', 'c'])
    assert next(iter(flp)) == 'a'
    assert next(iter(flp)) == 'b'
    assert next(iter(flp)) == 'c'
    assert next(iter(flp)) == 'a'
