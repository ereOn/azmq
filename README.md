[![Build Status](https://travis-ci.org/ereOn/azmq.svg?branch=master)](https://travis-ci.org/ereOn/azmq)
[![Coverage Status](https://coveralls.io/repos/ereOn/azmq/badge.svg?branch=master&service=github)](https://coveralls.io/github/ereOn/azmq?branch=master)
[![Documentation Status](https://readthedocs.org/projects/azmq/badge/?version=latest)](http://azmq.readthedocs.org/en/latest/?badge=latest)
[![PyPI](https://img.shields.io/pypi/pyversions/azmq.svg)](https://pypi.python.org/pypi/azmq/1.0.0)
[![PyPi version](https://img.shields.io/pypi/v/azmq.svg)](https://pypi.python.org/pypi/azmq/1.0.0)
[![PyPi downloads](https://img.shields.io/pypi/dm/azmq.svg)](https://pypi.python.org/pypi/azmq/1.0.0)

# AZMQ

**AZMQ** is a Python 3 asyncio-native implementation of
[ZMTP](http://rfc.zeromq.org/spec:37) (the protocol behind ZMQ).

## Motivation

None of the existing Python ZMQ implementation currently implements a fully
functional asyncio-compatible version that works well on all platforms. This is
especially true on Windows for which the few existing implementations are
seriously limited in performance or functionality, and some require the use of
a specific event-loop, which prevents using the default, standard ones.

**AZMQ**'s goal is to lift those restrictions and to work the same on all
platforms that Python 3.5 supports by providing a pure asyncio-native
implementation. Windows is no second-class citizen when it comes to **AZMQ**.

## Example

Here is a short example of AZMQ's usage:

```python
import asyncio
import sys

from azmq import (
    Context,
    REQ,
    REP,
)

async def run():
    async with Context() as context:
        req = context.socket(REQ)
        rep = context.socket(REP)

        rep.bind('tcp://0.0.0.0:3333')
        req.connect('tcp://127.0.0.1:3333')

        await req.send_multipart([b'hello'])
        msg = await rep.recv_multipart()
        assert msg == [b'hello']
        await rep.send_multipart([b'world'])
        msg = await req.recv_multipart()
        assert msg == [b'world']


if __name__ == '__main__':
    if sys.platform == 'win32':
        loop = asyncio.ProactorEventLoop()
    else:
        loop = asyncio.SelectorEventLoop()

    asyncio.set_event_loop(loop)

    loop.run_until_complete(run())
```

## API

**AZMQ** implements the following things:

- All socket types:
  * [x] REQ
  * [x] REP
  * [x] DEALER
  * [x] ROUTER
  * [x] PUB
  * [x] XPUB
  * [x] SUB
  * [x] XSUB
  * [x] PUSH
  * [x] PULL
  * [x] PAIR

- Those transports:
  * [x] TCP client
  * [x] TCP server
  * [x] Inproc
  * [x] IPC sockets (UNIX sockets and named pipes)

- Those mechanisms:
  * [x] NULL
  * [x] PLAIN
  * [x] CURVE
  * [x] ZAP

It is worth noting that, for once, IPC sockets are implemented on **all
platforms**, including *Windows*. This is done through named pipes and is very
convenient for portability.

Both TCP and IPC transports (on UNIX) are compatible with the legacy `zmq`
implementation.

Please refer to the documentation for details.

## Current state and goals

The API tries to be close to the one of pyzmq, but not too close. Here is an
**non-exhaustive** list of some differences in the APIs:

- **AZMQ** methods never take a `timeout` parameter. In the asyncio world, you
  just use
  [`asyncio.wait_for()`](https://docs.python.org/3/library/asyncio-task.html#asyncio.wait_for)
  for that purpose. All asynchronous methods are cancellable at anytime.
- There is no
  [`Poller`](http://learning-0mq-with-pyzmq.readthedocs.io/en/latest/pyzmq/multisocket/zmqpoller.html)
  class. The asyncio event-loop already gives you everything you need in terms
  of "polling" several ZMQ sockets at the same time. Actually, it's better
  because you are not limited to ZMQ sockets.

## Performances

**AZMQ**'s first release has been tested on all major platforms and
benchmarked, following the recommendations at
[zeromq.org](http://zeromq.org/whitepapers:measuring-performance).

Performance tests show that **AZMQ** is (unsurprisingly) slower than pyzmq for
this specific benchmark. It's hard to compete with a very-well optimized C
implementation, especially when it comes to networking. Also, and very
importantly, AZMQ is by design single-threaded (asyncio) while pyzmq can use
threads to parallelize work, thus furthering even more the difference.

AZMQ is capable of sending thousands of messages/second while it's C
counterpart can do much, much more. The main bottleneck seems to be the
event-loop overhead, as the same throughput is computed for various transports
(TCP/localhost, IPC, TCP/LAN, ...) and this throughput varies mainly from one
computer to another.

That being said, not every application requires top-notch performance and the
ability to send millions of messages/second. As the bottleneck is the
event-loop/CPU, you can always scale-up by starting multiple processes (is
there any other effective way to scale-up in Python anyway ?).

In the worst case where you actually need that kind of performance in some
parts of your system, ZMQ high-interoperability makes it easy to use any ZMQ
implementation that suits your needs. You can then use AZMQ and its friendly
user-interface everywhere else you want to: it'll just work nicely.

If you want to replicate, analyse the benchmark results, you may find the code
and instructions in the `benchmark` folder.

## Installation

Just do:

```
pip install azmq
```

This will install AZMQ **without CURVE support**.

If you want CURVE support, you may install one of the two variants:

```
pip install azmq[csodium]
```

Or:

```
pip install azmq[pysodium]
```

To enable curve support. The former is preferred as it comes with an embedded,
ready-to-use libsodium library.

Alternatively, libsodium support will be automatically enabled if you install
one of `csodium` or `pysodium` in the virtual environment.
