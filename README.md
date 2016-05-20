[![Build Status](https://travis-ci.org/ereOn/azmq.svg?branch=master)](https://travis-ci.org/ereOn/azmq)
[![Coverage Status](https://coveralls.io/repos/ereOn/azmq/badge.svg?branch=master&service=github)](https://coveralls.io/github/ereOn/azmq?branch=master)
[![Documentation Status](https://readthedocs.org/projects/azmq/badge/?version=latest)](http://azmq.readthedocs.org/en/latest/?badge=latest)
[![PyPI](https://img.shields.io/pypi/pyversions/azmq.svg)](https://pypi.python.org/pypi/azmq/1.0.0)
[![PyPi version](https://img.shields.io/pypi/v/azmq.svg)](https://pypi.python.org/pypi/azmq/1.0.0)
[![PyPi downloads](https://img.shields.io/pypi/dm/azmq.svg)](https://pypi.python.org/pypi/azmq/1.0.0)

# AZMQ

**AZMQ** is a Python 3 asyncio-native implementation of [ZMTP](http://rfc.zeromq.org/spec:37) (the protocol behind ZMQ).

## Motivation

None of the existing Python ZMQ implementation currently implements a fully
functional asyncio-compatible version that works well on all platforms. This is
especially true on Windows for which the few existing implementations are
seriously limited in performance or functionality, and some require the use of
a specific event-loop, which prevents using the default, standard ones.

**AZMQ**'s goal is to lift those restrictions and to work the same on all
platforms that Python 3.5 supports by providing a pure asyncio-native
implementation.

## Current state and goals

**AZMQ** is currently **NOT** in a production-ready state. The code has not
been performance-tested let alone optimized. This will come later.

The main focus at the moment is to provide support for (checked items are
implemented already):

- All socket types:
  * [x] REQ
  * [x] REP
  * [x] DEALER
  * [x] ROUTER
  * [x] PUB
  * [ ] SUB
  * [ ] PUSH
  * [ ] PULL
  * [ ] PAIR

- Those transports:
  * [x] TCP client
  * [x] TCP server
  * [ ] Inproc
  * [ ] UNIX sockets

Also, the intended API tries to be close to the one of pyzmq, but not too
close. Here is an **non-exhaustive** list of some differences in the APIs:

- **AZMQ** methods never take a `timeout` parameter. In the asyncio world, you
  just use
  [`asyncio.wait_for()`](https://docs.python.org/3/library/asyncio-task.html#asyncio.wait_for)
  for that purpose.
- There is no
  [`Poller`](http://learning-0mq-with-pyzmq.readthedocs.io/en/latest/pyzmq/multisocket/zmqpoller.html)
  class. The asyncio event-loop already gives you everything you need in terms
  of "polling" several ZMQ sockets at the same time. Actually, it's better
  because you are not limited to ZMQ sockets.

## Installation

There is not official package available just yet but once there is, follow
these instructions:

---

You may install it by using `pip`:

> pip install azmq
