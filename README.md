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
seriously limited in performance and functionality.

**AZMQ**'s goal is to lift those restrictions and to work the same on all
platforms that Python 3.5 supports.

## Installation

You may install it by using `pip`:

> pip install azmq
