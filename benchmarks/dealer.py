import asyncio
import azmq
import logging
import sys
import threading
import time
import zmq

from contextlib import contextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('benchmark')

if sys.platform == 'win32':
    loop = asyncio.ProactorEventLoop()
else:
    loop = asyncio.SelectorEventLoop()

asyncio.set_event_loop(loop)

@contextmanager
def measure(label, results):
    logger.info("%s - Started.", label)
    before = time.clock()

    try:
        yield
    finally:
        after = time.clock()
        logger.info(
            "%s - Stopped. Execution took: %s second(s).",
            label,
            after - before,
        )
        results[label] = after - before

ctx = azmq.Context()
dealer = ctx.socket(azmq.DEALER)

async def run():
    dealer.connect(sys.argv[1])

    for _ in range(1000):
        await dealer.send_multipart([b'hello'])

    for _ in range(1000):
        await dealer.recv_multipart()

results = {}

with measure('azmq', results):
    loop.run_until_complete(run())

ctx.close()
loop.run_until_complete(ctx.wait_closed())

ctx = zmq.Context()
dealer = ctx.socket(zmq.DEALER)

def run():
    dealer.connect(sys.argv[1])

    for _ in range(1000):
        dealer.send_multipart([b'hello'])

    for _ in range(1000):
        dealer.recv_multipart()

with measure('zmq', results):
    run()

logger.info("azmq/zmq: %.2f", results['azmq'] / results['zmq'])
