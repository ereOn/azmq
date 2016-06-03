import asyncio
import azmq
import logging
import struct
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

messages = [
    [struct.pack('!H', i) * 1024] * 10 for i in range(100)
]
ctx = azmq.Context()
dealer = ctx.socket(azmq.DEALER)

async def run():
    for ep in sys.argv[1:]:
        dealer.connect(ep)

    for msg in messages:
        await dealer.send_multipart(msg)

    await asyncio.gather(*[
        dealer.recv_multipart() for _ in range(len(messages))
    ])

results = {}

with measure('azmq', results):
    loop.run_until_complete(run())

ctx.close()
loop.run_until_complete(ctx.wait_closed())

logging.info("Waiting 2 seconds...")
loop.run_until_complete(asyncio.sleep(2))

ctx = zmq.Context()
dealer = ctx.socket(zmq.DEALER)

def run():
    for ep in sys.argv[1:]:
        dealer.connect(ep)

    for msg in messages:
        dealer.send_multipart(msg)

    for _ in range(len(messages)):
        dealer.recv_multipart()

with measure('zmq', results):
    run()

logger.info("zmq/azmq: %.2f", results['azmq'] / results['zmq'])
