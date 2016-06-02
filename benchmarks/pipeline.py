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
push = ctx.socket(azmq.PUSH)
pull = ctx.socket(azmq.PULL)

async def run():
    push.bind('tcp://127.0.0.1:3333')
    pull.connect('tcp://127.0.0.1:3333')

    async def send():
        for _ in range(1000):
            await push.send_multipart([b'hello'])

    async def recv():
        for _ in range(1000):
            await pull.recv_multipart()

    send_task = asyncio.ensure_future(send())
    recv_task = asyncio.ensure_future(recv())
    await asyncio.gather(send_task, recv_task)

results = {}

with measure('azmq', results):
    loop.run_until_complete(run())

ctx.close()
loop.run_until_complete(ctx.wait_closed())

ctx = zmq.Context()
push = ctx.socket(zmq.PUSH)
pull = ctx.socket(zmq.PULL)

def run():
    push.bind('tcp://127.0.0.1:3333')
    pull.connect('tcp://127.0.0.1:3333')

    def send():
        for _ in range(1000):
            push.send_multipart([b'hello'])

    def recv():
        for _ in range(1000):
            pull.recv_multipart()

    send_thread = threading.Thread(target=send)
    recv_thread = threading.Thread(target=recv)
    send_thread.start()
    recv_thread.start()
    send_thread.join()
    recv_thread.join()

with measure('zmq', results):
    run()

logger.info("azmq/zmq: %.2f", results['azmq'] / results['zmq'])
