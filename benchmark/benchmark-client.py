import asyncio
import azmq
import logging
import struct
import sys
import threading
import time
import zmq

REPEAT = 10
DISCARD_EXTREMES = 2
MESSAGES_SIZES = [2 ** exp for exp in range(14)]
FRAMES_COUNT = 10
MESSAGES_COUNT = 100
ENDPOINTS = sys.argv[1:]

if not ENDPOINTS:
    ENDPOINTS = ['tcp://127.0.0.1:3000']

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('benchmark')

if sys.platform == 'win32':
    loop = asyncio.ProactorEventLoop()
    timefunc = time.clock
else:
    loop = asyncio.SelectorEventLoop()
    timefunc = time.time

asyncio.set_event_loop(loop)

results = []

for messages_size in MESSAGES_SIZES:
    messages = [
        [struct.pack('!H', i) * messages_size] * FRAMES_COUNT
        for i in range(MESSAGES_COUNT)
    ]

    azmq_durations = []

    for index in range(1, REPEAT + 1):
        ctx = azmq.Context()
        dealer = ctx.socket(azmq.DEALER)

        for endpoint in ENDPOINTS:
            dealer.connect(endpoint)

        async def run():
            sock = dealer

            for msg in messages:
                await sock.send_multipart(msg)

            await asyncio.gather(*[
                sock.recv_multipart() for _ in range(len(messages))
            ])

        # This gives a chance to the underlying TCP connections to complete.
        loop.run_until_complete(asyncio.sleep(0))
        logger.info(
            "azmq - %s - %s/%s - Started.",
            messages_size,
            index,
            REPEAT,
        )

        try:
            before = timefunc()
            loop.run_until_complete(run())
        finally:
            after = timefunc()
            duration = after - before
            azmq_durations.append(duration)
            logger.info(
                "azmq - %s - %s/%s - Stopped. Duration: %.2f second(s).",
                messages_size,
                index,
                REPEAT,
                duration,
            )
            ctx.close()
            loop.run_until_complete(ctx.wait_closed())

    azmq_durations = sorted(azmq_durations)[DISCARD_EXTREMES:-DISCARD_EXTREMES]
    pyzmq_durations = []

    for index in range(1, REPEAT + 1):
        ctx = zmq.Context()
        dealer = ctx.socket(zmq.DEALER)

        for endpoint in ENDPOINTS:
            dealer.connect(endpoint)

        logger.info(
            "pyzmq - %s - %s/%s - Started.",
            messages_size,
            index,
            REPEAT,
        )

        try:
            before = timefunc()

            for msg in messages:
                dealer.send_multipart(msg)

            for _ in range(len(messages)):
                dealer.recv_multipart()
        finally:
            after = timefunc()
            duration = after - before
            pyzmq_durations.append(duration)
            logger.info(
                "pyzmq - %s - %s/%s - Stopped. Duration: %.2f second(s).",
                messages_size,
                index,
                REPEAT,
                duration,
            )
            dealer.close()
            ctx.destroy()

    pyzmq_durations = sorted(pyzmq_durations)[
        DISCARD_EXTREMES:-DISCARD_EXTREMES
    ]
    pyzmq_no_copy_durations = []

    for index in range(1, REPEAT + 1):
        ctx = zmq.Context()
        dealer = ctx.socket(zmq.DEALER)

        for endpoint in ENDPOINTS:
            dealer.connect(endpoint)

        logger.info(
            "pyzmq (no copy) - %s - %s/%s - Started.",
            messages_size,
            index,
            REPEAT,
        )

        try:
            before = timefunc()

            for msg in messages:
                dealer.send_multipart(msg, copy=False)

            for _ in range(len(messages)):
                dealer.recv_multipart(copy=False)
        finally:
            after = timefunc()
            duration = after - before
            pyzmq_no_copy_durations.append(duration)
            logger.info(
                "pyzmq - %s - %s/%s - Stopped. Duration: %.2f second(s).",
                messages_size,
                index,
                REPEAT,
                duration,
            )
            dealer.close()
            ctx.destroy()

    pyzmq_no_copy_durations = sorted(pyzmq_no_copy_durations)[
        DISCARD_EXTREMES:-DISCARD_EXTREMES
    ]
    results.append(
        (
            messages_size,
            azmq_durations,
            pyzmq_durations,
            pyzmq_no_copy_durations,
        ),
    )

# Generate the data for the graph.
from numpy import mean
from plotly import plotly
from plotly.graph_objs import (
    Figure,
    Layout,
    Scatter,
)

data = [
    Scatter(
        name='azmq',
        x=[messages_size for messages_size, _, _, _ in results],
        y=[mean(durations) for _, durations, _, _ in results],
        error_y=dict(
            type='data',
            symmetric=False,
            array=[
                max(durations) - mean(durations)
                for _, durations, _, _ in results
            ],
            arrayminus=[
                mean(durations) - min(durations)
                for _, durations, _, _ in results
            ],
        ),
    ),
    Scatter(
        name='pyzmq',
        x=[messages_size for messages_size, _, _, _ in results],
        y=[mean(durations) for _, _, durations, _ in results],
        error_y=dict(
            type='data',
            symmetric=False,
            array=[
                max(durations) - mean(durations)
                for _, _, durations, _ in results
            ],
            arrayminus=[
                mean(durations) - min(durations)
                for _, _, durations, _ in results
            ],
        ),
    ),
    Scatter(
        name='pyzmq (no copy)',
        x=[messages_size for messages_size, _, _, _ in results],
        y=[mean(durations) for _, _, _, durations in results],
        error_y=dict(
            type='data',
            symmetric=False,
            array=[
                max(durations) - mean(durations)
                for _, _, _, durations in results
            ],
            arrayminus=[
                mean(durations) - min(durations)
                for _, _, _, durations in results
            ],
        ),
    ),
]
layout = Layout(
    title='AZMQ-PYZMQ time spent comparison',
    xaxis={
        'title': 'Messages size',
    },
    yaxis={
        'title': 'Duration (lower is better)',
    },
)
figure = Figure(data=data, layout=layout)
plotly.image.save_as(figure, filename='azmq_pyzmq_time_spent_comparison.png')
plot_url = plotly.plot(figure, filename='AZMQ-PYZMQ time spent comparison')
logger.info("Graph is available at: %s", plot_url)
