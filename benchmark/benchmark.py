"""
A benchmark for AZMQ.
"""

import asyncio
import azmq
import chromalog
import click
import logging
import signal
import sys
import threading

from contextlib import contextmanager
from functools import partial
from time import perf_counter


@contextmanager
def allow_interruption(*callbacks):
    if sys.platform == 'win32':
        from ctypes import WINFUNCTYPE, windll
        from ctypes.wintypes import BOOL, DWORD
        kernel32 = windll.LoadLibrary('kernel32')
        phandler_routine = WINFUNCTYPE(BOOL, DWORD)
        setconsolectrlhandler = kernel32.SetConsoleCtrlHandler
        setconsolectrlhandler.argtypes = (phandler_routine, BOOL)
        setconsolectrlhandler.restype = BOOL

        @phandler_routine
        def shutdown(event):
            if event == 0:
                for loop, cb in callbacks:
                    loop.call_soon_threadsafe(cb)

                return 1

            return 0

        if setconsolectrlhandler(shutdown, 1) == 0:
            raise WindowsError()
    else:
        def handler(*args):
            for loop, cb in callbacks:
                loop.call_soon_threadsafe(cb)

        signal.signal(signal.SIGINT, handler)

    try:
        yield
    finally:
        if sys.platform == 'win32':
            if setconsolectrlhandler(shutdown, 0) == 0:
                    raise WindowsError()
        else:
            signal.signal(signal.SIGINT, signal.SIG_DFL)


@click.group()
@click.option('--debug', '-d', is_flag=True, default=False)
@click.pass_context
def benchmark(ctx, debug):
    if debug:
        chromalog.basicConfig(level=logging.DEBUG)

    ctx.obj = {
        'debug': debug,
    }


@benchmark.command()
@click.option(
    '--in-connect',
    '-i',
    help=(
        "The endpoint to connect to for incoming messages.\n\n"
        "Not compatible with --in-bind."
    ),
    default='tcp://127.0.0.1:3334',
)
@click.option(
    '--in-bind',
    '-j',
    help=(
        "The endpoint to bind to for incoming messages."
        "\n\nNot compatible with --in-connect."
    ),
)
@click.option(
    '--out-connect',
    '-o',
    help=(
        "The endpoint to connect to for outgoing messages.\n\n"
        "Not compatible with --out-bind."
    ),
    default='tcp://127.0.0.1:3333',
)
@click.option(
    '--out-bind',
    '-k',
    help=(
        "The endpoint to bind to for outgoing messages."
        "\n\nNot compatible with --out-connect."
    ),
)
@click.option(
    '--count',
    '-n',
    help="The count of messages to send.",
    type=int,
    default=1000,
)
@click.option(
    '--size',
    '-s',
    help="The size of the messages.",
    type=int,
    default=1024,
)
@click.pass_context
def client(ctx, in_connect, in_bind, out_connect, out_bind, count, size):
    if sys.platform == 'win32':
        in_loop = asyncio.ProactorEventLoop()
    else:
        in_loop = asyncio.SelectorEventLoop()

    in_context = azmq.Context(loop=in_loop)
    in_socket = in_context.socket(azmq.PULL)

    if in_connect:
        click.echo("Incoming connecting to %s." % in_connect)
        in_socket.connect(in_connect)
    else:
        click.echo("Incoming binding on %s." % in_bind)
        in_socket.bind(in_bind)

    if sys.platform == 'win32':
        out_loop = asyncio.ProactorEventLoop()
    else:
        out_loop = asyncio.SelectorEventLoop()

    out_context = azmq.Context(loop=out_loop)
    out_socket = out_context.socket(azmq.PUSH)
    out_socket.max_outbox_size = 10

    if out_connect:
        click.echo("Outgoing connecting to %s." % out_connect)
        out_socket.connect(out_connect)
    else:
        click.echo("Outgoing binding on %s." % out_bind)
        out_socket.bind(out_bind)

    in_done = asyncio.Event(loop=out_loop)
    out_done = asyncio.Event(loop=in_loop)

    async def run_sender(context, socket, msg, count):
        click.echo("Sender started.")

        # The list of sending times.
        send_times = [None] * count
        send_overheads = [None] * count

        async with context:
            try:
                before = perf_counter()

                for i in range(count):
                    await socket.send_multipart([('%s' % i).encode('utf-8')] + msg)
                    now = perf_counter()
                    send_times[i] = now
                    send_overheads[i] = now - before
                    before = now
            finally:
                in_loop.call_soon_threadsafe(out_done.set)
                click.echo("Sender stopping...")
                await in_done.wait()
                click.echo("Sender stopped.")

    async def run_receiver(context, socket, msg, count):
        click.echo("Receiver started.")

        # The list of receiving times.
        receive_times = [None] * count
        receive_overheads = [None] * count

        async with context:
            try:
                before = perf_counter()

                for i in range(count):
                    await socket.recv_multipart()
                    now = perf_counter()
                    receive_times[i] = now
                    receive_overheads[i] = now - before
                    before = now
            finally:
                out_loop.call_soon_threadsafe(in_done.set)
                click.echo("Receiver stopping...")
                await out_done.wait()
                click.echo("Receiver stopped.")

    msg = [b'x' * size]

    out_task = asyncio.ensure_future(
        run_sender(out_context, out_socket, msg, count),
        loop=out_loop,
    )
    in_task = asyncio.ensure_future(
        run_receiver(in_context, in_socket, msg, count),
        loop=in_loop,
    )

    in_thread = threading.Thread(
        target=in_loop.run_until_complete,
        args=(in_context.wait_closed(),),
    )
    out_thread = threading.Thread(
        target=out_loop.run_until_complete,
        args=(out_context.wait_closed(),),
    )
    in_thread.start()
    out_thread.start()

    with allow_interruption(
        (in_loop, in_context.close),
        (out_loop, out_context.close),
    ):
        out_thread.join()
        in_thread.join()

    in_loop.run_until_complete(
        asyncio.gather(in_task, return_exceptions=True, loop=in_loop),
    )
    out_loop.run_until_complete(
        asyncio.gather(out_task, return_exceptions=True, loop=out_loop),
    )

    in_loop.close()
    out_loop.close()

@benchmark.command()
@click.option(
    '--in-connect',
    '-i',
    help=(
        "The endpoint to connect to for incoming messages.\n\n"
        "Not compatible with --in-bind."
    ),
)
@click.option(
    '--in-bind',
    '-j',
    help=(
        "The endpoint to bind to for incoming messages."
        "\n\nNot compatible with --in-connect."
    ),
    default='tcp://127.0.0.1:3333',
)
@click.option(
    '--out-connect',
    '-o',
    help=(
        "The endpoint to connect to for outgoing messages.\n\n"
        "Not compatible with --out-bind."
    ),
)
@click.option(
    '--out-bind',
    '-k',
    help=(
        "The endpoint to bind to for outgoing messages."
        "\n\nNot compatible with --out-connect."
    ),
    default='tcp://127.0.0.1:3334',
)
@click.pass_context
def broker(ctx, in_connect, in_bind, out_connect, out_bind):
    if sys.platform == 'win32':
        loop = asyncio.ProactorEventLoop()
    else:
        loop = asyncio.SelectorEventLoop()

    context = azmq.Context(loop=loop)
    in_socket = context.socket(azmq.PULL)
    out_socket = context.socket(azmq.PUSH)

    if in_connect:
        click.echo("Incoming connecting to %s." % in_connect)
        in_socket.connect(in_connect)
    else:
        click.echo("Incoming binding on %s." % in_bind)
        in_socket.bind(in_bind)

    if out_connect:
        click.echo("Outgoing connecting to %s." % out_connect)
        out_socket.connect(out_connect)
    else:
        click.echo("Outgoing binding on %s." % out_bind)
        out_socket.bind(out_bind)

    async def run(context, in_socket, out_socket):
        click.echo("Broker started.")

        try:
            while True:
                msg = await in_socket.recv_multipart()
                await out_socket.send_multipart(msg)
        finally:
            click.echo("Broker stopped.")

    task = asyncio.ensure_future(
        run(context, in_socket, out_socket),
        loop=loop,
    )

    with allow_interruption((loop, context.close)):
        loop.run_until_complete(context.wait_closed())

    loop.close()


if __name__ == '__main__':
    benchmark()
