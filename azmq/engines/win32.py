"""
Windows-specific classes.
"""

import asyncio


DEFAULT_LIMIT = 2 ** 16


async def start_pipe_server(
    client_connected_cb,
    *,
    path,
    loop=None,
    limit=DEFAULT_LIMIT
):
    """
    Start listening for connection using Windows named pipes.
    """
    path = path.replace('/', '\\')
    loop = loop or asyncio.get_event_loop()

    def factory():
        reader = asyncio.StreamReader(limit=limit, loop=loop)
        protocol = asyncio.StreamReaderProtocol(
            reader,
            client_connected_cb,
            loop=loop,
        )

        return protocol

    server, *_ = await loop.start_serving_pipe(factory, address=path)

    # The returned instance sadly doesn't have a `wait_closed` method so we add
    # one.
    closed = asyncio.Event(loop=loop)
    original_close = server.close

    def close():
        original_close()
        closed.set()

    server.close = close
    server.wait_closed = closed.wait

    return server


async def open_pipe_connection(
    path=None,
    *,
    loop=None,
    limit=DEFAULT_LIMIT,
    **kwargs
):
    """
    Connect to a server using a Windows named pipe.
    """
    path = path.replace('/', '\\')
    loop = loop or asyncio.get_event_loop()

    reader = asyncio.StreamReader(limit=limit, loop=loop)
    protocol = asyncio.StreamReaderProtocol(reader, loop=loop)
    transport, _ = await loop.create_pipe_connection(
        lambda: protocol,
        path,
        **kwargs
    )
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)

    return reader, writer
