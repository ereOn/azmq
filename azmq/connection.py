"""
Implements a ZMTP connection.
"""

import asyncio

from pyslot import Signal

from .common import ClosableAsyncObject
from .errors import ProtocolError
from .log import logger
from .messaging import (
    dump_ready_command,
    load_ready_command,
    read_command,
    read_first_greeting,
    read_second_greeting,
    write_command,
    write_first_greeting,
    write_second_greeting,
)

class Connection(ClosableAsyncObject):
    CLOSE_RETRY = object()

    class Retry(RuntimeError):
        pass

    """
    Implements a ZMTP connection.

    If closed with `CLOSE_RETRY`, instructs the managing engine to retry the
    connection.
    """
    def __init__(self, reader, writer, attributes):
        super().__init__()
        self.reader = reader
        self.writer = writer
        self.attributes = attributes
        self.on_ready = Signal()
        self.run_task = asyncio.ensure_future(self.run())

    async def on_close(self, result):
        self.writer.close()
        await self.run_task
        return result

    async def run(self):
        try:
            await self.on_run()
        except self.Retry:
            self.close(self.CLOSE_RETRY)
        except ProtocolError as ex:
            logger.info("Protocol error (%s). Terminating connection.", ex)
        except Exception:
            logger.exception("Unexpected error. Terminating connection.")
        finally:
            self.close()

    async def on_run(self):
        write_first_greeting(self.writer, major_version=3)
        major_version = await read_first_greeting(self.reader)

        if major_version < 3:
            logger.warning(
                "Unsupported peer's major version (%s). Disconnecting.",
                major_version,
            )
            return

        write_second_greeting(
            self.writer,
            minor_version=1,
            mechanism=b'NULL',
            as_server=False,
        )
        minor_version, mechanism, as_server = await read_second_greeting(
            self.reader,
        )
        logger.debug(
            "Peer is using version %s.%s with the '%s' authentication "
            "mechanism.",
            major_version,
            minor_version,
            mechanism.decode(),
        )

        if mechanism == b'NULL':
            write_command(
                self.writer,
                b'READY',
                dump_ready_command(self.attributes),
            )
            command_name, command_data = await read_command(self.reader)

            if command_name != b'READY':
                logger.warning("Unexpected command: %s.", command_name)
                return

            peer_attributes = load_ready_command(command_data)
            logger.debug("Peer attributes: %s", peer_attributes)
        else:
            logger.warning("Unsupported mechanism: %s.", mechanism)
            return
