"""
The base class for ZMTP connections.
"""

import asyncio

from contextlib import contextmanager

from ..common import (
    CompositeClosableAsyncObject,
    cancel_on_closing,
)
from ..constants import (
    LEGAL_COMBINATIONS,
    XPUB,
)
from ..errors import ProtocolError
from ..log import logger


class BaseConnection(CompositeClosableAsyncObject):
    """
    The base class for ZMTP connections.
    """
    def __init__(
        self,
        socket_type,
        identity,
        mechanism,
        on_ready,
        on_lost,
        **kwargs
    ):
        assert socket_type, "A socket-type must be specified."

        super().__init__(**kwargs)
        self.socket_type = socket_type
        self.identity = identity
        self.mechanism = mechanism()
        self.on_ready = on_ready
        self.on_lost = on_lost

        self.remote_socket_type = None
        self.remote_identity = None

        self.inbox = None
        self.outbox = None
        self.subscriptions = []
        self._discard_incoming_messages = False
        self._run_task = asyncio.ensure_future(self.run(), loop=self.loop)

    async def on_close(self, result):
        # When the closing state is set, all tasks in the run task are
        # guaranteed to stop, so we can just wait gracefully for it to happen.
        await self._run_task
        await self.unsubscribe_all()
        await super().on_close(result)

        return result

    def get_metadata(self):
        metadata = {
            b'Socket-Type': self.socket_type,
        }

        if self.identity:
            metadata[b'Identity'] = self.identity

        return metadata

    def set_remote_metadata(self, metadata):
        logger.debug("Peer metadata: %s", metadata)

        for key, value in metadata.items():
            key = key.lower()

            if key == b'identity':
                # Peer-specified identities can't start with b'\x00'.
                if value and not value[0]:
                    raise ProtocolError(
                        "Peer specified an invalid identity (%r)." % value,
                        fatal=True,
                    )

                self.remote_identity = value
            elif key == b'socket-type':
                self.remote_socket_type = value

        pair = (self.socket_type, self.remote_socket_type)

        if pair not in LEGAL_COMBINATIONS:
            raise ProtocolError(
                "Incompatible socket types (%s <-> %s)." % pair,
                fatal=True,
            )

    def set_queues(self, inbox, outbox):
        self.inbox = inbox
        self.outbox = outbox

    @contextmanager
    def discard_incoming_messages(self, clear=True):
        """
        Discard all incoming messages for the time of the context manager.

        :param clear: A flag that, if set, also clears already received (but
            not read) incoming messages from the inbox. Default is `True`.
        """
        if clear:
            # Flush any received message so far.
            self.inbox.clear()

        # This allows nesting of discard_incoming_messages() calls.
        previous = self._discard_incoming_messages
        self._discard_incoming_messages = True

        try:
            yield
        finally:
            self._discard_incoming_messages = previous

    @cancel_on_closing
    async def local_subscribe(self, topic):
        logger.debug("Subscribed to topic %r.", topic)

        if self.outbox:
            await self.outbox.write([b'\x01' + topic])

    @cancel_on_closing
    async def local_unsubscribe(self, topic):
        logger.debug("Unsubscribed from topic %r.", topic)

        if self.outbox:
            await self.outbox.write([b'\x00' + topic])

    async def subscribe(self, topic):
        self.subscriptions.append(topic)
        logger.debug("Peer subscribed to topic %r.", topic)

        # XPUB sockets must inform the application of the subscription.
        if self.socket_type == XPUB:
            if self.inbox:
                await self.inbox.write([b'\x01' + topic])

    async def unsubscribe(self, topic):
        try:
            self.subscriptions.remove(topic)
            logger.debug("Peer unsubscribed from topic %r.", topic)
        except ValueError:
            pass
        else:
            # XPUB sockets must inform the application of the unsubscription.
            if self.socket_type == XPUB:
                if self.inbox:
                    await self.inbox.write([b'\x00' + topic])

    async def unsubscribe_all(self):
        await asyncio.gather(
            *[self.unsubscribe(topic) for topic in self.subscriptions[:]]
        )
