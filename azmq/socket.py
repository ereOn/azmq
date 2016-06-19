"""
A ZMQ socket class implementation.
"""

import asyncio
import random
import struct

from urllib.parse import urlsplit
from contextlib import ExitStack
from functools import partial

from .common import (
    CompositeClosableAsyncObject,
    cancel_on_closing,
    AsyncInbox,
    AsyncOutbox,
)
from .constants import (
    DEALER,
    PAIR,
    PUB,
    PUSH,
    PULL,
    REP,
    REQ,
    ROUTER,
    SUB,
    XPUB,
    XSUB,
)
from .errors import (
    UnsupportedSchemeError,
    InvalidOperation,
)
from .connections.mechanisms import Null
from .engines.tcp import (
    TCPClientEngine,
    TCPServerEngine,
)
from .engines.inproc import (
    InprocClientEngine,
    InprocServerEngine,
)
from .log import logger
from .containers import AsyncList


class Peer(object):
    """
    Represents a peer connection, either establish or in the process of being
    established.
    """
    __slots__ = [
        'engine',
        'connection',
        'inbox',
        'outbox',
    ]

    def __init__(self, engine, connection, inbox, outbox):
        self.engine = engine
        self.connection = connection
        self.inbox = inbox
        self.outbox = outbox


class Socket(CompositeClosableAsyncObject):
    """
    A ZMQ socket.

    This class is **NOT** thread-safe.
    """
    def on_open(self, context, socket_type, identity=None, mechanism=None):
        super().on_open()

        self.context = context
        self.socket_type = socket_type
        self.identity = identity
        self.mechanism = mechanism or Null()
        self.max_inbox_size = 0
        self.max_outbox_size = 0
        self._outgoing_engines = {}
        self._outgoing_peers = {}
        self._incoming_engines = {}
        self._peers = AsyncList()
        self._in_peers = self._peers.create_proxy()
        self._out_peers = self._peers.create_proxy()
        self._base_identity = random.getrandbits(32)
        self._subscriptions = []
        self._read_lock = asyncio.Lock(loop=self.loop)

        if self.socket_type == REQ:
            # This future holds the last connection we sent a request to (or
            # none, if no request was sent yet). This allows to start receiving
            # before we send.
            self._current_peer = asyncio.Future(loop=self.loop)
            self.recv_multipart = self._recv_req
            self.send_multipart = self._send_req
        elif self.socket_type == REP:
            # This future holds the last connection we received a request from
            # (or none, if no request was received yet). This allows to start
            # receiving before we send.
            self._current_peer = asyncio.Future(loop=self.loop)
            self.recv_multipart = self._recv_rep
            self.send_multipart = self._send_rep
        elif self.socket_type == DEALER:
            self.recv_multipart = self._recv_dealer
            self.send_multipart = self._send_dealer
        elif self.socket_type == ROUTER:
            self.recv_multipart = self._recv_router
            self.send_multipart = self._send_router
        elif self.socket_type == PUB:
            self.recv_multipart = self._no_recv
            self.send_multipart = self._send_pub
        elif self.socket_type == XPUB:
            self.recv_multipart = self._recv_xpub
            self.send_multipart = self._send_pub  # This is not a typo.
        elif self.socket_type == SUB:
            self.recv_multipart = self._recv_sub
            self.send_multipart = self._no_send
        elif self.socket_type == XSUB:
            self.recv_multipart = self._recv_sub  # This is not a typo.
            self.send_multipart = self._send_xsub
        elif self.socket_type == PUSH:
            self.recv_multipart = self._no_recv
            self.send_multipart = self._send_push
        elif self.socket_type == PULL:
            self.recv_multipart = self._recv_pull
            self.send_multipart = self._no_send
        elif self.socket_type == PAIR:
            self.recv_multipart = self._recv_pair
            self.send_multipart = self._send_pair
        else:
            raise ValueError("Unsupported socket type: %r" % self.socket_type)

    def create_inbox(self):
        return AsyncInbox(
            maxsize=self.max_inbox_size,
            loop=self.loop,
        )

    def create_outbox(self):
        return AsyncOutbox(
            maxsize=self.max_outbox_size,
            loop=self.loop,
        )

    def connect(self, endpoint):
        url = urlsplit(endpoint)

        if url.scheme == 'tcp':
            engine = TCPClientEngine(
                host=url.hostname,
                port=url.port,
                zap_client=self.context.zap_client,
                socket_type=self.socket_type,
                identity=self.identity,
                mechanism=self.mechanism,
            )
        elif url.scheme == 'inproc':
            engine = InprocClientEngine(
                context=self.context,
                path=url.netloc,
                socket_type=self.socket_type,
                identity=self.identity,
                mechanism=self.mechanism,
            )
        else:
            raise UnsupportedSchemeError(scheme=url.scheme)

        engine.on_connection_ready.connect(
            partial(self.register_connection, engine=engine),
        )
        engine.on_connection_lost.connect(
            partial(self.unregister_connection, engine=engine),
        )
        self._outgoing_engines[url] = engine
        peer = Peer(
            engine=engine,
            connection=None,
            inbox=self.create_inbox(),
            outbox=self.create_outbox(),
        )
        self._outgoing_peers[engine] = peer
        self._peers.append(peer)
        self.register_child(engine)

    async def disconnect(self, endpoint):
        url = urlsplit(endpoint)

        engine = self._outgoing_engines.pop(url)
        peer = self._outgoing_peers.pop(engine)
        self._peers.remove(peer)
        engine.close()
        await engine.wait_closed()

    def bind(self, endpoint):
        url = urlsplit(endpoint)

        if url.scheme == 'tcp':
            engine = TCPServerEngine(
                host=url.hostname,
                port=url.port,
                zap_client=self.context.zap_client,
                socket_type=self.socket_type,
                identity=self.identity,
                mechanism=self.mechanism,
            )
        elif url.scheme == 'inproc':
            engine = InprocServerEngine(
                context=self.context,
                path=url.netloc,
                socket_type=self.socket_type,
                identity=self.identity,
                mechanism=self.mechanism,
            )
        else:
            raise UnsupportedSchemeError(scheme=url.scheme)

        engine.on_connection_ready.connect(
            partial(self.register_connection, engine=engine),
        )
        engine.on_connection_lost.connect(
            partial(self.unregister_connection, engine=engine),
        )
        self._incoming_engines[url] = engine
        self.register_child(engine)

    async def unbind(self, endpoint):
        url = urlsplit(endpoint)

        engine = self._incoming_engines.pop(url)
        engine.close()
        await engine.wait_closed()

    def register_connection(self, connection, engine):
        logger.debug("Registering new active connection: %s", connection)

        peer = self._outgoing_peers.get(engine)

        if peer:
            peer.connection = connection
            logger.debug("Updating connected peer with it's new connection.")
        else:
            peer = Peer(
                engine=engine,
                connection=connection,
                inbox=self.create_inbox(),
                outbox=self.create_outbox(),
            )
            self._peers.append(peer)
            logger.debug("Creating a new peer with the new connection.")

        connection.set_queues(inbox=peer.inbox, outbox=peer.outbox)

        if self.socket_type == ROUTER and not connection.remote_identity:
            connection.remote_identity = self.generate_identity()
            logger.info(
                "Peer did not specify an identity. Generated one for him "
                "(%r).",
                connection.remote_identity,
            )

        if self.socket_type in {SUB, XSUB}:
            for topic in self._subscriptions:
                asyncio.ensure_future(
                    connection.local_subscribe(topic),
                    loop=self.loop,
                )

    def unregister_connection(self, connection, engine):
        logger.debug("Unregistering inactive connection: %s", connection)

        if self.socket_type == XPUB and not connection.inbox.empty():
            logger.debug(
                "XPUB socket's inbox not empty: adding it back to the pool of "
                "inboxes.",
            )
            peer = next(p for p in self._peers if p.connection is connection)
            peer.engine = None
            peer.connection = None
            peer.outbox = None
        else:
            peer = self._outgoing_peers.get(engine)

            if peer:
                peer.connection = None
            else:
                self._peers.pop_first_match(
                    lambda p: p.connection is connection,
                )

    def generate_identity(self):
        """
        Generate a unique but random identity.
        """
        identity = struct.pack('!BI', 0, self._base_identity)
        self._base_identity += 1

        if self._base_identity >= 2 ** 32:
            self._base_identity = 0

        return identity

    async def _fair_get_in_peer(self):
        """
        Get the first available available inbound peer in a fair manner.

        :returns: A `Peer` inbox, whose inbox is guaranteed not to be
            empty (and thus can be read from without blocking).
        """
        peer = None

        while not peer:
            await self._in_peers.wait_not_empty()

            # This rotates the list, implementing fair-queuing.
            peers = list(self._in_peers)

            tasks = [
                asyncio.ensure_future(
                    p.inbox.wait_not_empty(),
                    loop=self.loop,
                )
                for p in peers
            ]

            try:
                done, pending = await asyncio.wait(
                    tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                for task in tasks:
                    task.cancel()

            peer = next(
                (
                    p
                    for task, p in zip(tasks, peers)
                    if task in done and not task.cancelled()
                ),
                None,
            )

        return peer

    async def _fair_recv(self):
        """
        Receive from all the existing peers, rotating the list of peers every
        time.

        :returns: The frames.
        """
        with await self._read_lock:
            peer = await self._fair_get_in_peer()
            result = peer.inbox.read_nowait()

            # Make sure we remove gone peers with empty inboxes.
            if not peer.engine and peer.inbox.empty():
                self._peers.remove(peer)

        return result

    async def _fair_get_out_peer(self):
        """
        Get the first available peer, with non-blocking inbox or wait until one
        meets the condition.

        :returns: The peer whose outbox is ready to be written to.
        """
        peer = None

        while not peer:
            await self._out_peers.wait_not_empty()

            # This rotates the list, implementing fair-queuing.
            peers = list(self._out_peers)

            tasks = [
                asyncio.ensure_future(
                    p.outbox.wait_not_full(),
                    loop=self.loop,
                )
                for p in peers
            ]

            try:
                done, pending = await asyncio.wait(
                    tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                for task in tasks:
                    task.cancel()

            peer = next(
                (
                    p
                    for task, p in zip(tasks, peers)
                    if task in done and not p.outbox.full()
                ),
                None,
            )

        return peer

    async def _fair_send(self, frames):
        """
        Send from the first available, non-blocking peer or wait until one
        meets the condition.

        :params frames: The frames to write.
        :returns: The peer that was used.
        """
        peer = await self._fair_get_out_peer()
        peer.outbox.write_nowait(frames)
        return peer

    @cancel_on_closing
    async def _send_req(self, frames):
        if self._current_peer.done():
            raise InvalidOperation(
                "Cannot send twice in a row from a REQ socket. Please recv "
                "from it first",
            )

        peer = await self._fair_send([b''] + frames)
        self._current_peer.set_result(peer)

    @cancel_on_closing
    async def _recv_req(self):
        await self._current_peer
        peer = self._current_peer.result()

        # Let's allow writes back as soon as we know on which peer to receive.
        self._current_peer = asyncio.Future(loop=self.loop)

        # As per 28/REQREP, a REQ socket SHALL discard silently any messages
        # received from other peers when processing incoming messages on the
        # current connection.
        with ExitStack() as stack:
            for p in self._peers:
                if p is not peer:
                    stack.enter_context(
                        p.connection.discard_incoming_messages(),
                    )

            frames = await peer.inbox.read()

            # We need to get rid of the empty delimiter.
            if frames[0] != b'':
                logger.warning(
                    "Received unexpected reply (%r) in REQ socket. Closing "
                    "connection. The recv() call will NEVER return !",
                    frames,
                )
                peer.connection.close()

                # This may seem weird but we must treat these errors as if the
                # peer did never reply, which means blocking forever (at least
                # until the socket is closed).
                #
                # ZMQ best-practices dictate the user should recreate the
                # socket anyway in case of timeouts and there is no other
                # sensible course of action: we can't return anything
                # meaningful and throwing an error puts the burden on the user
                # by forcing him/her to handle two possible outcomes.
                forever = asyncio.Future(loop=self.loop)
                await forever

            frames.pop(0)
            return frames

    @cancel_on_closing
    async def _send_rep(self, frames):
        await self._current_peer
        peer, envelope = self._current_peer.result()

        # Let's allow reads back as soon as we know on which connection to
        # receive.
        self._current_peer = asyncio.Future(loop=self.loop)

        await peer.outbox.write(envelope + frames)

    @cancel_on_closing
    async def _recv_rep(self):
        if self._current_peer.done():
            raise InvalidOperation(
                "Cannot receive twice in a row from a REP socket. Please send "
                "from it first",
            )

        peer = await self._fair_get_in_peer()
        frames = peer.inbox.read_nowait()
        delimiter_index = frames.index(b'')
        envelope = frames[:delimiter_index + 1]
        message = frames[delimiter_index + 1:]
        self._current_peer.set_result((peer, envelope))
        return message

    @cancel_on_closing
    async def _send_dealer(self, frames):
        await self._fair_send(frames)

    @cancel_on_closing
    async def _recv_dealer(self):
        return await self._fair_recv()

    @cancel_on_closing
    async def _send_router(self, frames):
        identity = frames.pop(0)

        try:
            peer = next(
                p for p in self._peers
                if p.connection.remote_identity == identity and
                not p.outbox.full()
            )
        except StopIteration:
            # We drop the messages as their is no suitable connection to write
            # it to.
            pass
        else:
            peer.outbox.write_nowait(frames)

    @cancel_on_closing
    async def _recv_router(self):
        peer = await self._fair_get_in_peer()
        frames = peer.inbox.read_nowait()
        frames.insert(0, peer.connection.remote_identity)
        return frames

    @cancel_on_closing
    async def _no_recv(self):
        raise AssertionError(
            "A %s socket cannot receive." % self.socket_type.decode(),
        )

    @cancel_on_closing
    async def _no_send(self):
        raise AssertionError(
            "A %s socket cannot send." % self.socket_type.decode(),
        )

    @cancel_on_closing
    async def _send_pub(self, frames):
        topic = frames[0]

        for peer in self._peers:
            if peer.connection and not peer.outbox.full() and next(
                (
                    topic.startswith(subs)
                    for subs in peer.connection.subscriptions
                ),
                None,
            ):
                peer.outbox.write_nowait(frames)

    @cancel_on_closing
    async def _recv_xpub(self):
        return await self._fair_recv()

    @cancel_on_closing
    async def _recv_sub(self):
        return await self._fair_recv()

    @cancel_on_closing
    async def _send_xsub(self, frames):
        first_frame = frames[0]

        if first_frame:
            type_ = first_frame[0]

            if type_ == 0:
                await self.unsubscribe(first_frame[1:])
            elif type_ == 1:
                await self.subscribe(first_frame[1:])

    @cancel_on_closing
    async def _send_push(self, frames):
        await self._fair_send(frames)

    @cancel_on_closing
    async def _recv_pull(self):
        return await self._fair_recv()

    @cancel_on_closing
    async def _send_pair(self, frames):
        # We only send to the first connected peer.
        peer = None

        while not peer:
            await self._peers.wait_not_empty()
            peer = self._peers[0]

            try:
                await peer.outbox.write(frames)
            except asyncio.CancelledError:
                peer = None

    @cancel_on_closing
    async def _recv_pair(self):
        # We only receive from the first connected peer.
        peer = None

        while not peer:
            await self._peers.wait_not_empty()
            peer = self._peers[0]

            try:
                return await peer.inbox.read()
            except asyncio.CancelledError:
                peer = None

    @cancel_on_closing
    async def subscribe(self, topic):
        """
        Subscribe the socket to the specified topic.

        :param topic: The topic to subscribe to.
        """
        if self.socket_type not in {SUB, XSUB}:
            raise AssertionError(
                "A %s socket cannot subscribe." % self.socket_type.decode(),
            )

        # Do this **BEFORE** awaiting so that new connections created during
        # the execution below honor the setting.
        self._subscriptions.append(topic)
        tasks = [
            asyncio.ensure_future(
                peer.connection.local_subscribe(topic),
                loop=self.loop,
            )
            for peer in self._peers
            if peer.connection
        ]

        if tasks:
            try:
                await asyncio.wait(tasks, loop=self.loop)
            finally:
                for task in tasks:
                    task.cancel()

    @cancel_on_closing
    async def unsubscribe(self, topic):
        """
        Unsubscribe the socket from the specified topic.

        :param topic: The topic to unsubscribe from.
        """
        if self.socket_type not in {SUB, XSUB}:
            raise AssertionError(
                "A %s socket cannot unsubscribe." % self.socket_type.decode(),
            )

        # Do this **BEFORE** awaiting so that new connections created during
        # the execution below honor the setting.
        self._subscriptions.append(topic)
        tasks = [
            asyncio.ensure_future(
                peer.connection.local_unsubscribe(topic),
                loop=self.loop,
            )
            for peer in self._peers
            if peer.connection
        ]

        if tasks:
            try:
                await asyncio.wait(tasks, loop=self.loop)
            finally:
                for task in tasks:
                    task.cancel()
