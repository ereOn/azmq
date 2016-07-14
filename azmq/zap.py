"""
ZAP-related classes.
"""

import asyncio

from itertools import count

from .constants import (
    DEALER,
    ROUTER,
)
from .common import (
    CompositeClosableAsyncObject,
    cancel_on_closing,
)
from .errors import (
    ZAPTemporaryError,
    ZAPAuthenticationFailure,
    ZAPInternalError,
    ZAPError,
)
from .metadata import (
    buffer_to_metadata,
    metadata_to_buffers,
)

from .log import logger

ZAP_INPROC_ENDPOINT = 'inproc://zeromq.zap.01'


class ZAPClient(CompositeClosableAsyncObject):
    """
    A ZAP client.
    """
    def on_open(self, context):
        super().on_open()

        self._socket = context.socket(DEALER)
        self._socket.connect(ZAP_INPROC_ENDPOINT)
        self.register_child(self._socket)
        self._requests = {}
        self._request_ids = count()
        self._run_task = asyncio.ensure_future(self.run())

    @cancel_on_closing
    async def run(self):
        while True:
            frames = await self._socket.recv_multipart()

            try:
                assert frames[0] == b''
                assert frames[1] == b'1.0'
                request_id = frames[2]
                status_code = int(frames[3].decode('ascii'))
                status_text = frames[4].decode('utf-8')
                user_id = frames[5].decode('utf-8')
                metadata = buffer_to_metadata(frames[6]) if frames[6] else {}
            except Exception as ex:
                logger.warning(
                    "Unexpected error while handling ZAP response (%r): %s."
                    " Ignoring.",
                    frames,
                    ex,
                )
                continue

            future = self._requests.get(request_id)

            if not future:
                logger.debug(
                    "Got authentication result for unknown request %r. "
                    "Ignoring.",
                    request_id,
                )
            else:
                if status_code == 200:
                    future.set_result((user_id, metadata))
                elif status_code == 300:
                    future.set_exception(ZAPTemporaryError(status_text))
                elif status_code == 400:
                    future.set_exception(ZAPAuthenticationFailure(status_text))
                elif status_code == 500:
                    future.set_exception(ZAPInternalError(status_text))
                else:
                    future.set_exception(ZAPError(status_text, status_code))

    def _get_next_request_id(self):
        request_id = next(self._request_ids)
        return '%x' % request_id

    @cancel_on_closing
    async def authenticate(
        self,
        domain,
        address,
        identity,
        mechanism,
        credentials,
    ):
        request_id = self._get_next_request_id()
        await self._socket.send_multipart([
            b'',
            b'1.0',
            request_id,
            domain.encode('utf-8'),
            address.encode('ascii'),
            identity,
            mechanism,
        ] + credentials)
        future = asyncio.Future(loop=self.loop)
        self._requests[request_id] = future

        @future.add_done_callback
        def remove_request(_):
            del self._requests[request_id]

        await future
        return future.result()


class BaseZAPAuthenticator(CompositeClosableAsyncObject):
    """
    A base class for ZAP authenticators.
    """
    def on_open(self, context):
        super().on_open()

        self._socket = context.socket(ROUTER)
        self._socket.bind(ZAP_INPROC_ENDPOINT)
        self.register_child(self._socket)
        self.run_task = asyncio.ensure_future(self.run())

    @cancel_on_closing
    async def run(self):
        logger.debug("ZAP authenticator started.")

        try:
            while True:
                frames = await self._socket.recv_multipart()

                try:
                    delimiter_index = frames.index(b'')
                    envelope = frames[:delimiter_index + 1]
                    message = frames[delimiter_index + 1:]
                    assert message[0] == b'1.0'
                    request_id = message[1]
                    domain = message[2].decode('utf-8')
                    address = message[3].decode('ascii')
                    identity = message[4]
                    mechanism = message[5]
                    credentials = message[6:]

                except Exception as ex:
                    logger.warning(
                        "Unexpected error while handling ZAP request (%r): %s."
                        " Ignoring.",
                        frames,
                        ex,
                    )
                    continue

                try:
                    user_id, metadata = await self.on_request(
                        domain=domain,
                        address=address,
                        identity=identity,
                        mechanism=mechanism,
                        credentials=credentials,
                    )

                    await self._socket.send_multipart(envelope + [
                        b'1.0',
                        request_id,
                        b'200',
                        b'',
                        user_id.encode('utf-8') if user_id else b'',
                        b''.join(metadata_to_buffers(metadata))
                        if metadata else b''
                    ])
                except ZAPError as ex:
                    await self._socket.send_multipart(envelope + [
                        b'1.0',
                        request_id,
                        str(ex.code).encode('ascii'),
                        ex.text.encode('utf-8'),
                        b'',
                        b'',
                    ])
                except Exception as ex:
                    await self._socket.send_multipart(envelope + [
                        b'1.0',
                        request_id,
                        b'500',
                        str(ex).encode('utf-8'),
                        b'',
                        b'',
                    ])
        finally:
            logger.debug("ZAP authenticator stopped.")

    async def on_request(
        self,
        domain,
        address,
        identity,
        mechanism,
        credentials,
    ):
        """
        Handle a ZAP request.
        """
        raise NotImplementedError  # pragma: no cover


class ZAPAuthenticator(BaseZAPAuthenticator):
    """
    A basic ZAP authenticator.
    """
    def on_open(self, *args, **kwargs):
        super().on_open(*args, **kwargs)
        self.whitelist = []
        self.blacklist = []
        self.passwords = {}
        self.authorized_keys = set()

    def allow(self, ip):
        """
        Allow the specified IP to connect.

        :param ip: The IPv4 or IPv6 address to allow.
        """
        self.whitelist.append(ip)
        self.blacklist.clear()

    def deny(self, ip):
        """
        Allow the specified IP to connect.

        :param ip: The IPv4 or IPv6 address to allow.
        """
        self.blacklist.append(ip)
        self.whitelist.clear()

    def add_user(self, username, password):
        """
        Add or update a user in the internal passwords database, for use with
        the PLAIN mechanism.

        :param username: The username.
        :param password: The password.
        """
        self.passwords[username] = password

    def remove_user(self, username):
        """
        Remove a user from the internal passwords database.

        :param username: The username. If no such user exists, nothing is done.
        """
        try:
            del self.passwords[username]
        except KeyError:
            pass

    def add_authorized_key(self, key):
        """
        Add a new authorized permanent client key.

        :param public_key: The public permanent key.
        """
        self.authorized_keys.add(key)

    def remove_authorized_key(self, key):
        """
        Remove an existing authorized permanent client key.

        :param public_key: The public permanent key.
        """
        self.authorized_keys.discard(key)

    async def on_request(
        self,
        domain,
        address,
        identity,
        mechanism,
        credentials,
    ):
        """
        Handle a ZAP request.
        """
        logger.debug(
            "Request in domain %s for %s (%r): %r (%r)",
            domain,
            address,
            identity,
            mechanism,
            credentials,
        )

        user_id = None
        metadata = {}

        if self.whitelist:
            if address not in self.whitelist:
                raise ZAPAuthenticationFailure(
                    "IP address is not in the whitelist",
                )
        elif self.blacklist:
            if address in self.blacklist:
                raise ZAPAuthenticationFailure("IP address is blacklisted")

        if mechanism == b'PLAIN':
            username = credentials[0].decode('utf-8')
            password = credentials[1].decode('utf-8')
            ref_password = self.passwords.get(username)

            if not ref_password:
                raise ZAPAuthenticationFailure("No such user %r" % username)

            if password != ref_password:
                raise ZAPAuthenticationFailure(
                    "Invalid password for user %r" % username,
                )

            user_id = username

        elif mechanism == b'CURVE':
            public_key = credentials[0]

            if public_key not in self.authorized_keys:
                raise ZAPAuthenticationFailure(
                    "Unauthorized key %r" % public_key,
                )

        return user_id, metadata
