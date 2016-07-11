"""
Seucrity integration tests.
"""

import asyncio
import pytest

import azmq

from azmq.mechanisms import (
    CurveClient,
    CurveServer,
    PlainClient,
    PlainServer,
)
from azmq.crypto import curve_gen_keypair
from azmq.errors import (
    ZAPAuthenticationFailure,
    ZAPInternalError,
    ZAPTemporaryError,
)
from azmq.zap import (
    BaseZAPAuthenticator,
    ZAPAuthenticator,
    ZAPClient,
    ZAP_INPROC_ENDPOINT,
)

from ..conftest import requires_libsodium


ENDPOINT = 'tcp://127.0.0.1:5000'


def onesec(awaitable):
    """
    Causes a normally blocking call to timeout after a while.

    :param awaitable: The awaitable to wrap.
    :returns: A decorated awaitable that times out.
    """
    return asyncio.wait_for(awaitable, 1)


@pytest.mark.asyncio
async def test_ip_allow(event_loop):
    async with azmq.Context() as context:
        authenticator = ZAPAuthenticator(context)
        authenticator.allow('127.0.0.1')
        context.set_zap_authenticator(authenticator)
        req_socket = context.socket(azmq.REQ)
        rep_socket = context.socket(azmq.REP)

        try:
            req_socket.connect(ENDPOINT)
            rep_socket.bind(ENDPOINT)

            await req_socket.send_multipart([b'my', b'request'])
            message = await onesec(rep_socket.recv_multipart())
            assert message == [b'my', b'request']
            await rep_socket.send_multipart([b'my', b'response'])
            message = await onesec(req_socket.recv_multipart())
            assert message == [b'my', b'response']

        finally:
            req_socket.close()
            rep_socket.close()


@pytest.mark.asyncio
async def test_ip_deny(event_loop):
    async with azmq.Context() as context:
        authenticator = ZAPAuthenticator(context)
        authenticator.deny('127.0.0.1')
        context.set_zap_authenticator(authenticator)
        req_socket = context.socket(azmq.REQ)
        rep_socket = context.socket(azmq.REP)

        try:
            req_socket.connect(ENDPOINT)
            rep_socket.bind(ENDPOINT)

            await req_socket.send_multipart([b'my', b'request'])

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(rep_socket.recv_multipart(), 0.25)

        finally:
            req_socket.close()
            rep_socket.close()


@pytest.mark.asyncio
async def test_plain_valid_password(event_loop):
    async with azmq.Context() as context:
        authenticator = ZAPAuthenticator(context)
        authenticator.add_user(username='user', password='pwd')
        context.set_zap_authenticator(authenticator)
        req_socket = context.socket(
            azmq.REQ,
            mechanism=PlainClient(username='user', password='pwd'),
        )
        rep_socket = context.socket(azmq.REP, mechanism=PlainServer())

        try:
            req_socket.connect(ENDPOINT)
            rep_socket.bind(ENDPOINT)

            await req_socket.send_multipart([b'my', b'request'])
            message = await onesec(rep_socket.recv_multipart())
            assert message == [b'my', b'request']
            await rep_socket.send_multipart([b'my', b'response'])
            message = await onesec(req_socket.recv_multipart())
            assert message == [b'my', b'response']

        finally:
            req_socket.close()
            rep_socket.close()


@pytest.mark.asyncio
async def test_plain_unknown_username(event_loop):
    async with azmq.Context() as context:
        authenticator = ZAPAuthenticator(context)
        authenticator.add_user(username='user', password='pwd')
        context.set_zap_authenticator(authenticator)
        req_socket = context.socket(
            azmq.REQ,
            mechanism=PlainClient(username='user2', password='pwd'),
        )
        rep_socket = context.socket(azmq.REP, mechanism=PlainServer())

        try:
            req_socket.connect(ENDPOINT)
            rep_socket.bind(ENDPOINT)

            await req_socket.send_multipart([b'my', b'request'])

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(rep_socket.recv_multipart(), 0.25)

        finally:
            req_socket.close()
            rep_socket.close()


@pytest.mark.asyncio
async def test_plain_invalid_password(event_loop):
    async with azmq.Context() as context:
        authenticator = ZAPAuthenticator(context)
        authenticator.add_user(username='user', password='pwd')
        context.set_zap_authenticator(authenticator)
        req_socket = context.socket(
            azmq.REQ,
            mechanism=PlainClient(username='user', password='pwd2'),
        )
        rep_socket = context.socket(azmq.REP, mechanism=PlainServer())

        try:
            req_socket.connect(ENDPOINT)
            rep_socket.bind(ENDPOINT)

            await req_socket.send_multipart([b'my', b'request'])

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(rep_socket.recv_multipart(), 0.25)

        finally:
            req_socket.close()
            rep_socket.close()


@requires_libsodium
@pytest.mark.asyncio
async def test_curve_valid_key(event_loop):
    async with azmq.Context() as context:
        c_public_key, c_secret_key = curve_gen_keypair()
        s_public_key, s_secret_key = curve_gen_keypair()
        authenticator = ZAPAuthenticator(context)
        authenticator.add_authorized_key(key=c_public_key)
        context.set_zap_authenticator(authenticator)
        req_socket = context.socket(
            azmq.REQ,
            mechanism=CurveClient(
                public_key=c_public_key,
                secret_key=c_secret_key,
                server_key=s_public_key,
            ),
        )
        rep_socket = context.socket(
            azmq.REP,
            mechanism=CurveServer(
                public_key=s_public_key,
                secret_key=s_secret_key,
            ),
        )

        try:
            req_socket.connect(ENDPOINT)
            rep_socket.bind(ENDPOINT)

            await req_socket.send_multipart([b'my', b'request'])
            message = await onesec(rep_socket.recv_multipart())
            assert message == [b'my', b'request']
            await rep_socket.send_multipart([b'my', b'response'])
            message = await onesec(req_socket.recv_multipart())
            assert message == [b'my', b'response']

        finally:
            req_socket.close()
            rep_socket.close()


@requires_libsodium
@pytest.mark.asyncio
async def test_curve_invalid_key(event_loop):
    async with azmq.Context() as context:
        c_public_key, c_secret_key = curve_gen_keypair()
        s_public_key, s_secret_key = curve_gen_keypair()
        authenticator = ZAPAuthenticator(context)
        context.set_zap_authenticator(authenticator)
        req_socket = context.socket(
            azmq.REQ,
            mechanism=CurveClient(
                public_key=c_public_key,
                secret_key=c_secret_key,
                server_key=s_public_key,
            ),
        )
        rep_socket = context.socket(
            azmq.REP,
            mechanism=CurveServer(
                public_key=s_public_key,
                secret_key=s_secret_key,
            ),
        )

        try:
            req_socket.connect(ENDPOINT)
            rep_socket.bind(ENDPOINT)

            await req_socket.send_multipart([b'my', b'request'])

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(rep_socket.recv_multipart(), 0.25)

        finally:
            req_socket.close()
            rep_socket.close()


@requires_libsodium
@pytest.mark.asyncio
async def test_incompatible_mechanisms(event_loop):
    async with azmq.Context() as context:
        c_public_key, c_secret_key = curve_gen_keypair()
        s_public_key, s_secret_key = curve_gen_keypair()
        authenticator = ZAPAuthenticator(context)
        authenticator.add_authorized_key(key=c_public_key)
        context.set_zap_authenticator(authenticator)
        req_socket = context.socket(
            azmq.REQ,
            mechanism=CurveClient(
                public_key=c_public_key,
                secret_key=c_secret_key,
                server_key=s_public_key,
            ),
        )
        rep_socket = context.socket(azmq.REP)

        try:
            req_socket.connect(ENDPOINT)
            rep_socket.bind(ENDPOINT)

            await req_socket.send_multipart([b'my', b'request'])

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(rep_socket.recv_multipart(), 0.25)

        finally:
            req_socket.close()
            rep_socket.close()


@pytest.mark.asyncio
async def test_zap_successful_authentication(event_loop):
    class MyZAPAuthenticator(BaseZAPAuthenticator):
        async def on_request(self, *args, **kwargs):
            return 'bob', {b'foo': b'bar'}

    async with azmq.Context() as context:
        async with MyZAPAuthenticator(context=context):
            async with ZAPClient(context=context) as zap_client:
                username, metadata = await asyncio.wait_for(
                    zap_client.authenticate(
                        domain='domain',
                        address='127.0.0.1',
                        identity=b'bob',
                        mechanism=b'CURVE',
                        credentials=[b'mycred', b'value'],
                    ),
                    1,
                )

    assert username == 'bob'
    assert metadata == {b'foo': b'bar'}


@pytest.mark.asyncio
async def test_zap_temporary_error(event_loop):
    class MyZAPAuthenticator(BaseZAPAuthenticator):
        async def on_request(self, *args, **kwargs):
            raise ZAPTemporaryError("Some error")

    async with azmq.Context() as context:
        async with MyZAPAuthenticator(context=context):
            async with ZAPClient(context=context) as zap_client:
                with pytest.raises(ZAPTemporaryError):
                    await asyncio.wait_for(
                        zap_client.authenticate(
                            domain='domain',
                            address='127.0.0.1',
                            identity=b'bob',
                            mechanism=b'CURVE',
                            credentials=[b'mycred', b'value'],
                        ),
                        1,
                    )


@pytest.mark.asyncio
async def test_zap_authentication_failure(event_loop):
    class MyZAPAuthenticator(BaseZAPAuthenticator):
        async def on_request(self, *args, **kwargs):
            raise ZAPAuthenticationFailure("Some error")

    async with azmq.Context() as context:
        async with MyZAPAuthenticator(context=context):
            async with ZAPClient(context=context) as zap_client:
                with pytest.raises(ZAPAuthenticationFailure):
                    await asyncio.wait_for(
                        zap_client.authenticate(
                            domain='domain',
                            address='127.0.0.1',
                            identity=b'bob',
                            mechanism=b'CURVE',
                            credentials=[b'mycred', b'value'],
                        ),
                        1,
                    )


@pytest.mark.asyncio
async def test_zap_internal_error(event_loop):
    class MyZAPAuthenticator(BaseZAPAuthenticator):
        async def on_request(self, *args, **kwargs):
            raise RuntimeError

    async with azmq.Context() as context:
        async with MyZAPAuthenticator(context=context):
            async with ZAPClient(context=context) as zap_client:
                with pytest.raises(ZAPInternalError):
                    await asyncio.wait_for(
                        zap_client.authenticate(
                            domain='domain',
                            address='127.0.0.1',
                            identity=b'bob',
                            mechanism=b'CURVE',
                            credentials=[b'mycred', b'value'],
                        ),
                        1,
                    )


@pytest.mark.asyncio
async def test_zap_successful_authentication_after_invalid_request(event_loop):
    class MyZAPAuthenticator(BaseZAPAuthenticator):
        async def on_request(self, *args, **kwargs):
            return 'bob', {b'foo': b'bar'}

    async with azmq.Context() as context:
        async with MyZAPAuthenticator(context=context):
            async with ZAPClient(context=context) as zap_client:
                async with context.socket(azmq.DEALER) as socket:
                    socket.connect(ZAP_INPROC_ENDPOINT)
                    await socket.send_multipart([b'invalid', b'data'])

                    username, metadata = await asyncio.wait_for(
                        zap_client.authenticate(
                            domain='domain',
                            address='127.0.0.1',
                            identity=b'bob',
                            mechanism=b'CURVE',
                            credentials=[b'mycred', b'value'],
                        ),
                        1,
                    )

    assert username == 'bob'
    assert metadata == {b'foo': b'bar'}
