"""
Unit tests for ZAP.
"""

import pytest

from azmq.context import Context
from azmq.zap import ZAPAuthenticator


@pytest.yield_fixture
def context(event_loop):
    context = Context(loop=event_loop)
    yield context
    context.close()


@pytest.yield_fixture
def authenticator(event_loop, context):
    authenticator = ZAPAuthenticator(
        context=context,
        loop=event_loop,
    )
    yield authenticator
    authenticator.close()


def test_zap_authenticator_allow(authenticator):
    authenticator.allow('127.0.0.1')
    assert authenticator.whitelist == ['127.0.0.1']
    assert authenticator.blacklist == []


def test_zap_authenticator_deny(authenticator):
    authenticator.deny('127.0.0.1')
    assert authenticator.blacklist == ['127.0.0.1']
    assert authenticator.whitelist == []


def test_zap_authenticator_deny_after_allow(authenticator):
    authenticator.allow('127.0.0.1')
    authenticator.deny('127.0.0.1')
    assert authenticator.blacklist == ['127.0.0.1']
    assert authenticator.whitelist == []


def test_zap_authenticator_allow_after_deny(authenticator):
    authenticator.deny('127.0.0.1')
    authenticator.allow('127.0.0.1')
    assert authenticator.whitelist == ['127.0.0.1']
    assert authenticator.blacklist == []


def test_zap_authenticator_add_user(authenticator):
    authenticator.add_user(username='bob', password='pwd')
    assert authenticator.passwords == {'bob': 'pwd'}


def test_zap_authenticator_remove_user(authenticator):
    authenticator.remove_user(username='bob')
    assert authenticator.passwords == {}


def test_zap_authenticator_remove_user_after_add(authenticator):
    authenticator.add_user(username='bob', password='pwd')
    authenticator.remove_user(username='bob')
    assert authenticator.passwords == {}


def test_zap_authenticator_add_authorized_key(authenticator):
    authenticator.add_authorized_key(b'mykey')
    assert authenticator.authorized_keys == {b'mykey'}


def test_zap_authenticator_remove_authorized_key(authenticator):
    authenticator.remove_authorized_key(b'mykey')
    assert authenticator.authorized_keys == set()


def test_zap_authenticator_remove_authorized_key_after_add(authenticator):
    authenticator.add_authorized_key(b'mykey')
    authenticator.remove_authorized_key(b'mykey')
    assert authenticator.authorized_keys == set()
