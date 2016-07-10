"""
Cryptographic helpers and tools.
"""

from functools import wraps


def libsodium_check(*args, **kwargs):
    """
    Check the presence and availability of libsodium.

    Accepts any argument.
    """
    if not HAS_LIBSODIUM:
        raise RuntimeError(  # pragma: no cover
            "No libsodium support installed. Please install on of the "
            "libsodium-enabled variants:\n- %s" % '\n- '.join([
                'azmq[sodium]',
                'azmq[pysodium]',
            ]),
        )


try:  # pragma: no cover
    from csodium import (
        crypto_box,
        crypto_box_keypair,
        crypto_box_afternm,
        crypto_box_beforenm,
        crypto_box_open,
        crypto_box_open_afternm,
        crypto_secretbox,
        crypto_secretbox_open,
        randombytes,
    )

    HAS_LIBSODIUM = True
except ImportError:  # pragma: no cover
    try:
        from pysodium import (
            crypto_box,
            crypto_box_keypair,
            crypto_box_afternm,
            crypto_box_beforenm,
            crypto_box_open,
            crypto_box_open_afternm,
            crypto_secretbox,
            crypto_secretbox_open,
            randombytes,
        )

        HAS_LIBSODIUM = True
    except ImportError:  # pragma: no cover
        # Provides fake functions so that import statements always work.
        crypto_box = libsodium_check
        crypto_box_keypair = libsodium_check
        crypto_box_afternm = libsodium_check
        crypto_box_beforenm = libsodium_check
        crypto_box_open = libsodium_check
        crypto_box_open_afternm = libsodium_check
        crypto_secretbox = libsodium_check
        crypto_secretbox_open = libsodium_check
        randombytes = libsodium_check

        HAS_LIBSODIUM = False


def requires_libsodium(func):
    """
    Mark a function as requiring libsodium.

    If no libsodium support is detected, a `RuntimeError` is thrown.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        libsodium_check()
        return func(*args, **kwargs)

    return wrapper


@requires_libsodium
def curve_gen_keypair():
    """
    Generate and return a pair of (public, secret) keys for use with the CURVE
    mechanism.

    :returns: A public, secret key pair, as a tuple.
    """
    return crypto_box_keypair()
