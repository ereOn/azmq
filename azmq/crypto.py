"""
Cryptographic helpers and tools.
"""

from pysodium import crypto_box_keypair


def curve_gen_keypair():
    """
    Generate and return a pair of (public, secret) keys for use with the CURVE
    mechanism.

    :returns: A public, secret key pair, as a tuple.
    """
    return crypto_box_keypair()
