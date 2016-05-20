"""
API constants.
"""

DEALER = b'DEALER'
PUB = b'PUB'
REP = b'REP'
REQ = b'REQ'
ROUTER = b'ROUTER'
SUB = b'SUB'
XPUB = b'XPUB'
XREP = b'XREP'
XREQ = b'XREQ'
XSUB = b'XSUB'
PUSH = b'PUSH'
PULL = b'PULL'
PAIR = b'PAIR'

# This set contains all the legal socket type combinations, as per 37/ZMTP
# (http://rfc.zeromq.org/spec:37).

LEGAL_COMBINATIONS = {
    (REQ, REP),
    (REP, REQ),
    (REQ, ROUTER),
    (ROUTER, REQ),
    (REP, DEALER),
    (DEALER, REP),
    (DEALER, DEALER),
    (DEALER, ROUTER),
    (ROUTER, DEALER),
    (ROUTER, ROUTER),
    (PUB, SUB),
    (SUB, PUB),
    (PUB, XSUB),
    (XSUB, PUB),
    (XPUB, SUB),
    (SUB, XPUB),
    (XPUB, XSUB),
    (XSUB, XPUB),
    (PUSH, PULL),
    (PULL, PUSH),
    (PAIR, PAIR),
}
