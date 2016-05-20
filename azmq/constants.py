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
    (REQ, XREP),
    (ROUTER, REQ),
    (XREP, REQ),
    (REP, DEALER),
    (REP, XREQ),
    (DEALER, REP),
    (XREQ, REP),
    (DEALER, DEALER),
    (XREQ, DEALER),
    (DEALER, XREQ),
    (DEALER, ROUTER),
    (DEALER, XREP),
    (XREQ, ROUTER),
    (XREQ, XREP),
    (ROUTER, DEALER),
    (XREP, DEALER),
    (ROUTER, XREQ),
    (XREP, XREQ),
    (ROUTER, ROUTER),
    (XREP, ROUTER),
    (ROUTER, XREP),
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
