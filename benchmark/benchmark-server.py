import logging
import sys
import zmq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('benchmark-server')

ctx = zmq.Context()
router = ctx.socket(zmq.ROUTER)
endpoint = sys.argv[1] if len(sys.argv) >= 2 else 'tcp://0.0.0.0:3000'

logger.info("Starting on %s.", endpoint)
logger.info("Hit Ctrl+C to interrupt.")

router.bind(endpoint)

try:
    while True:
        if router.poll(1000):
            msg = router.recv_multipart()
            router.send_multipart(msg)
except KeyboardInterrupt:
    logger.warning("Interrupted.")
finally:
    logger.info("Stopping.")
