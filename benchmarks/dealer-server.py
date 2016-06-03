import logging
import sys
import zmq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

ctx = zmq.Context()
router = ctx.socket(zmq.ROUTER)

logger.info("Starting on %s.", sys.argv[1])

router.bind(sys.argv[1])

try:
    while True:
        msg = router.recv_multipart()
        logger.info("Received: %r", msg)
        router.send_multipart(msg)
except KeyboardInterrupt:
    pass
finally:
    logger.info("Stopping.")
