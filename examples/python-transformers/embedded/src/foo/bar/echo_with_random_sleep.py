import logging
import random
import time

from stargate.api.transform_api import Transform

logging.basicConfig(encoding='UTF-8', level=logging.INFO)
logger = logging.getLogger(__name__)


class Echo(Transform):
    def __init__(self):
        logging.info("Initialized Echo class successfully")

    def apply(self, input: dict):
        logger.info("Echo apply method invoked with input - " + input.__str__())
        random_sleep = random.randint(350, 600)
        logger.info(f"sleeping for {random_sleep} seconds")
        time.sleep(random_sleep)
        logger.info(f"Returning after sleeping for {random_sleep} seconds - {input.__str__()}")
        return input
