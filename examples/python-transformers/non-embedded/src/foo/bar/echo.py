import logging

from stargate.api.transform_api import Transform

logging.basicConfig(encoding='UTF-8', level=logging.INFO)
logger = logging.getLogger(__name__)


class Echo(Transform):
    def __init__(self):
        logging.info("Initialized Echo class successfully")

    def apply(self, input: dict):
        logger.info("Echo apply method invoked with input - " + input.__str__())
        return input
