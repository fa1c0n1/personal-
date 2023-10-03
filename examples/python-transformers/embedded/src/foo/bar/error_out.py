import logging

from stargate.api.transform_api import Transform

logging.basicConfig(encoding='UTF-8', level=logging.INFO)
logger = logging.getLogger(__name__)


class ErrorOut(Transform):
    def __init__(self):
        logging.info("Initialized ErrorOut class successfully")

    def apply(self, input: dict):
        logger.info("ErrorOut apply method invoked with input - " + input.__str__())
        raise Exception("Errorring out purposefully !!")
