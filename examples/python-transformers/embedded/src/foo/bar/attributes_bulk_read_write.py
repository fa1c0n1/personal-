import logging

from stargate.api.transform_api import Transform
from faker import Faker

logging.basicConfig(encoding='UTF-8', level=logging.INFO)
logger = logging.getLogger(__name__)


class BulkReadWriteAttributes(Transform):

    def apply(self, input: dict):
        fake = Faker(['it_IT', 'en_US', 'ja_JP'])
        lookup_key = "stargate-test:bulk-attr-key-"
        bulk_dict = {}

        for i in range(0, 100):
            bulk_dict[lookup_key + str(i)] = {"name": fake.name(), "address": fake.address(),
                                              "cc_provider": fake.credit_card_provider(),
                                              "cc_sec_code": fake.credit_card_security_code()}

        logger.info(f"Attribute Bulk write input - {str(bulk_dict)}")
        status = self.attributes_service.bulk_write(bulk_dict)
        logger.info(f"Attribute bulk write status - {status}")
        lookup_value = self.attributes_service.bulk_read(list(bulk_dict.keys()))
        logger.info(f"Reading back bulk attributes from service - {str(lookup_value)}")
        return input
