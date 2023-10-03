import logging

from faker import Faker
from stargate.api.transform_api import Transform

logging.basicConfig(encoding='UTF-8', level=logging.INFO)
logger = logging.getLogger(__name__)


class LookupSample(Transform):
    def apply(self, input: dict):
        lookup_key = "stargate-test:key-x"
        status = self.lookup_service.write(lookup_key, input)
        logger.info(f"Lookup write status - {status} for key {lookup_key} with value {input.__str__()}")
        lookup_value = self.lookup_service.read(lookup_key)
        logger.info(f"Reading back saved lookup from service - for key {lookup_key} with value {lookup_value.__str__()}")
        return input


class BulkAsyncWriteLookup(Transform):
    def apply(self, input: dict):
        fake = Faker('en_US')
        lookup_key = "stargate-test:async-bulk-lookup-key-"
        bulk_dict = {}

        for i in range(0, 5):
            bulk_dict[lookup_key + str(i)] = {"name": fake.name(), "address": fake.address(),
                                              "cc_provider": fake.credit_card_provider(),
                                              "cc_sec_code": fake.credit_card_security_code()}

        logger.info(f"Async Lookup bulk write input - {str(bulk_dict)}")
        status = self.lookup_service.async_bulk_write(key_value_dict=bulk_dict,
                                                      publisher_id="quotes365-lookups",
                                                      namespaces=["live", "val"])
        logger.info(f"Async Lookup bulk write status - {str(status)}")
        return input


class BulkReadLookups(Transform):
    def apply(self, input: dict):
        lookup_key = "stargate-test:async-bulk-lookup-key-"
        bulk_read_keys = []

        for i in range(0, 5):
            bulk_read_keys.append(lookup_key+str(i))

        lookup_value = self.lookup_service.bulk_read(bulk_read_keys)
        logger.info(f"Reading back bulk lookups from service - {str(lookup_value)}")
        return input
