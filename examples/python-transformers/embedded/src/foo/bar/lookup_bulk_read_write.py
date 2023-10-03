import logging

from faker import Faker
from stargate.api.transform_api import Transform

logging.basicConfig(encoding='UTF-8', level=logging.INFO)
logger = logging.getLogger(__name__)


class BulkReadWriteLookup(Transform):

    def apply(self, input: dict):
        fake = Faker('en_US')
        lookup_key = "stargate-test:bulk-lookup-key-"
        bulk_dict = {}

        for i in range(0, 5):
            bulk_dict[lookup_key + str(i)] = {"name": fake.name(), "address": fake.address(),
                                              "cc_provider": fake.credit_card_provider(),
                                              "cc_sec_code": fake.credit_card_security_code()}

        logger.info(f"Lookup Bulk write input - {str(bulk_dict)}")
        status = self.lookup_service.bulk_write(bulk_dict)
        logger.info(f"Lookup bulk write status - {status}")
        lookup_value = self.lookup_service.bulk_read(list(bulk_dict.keys()))
        logger.info(f"Reading back bulk lookup from service - {str(lookup_value)}")
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
        # lookup_key = "stargate-test:async-bulk-lookup-key-"
        # bulk_read_keys = []
        #
        # for i in range(0, 5):
        #     bulk_read_keys.append(lookup_key+str(i))

        bulk_read_keys = input['value'].split(" ")
        logger.info(f"Bulk read keys - {str(bulk_read_keys)}")
        lookup_value = self.lookup_service.bulk_read(bulk_read_keys)
        logger.info(f"lookup read value type {type(lookup_value)}")

        lookup_value_type = type(lookup_value)
        if lookup_value_type is dict:
            for key, value in lookup_value.items():
                logger.info(f"{key} -> {value}")

        else:
            logger.info(f"lookup_value_type -> {lookup_value_type}")
            logger.info(f"lookup value -> {str(lookup_value)}")

        # logger.info(f"Reading back bulk lookups from service - {str(lookup_value)}")
        return input