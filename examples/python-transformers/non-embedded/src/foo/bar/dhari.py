import logging

from stargate.api.transform_api import Transform

logging.basicConfig(encoding='UTF-8', level=logging.INFO)
logger = logging.getLogger(__name__)


class DhariSample(Transform):
    def apply(self, input: dict):
        dhari_response = self.dhari_service.ingest(input, schema_id=self.context.get("payloadSchemaId"), publisher_id=self.context.get("payloadPublisherId"))
        logger.info(f"Dhari response - {dhari_response.__str__()}")
        return input
