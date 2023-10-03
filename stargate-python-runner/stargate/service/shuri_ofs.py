import logging
from typing import Dict, List, Optional

from stargate.api.service_api import ShuriOfsService
from stargate.options.service_options import ShuriOfsOptions

from stargate.service.resource_manager_client import ResourceManagerGrpcClient

logger = logging.getLogger(__name__)


class ShuriOfsServiceImpl(ShuriOfsService):
    """The Shuri Ofs Service implementation using a GRPC client talking to external GRPC server."""

    def __init__(self, node_name: str, options: Optional[ShuriOfsOptions]):
        # With the current approach of calling back the JVM which already has all these configs,
        # options are not used by this implementation.
        self.node_name = node_name
        self.options = options
        self.client = ResourceManagerGrpcClient()
        logger.info(f"Shuri ofs service initialized successfully for node {self.node_name}")

    def read_feature_group(self, feature_name: str, entity_id: str) -> object:
        return self.client.read_shuri_ofs_feature(self.node_name, feature_name, entity_id)  # returns string

    def read_feature_field(self, feature_name: str, entity_id: str, field_name: str) -> object:
        return self.client.read_shuri_ofs_feature(self.node_name, feature_name, entity_id, field_name)  # returns string

    def read_feature_groups(self, feature_name: str, entity_ids: List[str]) -> object:
        return self.client.read_shuri_ofs_feature_groups(self.node_name, feature_name, entity_ids)  # returns string

    def write_feature_group(self, feature_name: str, entity_id: str, data: Dict[str, object], ttl: int = -1) -> bool:
        return self.client.write_shuri_ofs_feature_group(self.node_name, feature_name, entity_id, data, ttl)

    def delete_feature_field(self, feature_name: str, entity_id: str, field_name: str) -> bool:
        return self.client.delete_shuri_ofs_feature_field(self.node_name, feature_name, entity_id, field_name)

    def delete_feature_entity(self, feature_name: str, entity_id: str) -> bool:
        raise Exception("Not implemented")
