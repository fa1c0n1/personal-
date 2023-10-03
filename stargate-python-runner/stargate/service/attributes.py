import logging
from typing import Any, Dict, List, Optional, Type

from stargate.service.resource_manager_client import ResourceManagerGrpcClient
from stargate.util.customexceptions import AttributesServiceException
from shuri_services.api import AttributesService
from shuri_services.options import AttributesOptions

logger = logging.getLogger(__name__)


class AttributesServiceImpl(AttributesService):
    """The Attributes Service implementation using a GRPC client talking to external GRPC server."""

    def __init__(self, node_name: str, options: Optional[AttributesOptions]):
        # With the current approach of calling back the JVM which already has all these configs,
        # options are not used by this implementation.
        self.node_name = node_name
        self.options = options
        self.client = ResourceManagerGrpcClient()
        logger.info(f"Attribute service initialized successfully for node {self.node_name}")

    def read(self, key: str, data_type: Type[Any] = dict) -> Any:
        ok, value = self.client.read_attributes(self.node_name, key, data_type)
        if not ok:
            self._raise_service_exc(value)
        return value

    def bulk_read(self, keys: List[str], data_type: Type[Any] = dict) -> Dict[str, Any]:
        ok, value = self.client.bulk_read_attributes(self.node_name, keys, data_type)
        if not ok:
            self._raise_service_exc(value)
        return value

    def read_field(self, key: str, field_name: str, data_type: Type[Any] = str) -> Any:
        ok, value = self.client.read_attributes(self.node_name, key, data_type, field_name)
        if not ok:
            self._raise_service_exc(value)
        return value

    def read_fields(self, key: str, field_names: List[str]) -> Dict[str, Any]:
        ok, value = self.client.read_attributes_fields(self.node_name, key, field_names)
        if not ok:
            self._raise_service_exc(value)
        return value

    def read_range_fields(self, key: str, start_key: str, end_key: str) -> Dict[str, Any]:
        ok, value = self.client.read_attributes_range_fields(self.node_name, key, start_key, end_key)
        if not ok:
            self._raise_service_exc(value)
        return value

    def write(self, key: str, value: Any, data_type: Type[Any] = dict, ttl: int = -1) -> bool:
        ok, message = self.client.write_attributes(self.node_name, key, value, data_type, ttl)
        if not ok:
            self._raise_service_exc(message)
        return ok

    def bulk_write(self, key_value_dict: dict, data_type: Type[Any] = dict, ttl: int = -1) -> Dict[str, bool]:
        status, message = self.client.bulk_write_attributes(self.node_name, key_value_dict, data_type, ttl)
        if message:
            self._raise_service_exc(message)
        return status

    def write_field(self, key: str, field_name: str, field_value: Any, data_type: Type[Any] = str,
                    ttl: int = -1) -> bool:
        ok, message = self.client.write_attributes_field(self.node_name, key, field_name, field_value, data_type, ttl)
        if not ok:
            self._raise_service_exc(message)
        return ok

    def write_fields(self, key: str, fields: Dict[str, Any], ttl: int = -1) -> bool:
        ok, message = self.client.write_attributes_fields(self.node_name, key, fields, ttl)
        if not ok:
            self._raise_service_exc(message)
        return ok

    def delete(self, key: str) -> bool:
        ok, message = self.client.delete_attributes(self.node_name, key)
        if not ok:
            self._raise_service_exc(message)
        return ok

    def bulk_delete(self, keys: List[str]) -> Dict[str, bool]:
        status, message = self.client.bulk_delete_attributes(self.node_name, keys)
        if not message:
            self._raise_service_exc(message)
        return status

    def delete_field(self, key: str, field_name: str) -> bool:
        ok, message = self.client.delete_attributes(self.node_name, key, [field_name])
        if not ok:
            self._raise_service_exc(message)
        return ok

    def delete_fields(self, key: str, field_names: List[str]) -> bool:
        ok, message = self.client.delete_attributes(self.node_name, key, field_names)
        if not ok:
            self._raise_service_exc(message)
        return ok

    def _raise_service_exc(self, message: str):
        """In case of upstream exception, raise a custom exception."""
        logger.error(f"Request can't be processed - {message}")
        raise AttributesServiceException(message)
