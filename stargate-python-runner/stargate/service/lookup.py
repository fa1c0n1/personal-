import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Any, Dict, List, Optional, Type

from shuri_services.options import LookupOptions
from stargate.api.stargate_lookup_service import StargateLookupService

from stargate.service.dhari import DhariServiceImpl
from stargate.service.resource_manager_client import ResourceManagerGrpcClient, _serialize_to_str
from stargate.util.customexceptions import LookupServiceException

logger = logging.getLogger(__name__)
executor = ThreadPoolExecutor(max_workers=int(os.getenv("LOOKUP_ASYNC_WRITE_DHARI_INVOKE_MAX_THREADPOOL_LIMIT", "10")))
BULK_WRITE_SCHEMA = "com.apple.aml.dhari.async.bulk.write.payload"


class LookupServiceImpl(StargateLookupService):
    """The Lookup Service implementation using a GRPC client talking to external GRPC server."""

    @property
    def dhari_service(self):
        return self._dhari_service

    @dhari_service.setter
    def dhari_service(self, dhari_service: DhariServiceImpl):
        logger.info("dhari service set in lookup service for bulk writes")
        self._dhari_service = dhari_service

    def __init__(self, node_name: str, options: Optional[LookupOptions]):
        self._dhari_service = None
        self.node_name = node_name
        self.options = options
        self.client = ResourceManagerGrpcClient()
        logger.info(f"Lookup service initialized successfully for node {self.node_name}")

    def read(self, key: str, data_type: Type[Any] = dict) -> Any:
        ok, value = self.client.read_lookup(self.node_name, key, data_type)
        if not ok:
            self._raise_service_exc(value)
        return value

    def bulk_read(self, keys: List[str], data_type: Type[Any] = dict) -> Dict[str, Any]:
        ok, value = self.client.bulk_read_lookup(self.node_name, keys, data_type)
        if not ok:
            self._raise_service_exc(value)
        else:
            return value

    def write(self, key: str, value: Any, data_type: Type[Any] = dict, ttl: int = -1) -> bool:
        ok, message = self.client.write_lookup(self.node_name, key, value, data_type, ttl)
        if not ok:
            self._raise_service_exc(message)
        return ok

    def bulk_write(self, key_value_dict: dict, data_type: Type[Any] = dict, ttl: int = -1) -> Dict[str, bool]:
        status, message = self.client.bulk_write_lookup(self.node_name, key_value_dict, data_type, ttl)
        if message:
            self._raise_service_exc(message)
        else:
            return status

    def async_bulk_write(self, key_value_dict: dict, publisher_id: str, data_type: Type[Any] = dict, ttl: int = -1,
                         namespaces: List[str] = []) -> Dict[str, str]:

        result = {}

        if not publisher_id:
            raise LookupServiceException("Missing publisher id for dhari")

        if not namespaces:
            raise LookupServiceException("Missing namespace.. Need at least one namespace for bulk inserts")

        for key, value in key_value_dict.items():
            ok, serialized_value, pb_data_type = _serialize_to_str(value, data_type)

            if not ok:
                raise LookupServiceException(f"Error during value serialization - {serialized_value}")
            else:
                kafka_payload = {
                    "key": key,
                    "value": serialized_value,
                    "dataType": pb_data_type,
                    "ttl": ttl,
                    "namespaces": [x.lower() for x in namespaces],
                    "timestamp": round(time.time() * 1000 * 1000)  # epoc time to microseconds
                }
                future: Future[Dict] = executor.submit(self.__publish_to_kafka,
                                                       kafka_payload, BULK_WRITE_SCHEMA, publisher_id)
                result[key] = future.result().get("payloadKey")

        return result

    def delete(self, key: str) -> bool:
        ok, message = self.client.delete_lookup(self.node_name, key)
        if not ok:
            self._raise_service_exc(message)
        return ok

    def bulk_delete(self, keys: List[str]) -> Dict[str, bool]:
        status, message = self.client.bulk_delete_lookup(self.node_name, keys)
        if message:
            self._raise_service_exc(message)
        else:
            return status

    def _raise_service_exc(self, message: str):
        """In case of upstream exception, raise a custom exception."""
        logger.error(f"Request can't be processed - {message}")
        raise LookupServiceException(message)

    def __publish_to_kafka(self, payload: str, schema_id: str, publisher_id: str) -> dict:
        return self._dhari_service.async_ingest(payload=payload, schema_id=schema_id, publisher_id=publisher_id)
