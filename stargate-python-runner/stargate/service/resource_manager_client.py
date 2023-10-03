import json
import os
import time
from typing import Any, Dict, List, Tuple, Type

from stargate.rpc.proto import sgrm_pb2_grpc
from stargate.rpc.proto.sgrm_pb2 import (
    A3TokenRequest, A3TokenValidateRequest, ErrorRequest, AttributesReadRequest, AttributesReadFieldsRequest,
    AttributesReadRangeFieldsRequest, AttributesWriteRequest, AttributesDeleteRequest, DataType,
    LookupReadRequest, LookupWriteRequest, LookupDeleteRequest, ShuriOfsReadFeatureRequest,
    ShuriOfsReadFeatureGroupsRequest, ShuriOfsWriteFeatureGroupRequest, ShuriOfsDeleteFeatureFieldRequest,
    AttributesBulkReadRequest, AttributesBulkWriteRequest, AttributesBulkDeleteRequest, LookupBulkReadRequest,
    LookupBulkWriteRequest, LookupBulkDeleteRequest
)
from stargate.util.grpc_utils import create_grpc_channel


class ResourceManagerGrpcClient:
    '''Client that talks to external services (on JVM, for example) over GRPC to deletegate service calls.'''

    def __init__(self, grpc_endpoint=None) -> None:
        # Since all are unary RPC calls, pool support may not be necessary and can be added later, if needed
        grpc_endpoint = grpc_endpoint or os.environ['STARGATE_MAIN_CONTAINER_GRPC_ENDPOINT']
        self.stub = sgrm_pb2_grpc.ResourceManagerProtoServiceStub(create_grpc_channel(grpc_endpoint, False))
        self._tokens_cache = {}

    def get_token(self, source_app_id: int, target_app_id: int, password: str, context_string: str = None) -> str:
        # This implementation is not thread-safe
        target_tokens = self._tokens_cache.get(source_app_id)
        if target_tokens is None:
            target_tokens = {}
            self._tokens_cache[source_app_id] = target_tokens

        if target_app_id not in target_tokens or target_tokens[target_app_id]['expiry'] <= round(time.time() * 1000):
            req = A3TokenRequest(
                sourceAppId=source_app_id,
                targetAppId=target_app_id,
                password=password,
                contextString=context_string
            )
            res = self.stub.fetchA3Token(req)
            target_tokens[target_app_id] = {'token': res.token, 'expiry': res.expiry}

        return target_tokens[target_app_id]['token']

    def validate_token(self, app_id: int, token: str) -> bool:
        res = self.stub.validateA3Token(A3TokenValidateRequest(appId=app_id, token=token))
        return res.valid

    def publish_error(self, node_name: str, schema_id: str, object_: str, message: str, details: str,
                      stack_trace: str) -> bool:
        req = ErrorRequest(
            nodeName=node_name,
            schemaId=schema_id,
            object=object_,
            message=message,
            details=details,
            stackTrace=stack_trace
        )
        res = self.stub.publishError(req)
        return res.status

    # Attributes Service methods
    # Read Attributes

    def read_attributes(self, node_name: str, key: str, data_type: Type[Any],
                        field_name: str = None) -> Tuple[bool, Any]:
        req = AttributesReadRequest(
            nodeName=node_name,
            key=key,
            fieldName=field_name,
            dataType=_get_pb_data_type(data_type)
        )
        res = self.stub.readAttributes(req)
        return _deserialize_from_str(res.value, res.dataType) if res.status else (False, res.message)

    def bulk_read_attributes(self, node_name: str, keys: List[str], data_type: Type[Any], field_name: str = None) -> \
    Tuple[bool, Dict[str, Any]]:
        bulk_req = AttributesBulkReadRequest(
            nodeName=node_name,
            keys=keys,
            fieldName=field_name,
            dataType=_get_pb_data_type(data_type))
        res = self.stub.bulkReadAttributes(bulk_req)
        return _deserialize_from_str(res.value, res.dataType) if res.status else (False, res.message)

    def read_attributes_fields(self, node_name: str, key: str,
                               field_names: List[str]) -> Tuple[bool, Dict[str, Any]]:
        req = AttributesReadFieldsRequest(
            nodeName=node_name,
            key=key,
            fieldNames=field_names,
            dataType=DataType.MAP
        )
        res = self.stub.readAttributesFields(req)
        return _deserialize_from_str(res.value, res.dataType) if res.status else (False, res.message)

    def read_attributes_range_fields(self, node_name: str, key: str, start_key: str,
                                     end_key: str) -> Tuple[bool, Dict[str, Any]]:
        req = AttributesReadRangeFieldsRequest(
            nodeName=node_name,
            key=key,
            startKey=start_key,
            endKey=end_key,
            dataType=DataType.MAP
        )
        res = self.stub.readAttributesRangeFields(req)
        return _deserialize_from_str(res.value, res.dataType) if res.status else (False, res.message)

    # Write Attributes

    def write_attributes(self, node_name: str, key: str, value: Any,
                         data_type: Type[Any], ttl: int) -> Tuple[bool, str]:
        ok, serialized_value, pb_data_type = _serialize_to_str(value, data_type)
        if not ok:
            return (ok, serialized_value)  # (False, Error message during serialization)
        req = AttributesWriteRequest(
            nodeName=node_name,
            key=key,
            value=serialized_value,
            dataType=pb_data_type,
            ttl=ttl
        )
        res = self.stub.writeAttributes(req)
        return res.status, res.message

    def bulk_write_attributes(self, node_name: str, key_value_dict: dict, data_type: Type[Any], ttl: int) -> Tuple[
        Dict[str, bool], str]:

        updated_key_value_dict = {}
        for key, value in key_value_dict.items():
            ok, serialized_value, pb_data_type = _serialize_to_str(value, data_type)
            if not ok:
                return ok, serialized_value  # (False, Error message during serialization)
            else:
                updated_key_value_dict[key] = serialized_value

        bulk_write_req = AttributesBulkWriteRequest(
            nodeName=node_name,
            keyValueCollection=updated_key_value_dict,
            dataType=pb_data_type,
            ttl=ttl
        )
        res = self.stub.bulkWriteAttributes(bulk_write_req)
        return res.status, res.message

    def write_attributes_field(self, node_name: str, key: str, field_name: str, field_value: Any,
                               data_type: Type[Any], ttl: int) -> Tuple[bool, str]:
        ok, serialized_value, pb_data_type = _serialize_to_str(field_value, data_type)
        if not ok:
            return (ok, serialized_value)  # (False, Error message during serialization)
        req = AttributesWriteRequest(
            nodeName=node_name,
            key=key,
            fieldName=field_name,
            value=serialized_value,
            dataType=pb_data_type,
            ttl=ttl
        )
        res = self.stub.writeAttributesField(req)
        return (res.status, res.message)

    def write_attributes_fields(self, node_name: str, key: str, fields: Dict[str, Any],
                                ttl: int) -> Tuple[bool, str]:
        ok, serialized_value, pb_data_type = _serialize_to_str(fields, dict)
        if not ok:
            return ok, serialized_value  # (False, Error message during serialization)
        req = AttributesWriteRequest(
            nodeName=node_name,
            key=key,
            value=serialized_value,
            dataType=pb_data_type,
            ttl=ttl
        )
        res = self.stub.writeAttributesFields(req)
        return res.status, res.message

    # Delete Attributes

    def delete_attributes(self, node_name: str, key: str, field_names: List[str] = None) -> Tuple[bool, str]:
        req = AttributesDeleteRequest(nodeName=node_name, key=key, fieldNames=field_names)
        res = self.stub.deleteAttributes(req)
        return res.status, res.message

    def bulk_delete_attributes(self, node_name: str, keys: List[str], field_names: List[str] = None) -> Tuple[
        Dict[str, bool], str]:
        bulk_del_req = AttributesBulkDeleteRequest(nodeName=node_name, keys=keys, fieldNames=field_names)
        res = self.stub.bulkDeleteAttributes(bulk_del_req)
        return res.status, res.message

    # Lookup Service methods
    # Read Lookup

    def read_lookup(self, node_name: str, key: str, data_type: Type[Any]) -> Tuple[bool, Any]:
        req = LookupReadRequest(nodeName=node_name, key=key, dataType=_get_pb_data_type(data_type))
        res = self.stub.readLookup(req)
        return _deserialize_from_str(res.value, res.dataType) if res.status else (False, res.message)

    def bulk_read_lookup(self, node_name: str, keys: List[str], data_type: Type[Any]) -> Tuple[bool, Dict[str, Any]]:
        req = LookupBulkReadRequest(nodeName=node_name, keys=keys, dataType=_get_pb_data_type(data_type))
        res = self.stub.bulkReadLookup(req)
        return _deserialize_from_str(res.value, res.dataType) if res.status else (False, res.message)

    # Write Lookup

    def write_lookup(self, node_name: str, key: str, value: Any,
                     data_type: Type[Any], ttl: int) -> Tuple[bool, str]:
        ok, serialized_value, pb_data_type = _serialize_to_str(value, data_type)
        if not ok:
            return (ok, serialized_value)  # (False, Error message during serialization)
        req = LookupWriteRequest(
            nodeName=node_name,
            key=key,
            value=serialized_value,
            dataType=pb_data_type,
            ttl=ttl
        )
        res = self.stub.writeLookup(req)
        return (res.status, res.message)

    def bulk_write_lookup(self, node_name: str, key_value_dict: dict, data_type: Type[Any], ttl: dict) -> Tuple[
        Dict[str, bool], str]:
        updated_key_value_dict = {}
        for key, value in key_value_dict.items():
            ok, serialized_value, pb_data_type = _serialize_to_str(value, data_type)
            if not ok:
                return ok, serialized_value  # (False, Error message during serialization)
            else:
                updated_key_value_dict[key] = serialized_value

        bulk_write_req = LookupBulkWriteRequest(
            nodeName=node_name,
            keyValueCollection=updated_key_value_dict,
            dataType=pb_data_type,
            ttl=ttl
        )
        res = self.stub.bulkWriteLookup(bulk_write_req)
        return res.status, res.message

    # Delete Lookup

    def delete_lookup(self, node_name: str, key: str) -> Tuple[bool, str]:
        req = LookupDeleteRequest(nodeName=node_name, key=key)
        res = self.stub.deleteLookup(req)
        return res.status, res.message

    def bulk_delete_lookup(self, node_name: str, keys: List[str]) -> Tuple[Dict[str, bool], str]:
        req = LookupBulkDeleteRequest(nodeName=node_name, keys=keys)
        res = self.stub.bulkDeleteLookup(req)
        return res.status, res.message

    # Shuri Ofs Service methods
    # Read Shuri Ofs

    def read_shuri_ofs_feature(self, node_name: str, feature_name: str, entity_id: str, field_name: str = None) -> str:
        req = ShuriOfsReadFeatureRequest(featureName=feature_name, entityId=entity_id, fieldName=field_name,
                                         nodeName=node_name)
        res = self.stub.readShuriOfsFeature(req)
        return res.value  # TODO - check if we can return something better than just string

    def read_shuri_ofs_feature_groups(self, node_name: str, feature_name: str, entity_ids: List[str]) -> str:
        req = ShuriOfsReadFeatureGroupsRequest(nodeName=node_name, featureName=feature_name, entityIds=entity_ids)
        res = self.stub.readShuriOfsFeatureGroups(req)
        return res.value

    # Write Shuri Ofs

    def write_shuri_ofs_feature_group(self, node_name: str, feature_name: str, entity_id: str, data: Dict[str, object],
                                      ttl: int = -1) -> bool:
        # If the value of the data item is a dict, then convert to json string, else just use the str representation
        data_s_dict = {k: json.dumps(v) if isinstance(v, dict) else str(v) for k, v in data.items()}
        req = ShuriOfsWriteFeatureGroupRequest(nodeName=node_name, featureName=feature_name, entityId=entity_id,
                                               data=data_s_dict, ttl=ttl)
        res = self.stub.writeShuriOfsFeatureGroup(req)
        return res.succeeded

    # Delete Shuri Ofs

    def delete_shuri_ofs_feature_field(self, node_name: str, feature_name: str, entity_id: str,
                                       field_name: str) -> bool:
        req = ShuriOfsDeleteFeatureFieldRequest(nodeName=node_name, featureName=feature_name, entityId=entity_id,
                                                fieldName=field_name)
        res = self.stub.deleteShuriOfsFeatureField(req)
        return res.succeeded


# Helper functions

def _serialize_to_str(py_value, py_data_type):
    """Input: Python value, Python data type.
    Returns tuple of (success, serialized str, Protobuf data type).
    If any error occurs during serialization, it returns the error message in serialized str field."""

    try:
        if py_data_type is set:
            return (True, json.dumps(list(py_value)), DataType.SET)
        elif py_data_type is dict:
            return (True, json.dumps(py_value), DataType.MAP)
        elif py_data_type is list:
            return (True, json.dumps(py_value), DataType.LIST)
        elif py_data_type is tuple:
            return (True, json.dumps(py_value), DataType.TUPLE)
        elif py_data_type is int:
            return (True, json.dumps(py_value), DataType.INTEGER)
        elif py_data_type is float:
            return (True, json.dumps(py_value), DataType.FLOAT)
        elif py_data_type is bool:
            return (True, json.dumps(py_value), DataType.BOOLEAN)
        elif py_data_type is str:
            return (True, json.dumps(py_value), DataType.STRING)
        else:
            return (True, str(py_value), DataType.OTHER)
    except Exception as e:
        return (False, f'{type(e).__name__}: {str(e)}', None)


def _deserialize_from_str(str_value, pb_data_type):
    """Input: String value from GRPC call, Protobuf data type.
    Returns tuple of (success, deserialized object).
    If any error occurs during deserialization, it returns the error message in deserialized value field.
    """
    try:
        if pb_data_type == DataType.SET:
            return (True, set(json.loads(str_value)))
        elif pb_data_type in (
                DataType.MAP, DataType.LIST, DataType.TUPLE, DataType.INTEGER, DataType.FLOAT, DataType.BOOLEAN,
                DataType.STRING):
            return (True, json.loads(str_value))
        else:
            return (True, str_value)
    except Exception as e:
        return (False, f'{type(e).__name__}: {str(e)}')


def _get_pb_data_type(py_data_type):
    """Returns Protobuf data type enum value based on Python data type.
    Note: Trying to avoid hashing a class object, hence these multiple if-else clauses instead of using a dict.
    """
    if py_data_type is set:
        return DataType.SET
    elif py_data_type is dict:
        return DataType.MAP
    elif py_data_type is list:
        return DataType.LIST
    elif py_data_type is tuple:
        return DataType.TUPLE
    elif py_data_type is int:
        return DataType.INTEGER
    elif py_data_type is float:
        return DataType.FLOAT
    elif py_data_type is bool:
        return DataType.BOOLEAN
    elif py_data_type is str:
        return DataType.STRING
    else:
        return DataType.OTHER
