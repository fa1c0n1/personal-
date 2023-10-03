from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

BOOLEAN: DataType
DESCRIPTOR: _descriptor.FileDescriptor
FLOAT: DataType
INTEGER: DataType
LIST: DataType
MAP: DataType
OTHER: DataType
SET: DataType
STRING: DataType
TUPLE: DataType

class A3TokenRequest(_message.Message):
    __slots__ = ["contextString", "password", "sourceAppId", "targetAppId"]
    CONTEXTSTRING_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    SOURCEAPPID_FIELD_NUMBER: _ClassVar[int]
    TARGETAPPID_FIELD_NUMBER: _ClassVar[int]
    contextString: str
    password: str
    sourceAppId: int
    targetAppId: int
    def __init__(self, sourceAppId: _Optional[int] = ..., targetAppId: _Optional[int] = ..., password: _Optional[str] = ..., contextString: _Optional[str] = ...) -> None: ...

class A3TokenResponse(_message.Message):
    __slots__ = ["expiry", "token"]
    EXPIRY_FIELD_NUMBER: _ClassVar[int]
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    expiry: int
    token: str
    def __init__(self, token: _Optional[str] = ..., expiry: _Optional[int] = ...) -> None: ...

class A3TokenValidateRequest(_message.Message):
    __slots__ = ["appId", "token"]
    APPID_FIELD_NUMBER: _ClassVar[int]
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    appId: int
    token: str
    def __init__(self, appId: _Optional[int] = ..., token: _Optional[str] = ...) -> None: ...

class A3TokenValidateResponse(_message.Message):
    __slots__ = ["message", "valid"]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    VALID_FIELD_NUMBER: _ClassVar[int]
    message: str
    valid: bool
    def __init__(self, valid: bool = ..., message: _Optional[str] = ...) -> None: ...

class AttributesBulkDeleteRequest(_message.Message):
    __slots__ = ["fieldNames", "keys", "nodeName"]
    FIELDNAMES_FIELD_NUMBER: _ClassVar[int]
    KEYS_FIELD_NUMBER: _ClassVar[int]
    NODENAME_FIELD_NUMBER: _ClassVar[int]
    fieldNames: _containers.RepeatedScalarFieldContainer[str]
    keys: _containers.RepeatedScalarFieldContainer[str]
    nodeName: str
    def __init__(self, nodeName: _Optional[str] = ..., keys: _Optional[_Iterable[str]] = ..., fieldNames: _Optional[_Iterable[str]] = ...) -> None: ...

class AttributesBulkReadRequest(_message.Message):
    __slots__ = ["dataType", "fieldName", "keys", "nodeName"]
    DATATYPE_FIELD_NUMBER: _ClassVar[int]
    FIELDNAME_FIELD_NUMBER: _ClassVar[int]
    KEYS_FIELD_NUMBER: _ClassVar[int]
    NODENAME_FIELD_NUMBER: _ClassVar[int]
    dataType: DataType
    fieldName: str
    keys: _containers.RepeatedScalarFieldContainer[str]
    nodeName: str
    def __init__(self, nodeName: _Optional[str] = ..., keys: _Optional[_Iterable[str]] = ..., fieldName: _Optional[str] = ..., dataType: _Optional[_Union[DataType, str]] = ...) -> None: ...

class AttributesBulkWriteRequest(_message.Message):
    __slots__ = ["dataType", "fieldName", "keyValueCollection", "nodeName", "ttl"]
    class KeyValueCollectionEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    DATATYPE_FIELD_NUMBER: _ClassVar[int]
    FIELDNAME_FIELD_NUMBER: _ClassVar[int]
    KEYVALUECOLLECTION_FIELD_NUMBER: _ClassVar[int]
    NODENAME_FIELD_NUMBER: _ClassVar[int]
    TTL_FIELD_NUMBER: _ClassVar[int]
    dataType: DataType
    fieldName: str
    keyValueCollection: _containers.ScalarMap[str, str]
    nodeName: str
    ttl: int
    def __init__(self, nodeName: _Optional[str] = ..., keyValueCollection: _Optional[_Mapping[str, str]] = ..., fieldName: _Optional[str] = ..., dataType: _Optional[_Union[DataType, str]] = ..., ttl: _Optional[int] = ...) -> None: ...

class AttributesDeleteRequest(_message.Message):
    __slots__ = ["fieldNames", "key", "nodeName"]
    FIELDNAMES_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    NODENAME_FIELD_NUMBER: _ClassVar[int]
    fieldNames: _containers.RepeatedScalarFieldContainer[str]
    key: str
    nodeName: str
    def __init__(self, nodeName: _Optional[str] = ..., key: _Optional[str] = ..., fieldNames: _Optional[_Iterable[str]] = ...) -> None: ...

class AttributesReadFieldsRequest(_message.Message):
    __slots__ = ["dataType", "fieldNames", "key", "nodeName"]
    DATATYPE_FIELD_NUMBER: _ClassVar[int]
    FIELDNAMES_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    NODENAME_FIELD_NUMBER: _ClassVar[int]
    dataType: DataType
    fieldNames: _containers.RepeatedScalarFieldContainer[str]
    key: str
    nodeName: str
    def __init__(self, nodeName: _Optional[str] = ..., key: _Optional[str] = ..., fieldNames: _Optional[_Iterable[str]] = ..., dataType: _Optional[_Union[DataType, str]] = ...) -> None: ...

class AttributesReadRangeFieldsRequest(_message.Message):
    __slots__ = ["dataType", "endKey", "key", "nodeName", "startKey"]
    DATATYPE_FIELD_NUMBER: _ClassVar[int]
    ENDKEY_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    NODENAME_FIELD_NUMBER: _ClassVar[int]
    STARTKEY_FIELD_NUMBER: _ClassVar[int]
    dataType: DataType
    endKey: str
    key: str
    nodeName: str
    startKey: str
    def __init__(self, nodeName: _Optional[str] = ..., key: _Optional[str] = ..., startKey: _Optional[str] = ..., endKey: _Optional[str] = ..., dataType: _Optional[_Union[DataType, str]] = ...) -> None: ...

class AttributesReadRequest(_message.Message):
    __slots__ = ["dataType", "fieldName", "key", "nodeName"]
    DATATYPE_FIELD_NUMBER: _ClassVar[int]
    FIELDNAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    NODENAME_FIELD_NUMBER: _ClassVar[int]
    dataType: DataType
    fieldName: str
    key: str
    nodeName: str
    def __init__(self, nodeName: _Optional[str] = ..., key: _Optional[str] = ..., fieldName: _Optional[str] = ..., dataType: _Optional[_Union[DataType, str]] = ...) -> None: ...

class AttributesReadResponse(_message.Message):
    __slots__ = ["dataType", "message", "status", "value"]
    DATATYPE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    dataType: DataType
    message: str
    status: bool
    value: str
    def __init__(self, value: _Optional[str] = ..., dataType: _Optional[_Union[DataType, str]] = ..., status: bool = ..., message: _Optional[str] = ...) -> None: ...

class AttributesWriteRequest(_message.Message):
    __slots__ = ["dataType", "fieldName", "key", "nodeName", "ttl", "value"]
    DATATYPE_FIELD_NUMBER: _ClassVar[int]
    FIELDNAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    NODENAME_FIELD_NUMBER: _ClassVar[int]
    TTL_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    dataType: DataType
    fieldName: str
    key: str
    nodeName: str
    ttl: int
    value: str
    def __init__(self, nodeName: _Optional[str] = ..., key: _Optional[str] = ..., fieldName: _Optional[str] = ..., value: _Optional[str] = ..., dataType: _Optional[_Union[DataType, str]] = ..., ttl: _Optional[int] = ...) -> None: ...

class BulkOpCompletionResponse(_message.Message):
    __slots__ = ["message", "status"]
    class StatusEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bool
        def __init__(self, key: _Optional[str] = ..., value: bool = ...) -> None: ...
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    message: str
    status: _containers.ScalarMap[str, bool]
    def __init__(self, status: _Optional[_Mapping[str, bool]] = ..., message: _Optional[str] = ...) -> None: ...

class CompletionResponse(_message.Message):
    __slots__ = ["message", "status"]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    message: str
    status: bool
    def __init__(self, status: bool = ..., message: _Optional[str] = ...) -> None: ...

class CompletionStatus(_message.Message):
    __slots__ = ["succeeded"]
    SUCCEEDED_FIELD_NUMBER: _ClassVar[int]
    succeeded: bool
    def __init__(self, succeeded: bool = ...) -> None: ...

class ErrorRequest(_message.Message):
    __slots__ = ["details", "message", "nodeName", "object", "schemaId", "stackTrace"]
    DETAILS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    NODENAME_FIELD_NUMBER: _ClassVar[int]
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    SCHEMAID_FIELD_NUMBER: _ClassVar[int]
    STACKTRACE_FIELD_NUMBER: _ClassVar[int]
    details: str
    message: str
    nodeName: str
    object: str
    schemaId: str
    stackTrace: str
    def __init__(self, nodeName: _Optional[str] = ..., schemaId: _Optional[str] = ..., object: _Optional[str] = ..., message: _Optional[str] = ..., details: _Optional[str] = ..., stackTrace: _Optional[str] = ...) -> None: ...

class ErrorResponse(_message.Message):
    __slots__ = ["status"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: bool
    def __init__(self, status: bool = ...) -> None: ...

class LookupBulkDeleteRequest(_message.Message):
    __slots__ = ["keys", "nodeName"]
    KEYS_FIELD_NUMBER: _ClassVar[int]
    NODENAME_FIELD_NUMBER: _ClassVar[int]
    keys: _containers.RepeatedScalarFieldContainer[str]
    nodeName: str
    def __init__(self, nodeName: _Optional[str] = ..., keys: _Optional[_Iterable[str]] = ...) -> None: ...

class LookupBulkReadRequest(_message.Message):
    __slots__ = ["dataType", "keys", "nodeName"]
    DATATYPE_FIELD_NUMBER: _ClassVar[int]
    KEYS_FIELD_NUMBER: _ClassVar[int]
    NODENAME_FIELD_NUMBER: _ClassVar[int]
    dataType: DataType
    keys: _containers.RepeatedScalarFieldContainer[str]
    nodeName: str
    def __init__(self, nodeName: _Optional[str] = ..., keys: _Optional[_Iterable[str]] = ..., dataType: _Optional[_Union[DataType, str]] = ...) -> None: ...

class LookupBulkWriteRequest(_message.Message):
    __slots__ = ["dataType", "keyValueCollection", "nodeName", "ttl"]
    class KeyValueCollectionEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    DATATYPE_FIELD_NUMBER: _ClassVar[int]
    KEYVALUECOLLECTION_FIELD_NUMBER: _ClassVar[int]
    NODENAME_FIELD_NUMBER: _ClassVar[int]
    TTL_FIELD_NUMBER: _ClassVar[int]
    dataType: DataType
    keyValueCollection: _containers.ScalarMap[str, str]
    nodeName: str
    ttl: int
    def __init__(self, nodeName: _Optional[str] = ..., keyValueCollection: _Optional[_Mapping[str, str]] = ..., dataType: _Optional[_Union[DataType, str]] = ..., ttl: _Optional[int] = ...) -> None: ...

class LookupDeleteRequest(_message.Message):
    __slots__ = ["key", "nodeName"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    NODENAME_FIELD_NUMBER: _ClassVar[int]
    key: str
    nodeName: str
    def __init__(self, nodeName: _Optional[str] = ..., key: _Optional[str] = ...) -> None: ...

class LookupReadRequest(_message.Message):
    __slots__ = ["dataType", "key", "nodeName"]
    DATATYPE_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    NODENAME_FIELD_NUMBER: _ClassVar[int]
    dataType: DataType
    key: str
    nodeName: str
    def __init__(self, nodeName: _Optional[str] = ..., key: _Optional[str] = ..., dataType: _Optional[_Union[DataType, str]] = ...) -> None: ...

class LookupReadResponse(_message.Message):
    __slots__ = ["dataType", "message", "status", "value"]
    DATATYPE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    dataType: DataType
    message: str
    status: bool
    value: str
    def __init__(self, value: _Optional[str] = ..., dataType: _Optional[_Union[DataType, str]] = ..., status: bool = ..., message: _Optional[str] = ...) -> None: ...

class LookupWriteRequest(_message.Message):
    __slots__ = ["dataType", "key", "nodeName", "ttl", "value"]
    DATATYPE_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    NODENAME_FIELD_NUMBER: _ClassVar[int]
    TTL_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    dataType: DataType
    key: str
    nodeName: str
    ttl: int
    value: str
    def __init__(self, nodeName: _Optional[str] = ..., key: _Optional[str] = ..., value: _Optional[str] = ..., dataType: _Optional[_Union[DataType, str]] = ..., ttl: _Optional[int] = ...) -> None: ...

class ShuriOfsDeleteFeatureFieldRequest(_message.Message):
    __slots__ = ["entityId", "featureName", "fieldName", "nodeName"]
    ENTITYID_FIELD_NUMBER: _ClassVar[int]
    FEATURENAME_FIELD_NUMBER: _ClassVar[int]
    FIELDNAME_FIELD_NUMBER: _ClassVar[int]
    NODENAME_FIELD_NUMBER: _ClassVar[int]
    entityId: str
    featureName: str
    fieldName: str
    nodeName: str
    def __init__(self, nodeName: _Optional[str] = ..., featureName: _Optional[str] = ..., entityId: _Optional[str] = ..., fieldName: _Optional[str] = ...) -> None: ...

class ShuriOfsReadFeatureGroupsRequest(_message.Message):
    __slots__ = ["entityIds", "featureName", "nodeName"]
    ENTITYIDS_FIELD_NUMBER: _ClassVar[int]
    FEATURENAME_FIELD_NUMBER: _ClassVar[int]
    NODENAME_FIELD_NUMBER: _ClassVar[int]
    entityIds: _containers.RepeatedScalarFieldContainer[str]
    featureName: str
    nodeName: str
    def __init__(self, nodeName: _Optional[str] = ..., featureName: _Optional[str] = ..., entityIds: _Optional[_Iterable[str]] = ...) -> None: ...

class ShuriOfsReadFeatureRequest(_message.Message):
    __slots__ = ["entityId", "featureName", "fieldName", "nodeName"]
    ENTITYID_FIELD_NUMBER: _ClassVar[int]
    FEATURENAME_FIELD_NUMBER: _ClassVar[int]
    FIELDNAME_FIELD_NUMBER: _ClassVar[int]
    NODENAME_FIELD_NUMBER: _ClassVar[int]
    entityId: str
    featureName: str
    fieldName: str
    nodeName: str
    def __init__(self, nodeName: _Optional[str] = ..., featureName: _Optional[str] = ..., entityId: _Optional[str] = ..., fieldName: _Optional[str] = ...) -> None: ...

class ShuriOfsReadResponse(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...

class ShuriOfsWriteFeatureGroupRequest(_message.Message):
    __slots__ = ["data", "entityId", "featureName", "nodeName", "ttl"]
    class DataEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    DATA_FIELD_NUMBER: _ClassVar[int]
    ENTITYID_FIELD_NUMBER: _ClassVar[int]
    FEATURENAME_FIELD_NUMBER: _ClassVar[int]
    NODENAME_FIELD_NUMBER: _ClassVar[int]
    TTL_FIELD_NUMBER: _ClassVar[int]
    data: _containers.ScalarMap[str, str]
    entityId: str
    featureName: str
    nodeName: str
    ttl: int
    def __init__(self, nodeName: _Optional[str] = ..., featureName: _Optional[str] = ..., entityId: _Optional[str] = ..., data: _Optional[_Mapping[str, str]] = ..., ttl: _Optional[int] = ...) -> None: ...

class DataType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
