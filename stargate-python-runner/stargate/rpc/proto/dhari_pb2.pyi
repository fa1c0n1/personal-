from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class DhariPing(_message.Message):
    __slots__ = ["time"]
    TIME_FIELD_NUMBER: _ClassVar[int]
    time: int
    def __init__(self, time: _Optional[int] = ...) -> None: ...

class DhariStatus(_message.Message):
    __slots__ = ["status"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: bool
    def __init__(self, status: bool = ...) -> None: ...

class EAILinesPayload(_message.Message):
    __slots__ = ["context", "count", "lineIndex", "lines", "recordIndex", "token"]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    LINEINDEX_FIELD_NUMBER: _ClassVar[int]
    LINES_FIELD_NUMBER: _ClassVar[int]
    RECORDINDEX_FIELD_NUMBER: _ClassVar[int]
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    context: str
    count: int
    lineIndex: int
    lines: str
    recordIndex: int
    token: str
    def __init__(self, token: _Optional[str] = ..., context: _Optional[str] = ..., count: _Optional[int] = ..., lines: _Optional[str] = ..., lineIndex: _Optional[int] = ..., recordIndex: _Optional[int] = ...) -> None: ...

class FilePayload(_message.Message):
    __slots__ = ["appId", "blockAmount", "blockNo", "fileExt", "filePayload", "firstBlockPresentBool", "payload", "payloadKey", "publisherId", "schemaId", "token", "totalBlocks", "totalSize", "txnId"]
    APPID_FIELD_NUMBER: _ClassVar[int]
    BLOCKAMOUNT_FIELD_NUMBER: _ClassVar[int]
    BLOCKNO_FIELD_NUMBER: _ClassVar[int]
    FILEEXT_FIELD_NUMBER: _ClassVar[int]
    FILEPAYLOAD_FIELD_NUMBER: _ClassVar[int]
    FIRSTBLOCKPRESENTBOOL_FIELD_NUMBER: _ClassVar[int]
    PAYLOADKEY_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    PUBLISHERID_FIELD_NUMBER: _ClassVar[int]
    SCHEMAID_FIELD_NUMBER: _ClassVar[int]
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    TOTALBLOCKS_FIELD_NUMBER: _ClassVar[int]
    TOTALSIZE_FIELD_NUMBER: _ClassVar[int]
    TXNID_FIELD_NUMBER: _ClassVar[int]
    appId: int
    blockAmount: int
    blockNo: int
    fileExt: str
    filePayload: str
    firstBlockPresentBool: bool
    payload: str
    payloadKey: str
    publisherId: str
    schemaId: str
    token: str
    totalBlocks: int
    totalSize: int
    txnId: str
    def __init__(self, appId: _Optional[int] = ..., token: _Optional[str] = ..., publisherId: _Optional[str] = ..., schemaId: _Optional[str] = ..., payloadKey: _Optional[str] = ..., payload: _Optional[str] = ..., txnId: _Optional[str] = ..., filePayload: _Optional[str] = ..., totalBlocks: _Optional[int] = ..., totalSize: _Optional[int] = ..., blockAmount: _Optional[int] = ..., fileExt: _Optional[str] = ..., blockNo: _Optional[int] = ..., firstBlockPresentBool: bool = ...) -> None: ...

class IngestCount(_message.Message):
    __slots__ = ["count"]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    count: int
    def __init__(self, count: _Optional[int] = ...) -> None: ...

class JsonPayload(_message.Message):
    __slots__ = ["appId", "payload", "payloadKey", "publisherId", "schemaId", "token", "txnId", "unstructuredFieldName"]
    APPID_FIELD_NUMBER: _ClassVar[int]
    PAYLOADKEY_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    PUBLISHERID_FIELD_NUMBER: _ClassVar[int]
    SCHEMAID_FIELD_NUMBER: _ClassVar[int]
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    TXNID_FIELD_NUMBER: _ClassVar[int]
    UNSTRUCTUREDFIELDNAME_FIELD_NUMBER: _ClassVar[int]
    appId: int
    payload: str
    payloadKey: str
    publisherId: str
    schemaId: str
    token: str
    txnId: str
    unstructuredFieldName: str
    def __init__(self, appId: _Optional[int] = ..., token: _Optional[str] = ..., publisherId: _Optional[str] = ..., schemaId: _Optional[str] = ..., payloadKey: _Optional[str] = ..., payload: _Optional[str] = ..., txnId: _Optional[str] = ..., unstructuredFieldName: _Optional[str] = ...) -> None: ...

class JsonPayloadWithHeaders(_message.Message):
    __slots__ = ["appId", "headers", "payload", "payloadKey", "publisherId", "schemaId", "token", "txnId", "unstructuredFieldName"]
    class HeadersEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    APPID_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    PAYLOADKEY_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    PUBLISHERID_FIELD_NUMBER: _ClassVar[int]
    SCHEMAID_FIELD_NUMBER: _ClassVar[int]
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    TXNID_FIELD_NUMBER: _ClassVar[int]
    UNSTRUCTUREDFIELDNAME_FIELD_NUMBER: _ClassVar[int]
    appId: int
    headers: _containers.ScalarMap[str, str]
    payload: str
    payloadKey: str
    publisherId: str
    schemaId: str
    token: str
    txnId: str
    unstructuredFieldName: str
    def __init__(self, appId: _Optional[int] = ..., token: _Optional[str] = ..., publisherId: _Optional[str] = ..., schemaId: _Optional[str] = ..., payloadKey: _Optional[str] = ..., payload: _Optional[str] = ..., txnId: _Optional[str] = ..., unstructuredFieldName: _Optional[str] = ..., headers: _Optional[_Mapping[str, str]] = ...) -> None: ...

class PayloadResponse(_message.Message):
    __slots__ = ["count", "exception", "payloadKey", "responseJson"]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    EXCEPTION_FIELD_NUMBER: _ClassVar[int]
    PAYLOADKEY_FIELD_NUMBER: _ClassVar[int]
    RESPONSEJSON_FIELD_NUMBER: _ClassVar[int]
    count: int
    exception: str
    payloadKey: str
    responseJson: str
    def __init__(self, count: _Optional[int] = ..., payloadKey: _Optional[str] = ..., responseJson: _Optional[str] = ..., exception: _Optional[str] = ...) -> None: ...

class PipelinePayload(_message.Message):
    __slots__ = ["appId", "payload", "payloadKey", "pipelineId", "pipelineToken", "publishFormat", "publisherId", "schemaId"]
    APPID_FIELD_NUMBER: _ClassVar[int]
    PAYLOADKEY_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    PIPELINEID_FIELD_NUMBER: _ClassVar[int]
    PIPELINETOKEN_FIELD_NUMBER: _ClassVar[int]
    PUBLISHERID_FIELD_NUMBER: _ClassVar[int]
    PUBLISHFORMAT_FIELD_NUMBER: _ClassVar[int]
    SCHEMAID_FIELD_NUMBER: _ClassVar[int]
    appId: int
    payload: str
    payloadKey: str
    pipelineId: str
    pipelineToken: str
    publishFormat: str
    publisherId: str
    schemaId: str
    def __init__(self, appId: _Optional[int] = ..., pipelineId: _Optional[str] = ..., pipelineToken: _Optional[str] = ..., publisherId: _Optional[str] = ..., schemaId: _Optional[str] = ..., payloadKey: _Optional[str] = ..., payload: _Optional[str] = ..., publishFormat: _Optional[str] = ...) -> None: ...

class TxnDetails(_message.Message):
    __slots__ = ["txnId"]
    TXNID_FIELD_NUMBER: _ClassVar[int]
    txnId: str
    def __init__(self, txnId: _Optional[str] = ...) -> None: ...
