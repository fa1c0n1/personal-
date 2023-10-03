from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class Epoc(_message.Message):
    __slots__ = ["time"]
    TIME_FIELD_NUMBER: _ClassVar[int]
    time: int
    def __init__(self, time: _Optional[int] = ...) -> None: ...

class InitPayload(_message.Message):
    __slots__ = ["nodeConfig", "nodeName"]
    NODECONFIG_FIELD_NUMBER: _ClassVar[int]
    NODENAME_FIELD_NUMBER: _ClassVar[int]
    nodeConfig: str
    nodeName: str
    def __init__(self, nodeName: _Optional[str] = ..., nodeConfig: _Optional[str] = ...) -> None: ...

class InitStatus(_message.Message):
    __slots__ = ["message", "status"]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    message: str
    status: bool
    def __init__(self, status: bool = ..., message: _Optional[str] = ...) -> None: ...

class MetricsResponse(_message.Message):
    __slots__ = ["response"]
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    response: str
    def __init__(self, response: _Optional[str] = ...) -> None: ...

class RequestPayload(_message.Message):
    __slots__ = ["logContext", "nodeName", "payload", "payloadKey", "schemaId"]
    class LogContextEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    LOGCONTEXT_FIELD_NUMBER: _ClassVar[int]
    NODENAME_FIELD_NUMBER: _ClassVar[int]
    PAYLOADKEY_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    SCHEMAID_FIELD_NUMBER: _ClassVar[int]
    logContext: _containers.ScalarMap[str, str]
    nodeName: str
    payload: str
    payloadKey: str
    schemaId: str
    def __init__(self, nodeName: _Optional[str] = ..., schemaId: _Optional[str] = ..., payload: _Optional[str] = ..., payloadKey: _Optional[str] = ..., logContext: _Optional[_Mapping[str, str]] = ...) -> None: ...

class ResponsePayload(_message.Message):
    __slots__ = ["exception", "response", "skip", "stackTrace"]
    EXCEPTION_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    SKIP_FIELD_NUMBER: _ClassVar[int]
    STACKTRACE_FIELD_NUMBER: _ClassVar[int]
    exception: str
    response: str
    skip: bool
    stackTrace: str
    def __init__(self, response: _Optional[str] = ..., skip: bool = ..., exception: _Optional[str] = ..., stackTrace: _Optional[str] = ...) -> None: ...
