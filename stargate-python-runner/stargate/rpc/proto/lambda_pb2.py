# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: lambda.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0clambda.proto\"3\n\x0bInitPayload\x12\x10\n\x08nodeName\x18\x01 \x01(\t\x12\x12\n\nnodeConfig\x18\x02 \x01(\t\"-\n\nInitStatus\x12\x0e\n\x06status\x18\x01 \x01(\x08\x12\x0f\n\x07message\x18\x02 \x01(\t\"\xc1\x01\n\x0eRequestPayload\x12\x10\n\x08nodeName\x18\x01 \x01(\t\x12\x10\n\x08schemaId\x18\x02 \x01(\t\x12\x0f\n\x07payload\x18\x03 \x01(\t\x12\x12\n\npayloadKey\x18\x04 \x01(\t\x12\x33\n\nlogContext\x18\x05 \x03(\x0b\x32\x1f.RequestPayload.LogContextEntry\x1a\x31\n\x0fLogContextEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"X\n\x0fResponsePayload\x12\x10\n\x08response\x18\x01 \x01(\t\x12\x0c\n\x04skip\x18\x02 \x01(\x08\x12\x11\n\texception\x18\x03 \x01(\t\x12\x12\n\nstackTrace\x18\x04 \x01(\t\"\x14\n\x04\x45poc\x12\x0c\n\x04time\x18\x01 \x01(\x03\"#\n\x0fMetricsResponse\x12\x10\n\x08response\x18\x01 \x01(\t2\x97\x01\n\x1c\x45xternalFunctionProtoService\x12#\n\x04init\x12\x0c.InitPayload\x1a\x0b.InitStatus\"\x00\x12,\n\x05\x61pply\x12\x0f.RequestPayload\x1a\x10.ResponsePayload\"\x00\x12$\n\x07metrics\x12\x05.Epoc\x1a\x10.MetricsResponse\"\x00\x42-\n)com.apple.aml.stargate.external.rpc.protoP\x01\x62\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'lambda_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'\n)com.apple.aml.stargate.external.rpc.protoP\001'
  _REQUESTPAYLOAD_LOGCONTEXTENTRY._options = None
  _REQUESTPAYLOAD_LOGCONTEXTENTRY._serialized_options = b'8\001'
  _INITPAYLOAD._serialized_start=16
  _INITPAYLOAD._serialized_end=67
  _INITSTATUS._serialized_start=69
  _INITSTATUS._serialized_end=114
  _REQUESTPAYLOAD._serialized_start=117
  _REQUESTPAYLOAD._serialized_end=310
  _REQUESTPAYLOAD_LOGCONTEXTENTRY._serialized_start=261
  _REQUESTPAYLOAD_LOGCONTEXTENTRY._serialized_end=310
  _RESPONSEPAYLOAD._serialized_start=312
  _RESPONSEPAYLOAD._serialized_end=400
  _EPOC._serialized_start=402
  _EPOC._serialized_end=422
  _METRICSRESPONSE._serialized_start=424
  _METRICSRESPONSE._serialized_end=459
  _EXTERNALFUNCTIONPROTOSERVICE._serialized_start=462
  _EXTERNALFUNCTIONPROTOSERVICE._serialized_end=613
# @@protoc_insertion_point(module_scope)
