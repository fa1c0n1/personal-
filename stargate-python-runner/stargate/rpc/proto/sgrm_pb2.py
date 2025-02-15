# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: sgrm.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\nsgrm.proto\"c\n\x0e\x41\x33TokenRequest\x12\x13\n\x0bsourceAppId\x18\x01 \x01(\x03\x12\x13\n\x0btargetAppId\x18\x02 \x01(\x03\x12\x10\n\x08password\x18\x03 \x01(\t\x12\x15\n\rcontextString\x18\x04 \x01(\t\"0\n\x0f\x41\x33TokenResponse\x12\r\n\x05token\x18\x01 \x01(\t\x12\x0e\n\x06\x65xpiry\x18\x02 \x01(\x03\"6\n\x16\x41\x33TokenValidateRequest\x12\r\n\x05\x61ppId\x18\x01 \x01(\x03\x12\r\n\x05token\x18\x02 \x01(\t\"9\n\x17\x41\x33TokenValidateResponse\x12\r\n\x05valid\x18\x01 \x01(\x08\x12\x0f\n\x07message\x18\x02 \x01(\t\"x\n\x0c\x45rrorRequest\x12\x10\n\x08nodeName\x18\x01 \x01(\t\x12\x10\n\x08schemaId\x18\x02 \x01(\t\x12\x0e\n\x06object\x18\x03 \x01(\t\x12\x0f\n\x07message\x18\x04 \x01(\t\x12\x0f\n\x07\x64\x65tails\x18\x05 \x01(\t\x12\x12\n\nstackTrace\x18\x06 \x01(\t\"\x1f\n\rErrorResponse\x12\x0e\n\x06status\x18\x01 \x01(\x08\"f\n\x15\x41ttributesReadRequest\x12\x10\n\x08nodeName\x18\x01 \x01(\t\x12\x0b\n\x03key\x18\x02 \x01(\t\x12\x11\n\tfieldName\x18\x03 \x01(\t\x12\x1b\n\x08\x64\x61taType\x18\x04 \x01(\x0e\x32\t.DataType\"k\n\x19\x41ttributesBulkReadRequest\x12\x10\n\x08nodeName\x18\x01 \x01(\t\x12\x0c\n\x04keys\x18\x02 \x03(\t\x12\x11\n\tfieldName\x18\x03 \x01(\t\x12\x1b\n\x08\x64\x61taType\x18\x04 \x01(\x0e\x32\t.DataType\"m\n\x1b\x41ttributesReadFieldsRequest\x12\x10\n\x08nodeName\x18\x01 \x01(\t\x12\x0b\n\x03key\x18\x02 \x01(\t\x12\x12\n\nfieldNames\x18\x03 \x03(\t\x12\x1b\n\x08\x64\x61taType\x18\x04 \x01(\x0e\x32\t.DataType\"\x80\x01\n AttributesReadRangeFieldsRequest\x12\x10\n\x08nodeName\x18\x01 \x01(\t\x12\x0b\n\x03key\x18\x02 \x01(\t\x12\x10\n\x08startKey\x18\x03 \x01(\t\x12\x0e\n\x06\x65ndKey\x18\x04 \x01(\t\x12\x1b\n\x08\x64\x61taType\x18\x05 \x01(\x0e\x32\t.DataType\"e\n\x16\x41ttributesReadResponse\x12\r\n\x05value\x18\x01 \x01(\t\x12\x1b\n\x08\x64\x61taType\x18\x02 \x01(\x0e\x32\t.DataType\x12\x0e\n\x06status\x18\x03 \x01(\x08\x12\x0f\n\x07message\x18\x04 \x01(\t\"\x83\x01\n\x16\x41ttributesWriteRequest\x12\x10\n\x08nodeName\x18\x01 \x01(\t\x12\x0b\n\x03key\x18\x02 \x01(\t\x12\x11\n\tfieldName\x18\x03 \x01(\t\x12\r\n\x05value\x18\x04 \x01(\t\x12\x1b\n\x08\x64\x61taType\x18\x05 \x01(\x0e\x32\t.DataType\x12\x0b\n\x03ttl\x18\x06 \x01(\x05\"\xf7\x01\n\x1a\x41ttributesBulkWriteRequest\x12\x10\n\x08nodeName\x18\x01 \x01(\t\x12O\n\x12keyValueCollection\x18\x02 \x03(\x0b\x32\x33.AttributesBulkWriteRequest.KeyValueCollectionEntry\x12\x11\n\tfieldName\x18\x03 \x01(\t\x12\x1b\n\x08\x64\x61taType\x18\x04 \x01(\x0e\x32\t.DataType\x12\x0b\n\x03ttl\x18\x05 \x01(\x05\x1a\x39\n\x17KeyValueCollectionEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"L\n\x17\x41ttributesDeleteRequest\x12\x10\n\x08nodeName\x18\x01 \x01(\t\x12\x0b\n\x03key\x18\x02 \x01(\t\x12\x12\n\nfieldNames\x18\x03 \x03(\t\"Q\n\x1b\x41ttributesBulkDeleteRequest\x12\x10\n\x08nodeName\x18\x01 \x01(\t\x12\x0c\n\x04keys\x18\x02 \x03(\t\x12\x12\n\nfieldNames\x18\x03 \x03(\t\"5\n\x12\x43ompletionResponse\x12\x0e\n\x06status\x18\x01 \x01(\x08\x12\x0f\n\x07message\x18\x02 \x01(\t\"\x91\x01\n\x18\x42ulkOpCompletionResponse\x12\x35\n\x06status\x18\x01 \x03(\x0b\x32%.BulkOpCompletionResponse.StatusEntry\x12\x0f\n\x07message\x18\x02 \x01(\t\x1a-\n\x0bStatusEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x08:\x02\x38\x01\"O\n\x11LookupReadRequest\x12\x10\n\x08nodeName\x18\x01 \x01(\t\x12\x0b\n\x03key\x18\x02 \x01(\t\x12\x1b\n\x08\x64\x61taType\x18\x03 \x01(\x0e\x32\t.DataType\"T\n\x15LookupBulkReadRequest\x12\x10\n\x08nodeName\x18\x01 \x01(\t\x12\x0c\n\x04keys\x18\x02 \x03(\t\x12\x1b\n\x08\x64\x61taType\x18\x03 \x01(\x0e\x32\t.DataType\"a\n\x12LookupReadResponse\x12\r\n\x05value\x18\x01 \x01(\t\x12\x1b\n\x08\x64\x61taType\x18\x02 \x01(\x0e\x32\t.DataType\x12\x0e\n\x06status\x18\x03 \x01(\x08\x12\x0f\n\x07message\x18\x04 \x01(\t\"l\n\x12LookupWriteRequest\x12\x10\n\x08nodeName\x18\x01 \x01(\t\x12\x0b\n\x03key\x18\x02 \x01(\t\x12\r\n\x05value\x18\x03 \x01(\t\x12\x1b\n\x08\x64\x61taType\x18\x04 \x01(\x0e\x32\t.DataType\x12\x0b\n\x03ttl\x18\x05 \x01(\x05\"\xdc\x01\n\x16LookupBulkWriteRequest\x12\x10\n\x08nodeName\x18\x01 \x01(\t\x12K\n\x12keyValueCollection\x18\x02 \x03(\x0b\x32/.LookupBulkWriteRequest.KeyValueCollectionEntry\x12\x1b\n\x08\x64\x61taType\x18\x03 \x01(\x0e\x32\t.DataType\x12\x0b\n\x03ttl\x18\x04 \x01(\x05\x1a\x39\n\x17KeyValueCollectionEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"4\n\x13LookupDeleteRequest\x12\x10\n\x08nodeName\x18\x01 \x01(\t\x12\x0b\n\x03key\x18\x02 \x01(\t\"9\n\x17LookupBulkDeleteRequest\x12\x10\n\x08nodeName\x18\x01 \x01(\t\x12\x0c\n\x04keys\x18\x02 \x03(\t\"h\n\x1aShuriOfsReadFeatureRequest\x12\x10\n\x08nodeName\x18\x01 \x01(\t\x12\x13\n\x0b\x66\x65\x61tureName\x18\x02 \x01(\t\x12\x10\n\x08\x65ntityId\x18\x03 \x01(\t\x12\x11\n\tfieldName\x18\x04 \x01(\t\"\\\n ShuriOfsReadFeatureGroupsRequest\x12\x10\n\x08nodeName\x18\x01 \x01(\t\x12\x13\n\x0b\x66\x65\x61tureName\x18\x02 \x01(\t\x12\x11\n\tentityIds\x18\x03 \x03(\t\"%\n\x14ShuriOfsReadResponse\x12\r\n\x05value\x18\x01 \x01(\t\"\xd0\x01\n ShuriOfsWriteFeatureGroupRequest\x12\x10\n\x08nodeName\x18\x01 \x01(\t\x12\x13\n\x0b\x66\x65\x61tureName\x18\x02 \x01(\t\x12\x10\n\x08\x65ntityId\x18\x03 \x01(\t\x12\x39\n\x04\x64\x61ta\x18\x04 \x03(\x0b\x32+.ShuriOfsWriteFeatureGroupRequest.DataEntry\x12\x0b\n\x03ttl\x18\x05 \x01(\x05\x1a+\n\tDataEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"o\n!ShuriOfsDeleteFeatureFieldRequest\x12\x10\n\x08nodeName\x18\x01 \x01(\t\x12\x13\n\x0b\x66\x65\x61tureName\x18\x02 \x01(\t\x12\x10\n\x08\x65ntityId\x18\x03 \x01(\t\x12\x11\n\tfieldName\x18\x04 \x01(\t\"%\n\x10\x43ompletionStatus\x12\x11\n\tsucceeded\x18\x01 \x01(\x08*m\n\x08\x44\x61taType\x12\x07\n\x03MAP\x10\x00\x12\x08\n\x04LIST\x10\x01\x12\x07\n\x03SET\x10\x02\x12\t\n\x05TUPLE\x10\x03\x12\x0b\n\x07INTEGER\x10\x04\x12\t\n\x05\x46LOAT\x10\x05\x12\x0b\n\x07\x42OOLEAN\x10\x06\x12\n\n\x06STRING\x10\x07\x12\t\n\x05OTHER\x10\x08\x32\x9e\r\n\x1bResourceManagerProtoService\x12\x33\n\x0c\x66\x65tchA3Token\x12\x0f.A3TokenRequest\x1a\x10.A3TokenResponse\"\x00\x12\x46\n\x0fvalidateA3Token\x12\x17.A3TokenValidateRequest\x1a\x18.A3TokenValidateResponse\"\x00\x12/\n\x0cpublishError\x12\r.ErrorRequest\x1a\x0e.ErrorResponse\"\x00\x12\x43\n\x0ereadAttributes\x12\x16.AttributesReadRequest\x1a\x17.AttributesReadResponse\"\x00\x12K\n\x12\x62ulkReadAttributes\x12\x1a.AttributesBulkReadRequest\x1a\x17.AttributesReadResponse\"\x00\x12O\n\x14readAttributesFields\x12\x1c.AttributesReadFieldsRequest\x1a\x17.AttributesReadResponse\"\x00\x12Y\n\x19readAttributesRangeFields\x12!.AttributesReadRangeFieldsRequest\x1a\x17.AttributesReadResponse\"\x00\x12\x41\n\x0fwriteAttributes\x12\x17.AttributesWriteRequest\x1a\x13.CompletionResponse\"\x00\x12O\n\x13\x62ulkWriteAttributes\x12\x1b.AttributesBulkWriteRequest\x1a\x19.BulkOpCompletionResponse\"\x00\x12\x46\n\x14writeAttributesField\x12\x17.AttributesWriteRequest\x1a\x13.CompletionResponse\"\x00\x12G\n\x15writeAttributesFields\x12\x17.AttributesWriteRequest\x1a\x13.CompletionResponse\"\x00\x12\x43\n\x10\x64\x65leteAttributes\x12\x18.AttributesDeleteRequest\x1a\x13.CompletionResponse\"\x00\x12Q\n\x14\x62ulkDeleteAttributes\x12\x1c.AttributesBulkDeleteRequest\x1a\x19.BulkOpCompletionResponse\"\x00\x12\x37\n\nreadLookup\x12\x12.LookupReadRequest\x1a\x13.LookupReadResponse\"\x00\x12?\n\x0e\x62ulkReadLookup\x12\x16.LookupBulkReadRequest\x1a\x13.LookupReadResponse\"\x00\x12\x39\n\x0bwriteLookup\x12\x13.LookupWriteRequest\x1a\x13.CompletionResponse\"\x00\x12G\n\x0f\x62ulkWriteLookup\x12\x17.LookupBulkWriteRequest\x1a\x19.BulkOpCompletionResponse\"\x00\x12;\n\x0c\x64\x65leteLookup\x12\x14.LookupDeleteRequest\x1a\x13.CompletionResponse\"\x00\x12I\n\x10\x62ulkDeleteLookup\x12\x18.LookupBulkDeleteRequest\x1a\x19.BulkOpCompletionResponse\"\x00\x12K\n\x13readShuriOfsFeature\x12\x1b.ShuriOfsReadFeatureRequest\x1a\x15.ShuriOfsReadResponse\"\x00\x12W\n\x19readShuriOfsFeatureGroups\x12!.ShuriOfsReadFeatureGroupsRequest\x1a\x15.ShuriOfsReadResponse\"\x00\x12S\n\x19writeShuriOfsFeatureGroup\x12!.ShuriOfsWriteFeatureGroupRequest\x1a\x11.CompletionStatus\"\x00\x12U\n\x1a\x64\x65leteShuriOfsFeatureField\x12\".ShuriOfsDeleteFeatureFieldRequest\x1a\x11.CompletionStatus\"\x00\x42\'\n#com.apple.aml.stargate.rm.rpc.protoP\x01\x62\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'sgrm_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'\n#com.apple.aml.stargate.rm.rpc.protoP\001'
  _ATTRIBUTESBULKWRITEREQUEST_KEYVALUECOLLECTIONENTRY._options = None
  _ATTRIBUTESBULKWRITEREQUEST_KEYVALUECOLLECTIONENTRY._serialized_options = b'8\001'
  _BULKOPCOMPLETIONRESPONSE_STATUSENTRY._options = None
  _BULKOPCOMPLETIONRESPONSE_STATUSENTRY._serialized_options = b'8\001'
  _LOOKUPBULKWRITEREQUEST_KEYVALUECOLLECTIONENTRY._options = None
  _LOOKUPBULKWRITEREQUEST_KEYVALUECOLLECTIONENTRY._serialized_options = b'8\001'
  _SHURIOFSWRITEFEATUREGROUPREQUEST_DATAENTRY._options = None
  _SHURIOFSWRITEFEATUREGROUPREQUEST_DATAENTRY._serialized_options = b'8\001'
  _DATATYPE._serialized_start=3055
  _DATATYPE._serialized_end=3164
  _A3TOKENREQUEST._serialized_start=14
  _A3TOKENREQUEST._serialized_end=113
  _A3TOKENRESPONSE._serialized_start=115
  _A3TOKENRESPONSE._serialized_end=163
  _A3TOKENVALIDATEREQUEST._serialized_start=165
  _A3TOKENVALIDATEREQUEST._serialized_end=219
  _A3TOKENVALIDATERESPONSE._serialized_start=221
  _A3TOKENVALIDATERESPONSE._serialized_end=278
  _ERRORREQUEST._serialized_start=280
  _ERRORREQUEST._serialized_end=400
  _ERRORRESPONSE._serialized_start=402
  _ERRORRESPONSE._serialized_end=433
  _ATTRIBUTESREADREQUEST._serialized_start=435
  _ATTRIBUTESREADREQUEST._serialized_end=537
  _ATTRIBUTESBULKREADREQUEST._serialized_start=539
  _ATTRIBUTESBULKREADREQUEST._serialized_end=646
  _ATTRIBUTESREADFIELDSREQUEST._serialized_start=648
  _ATTRIBUTESREADFIELDSREQUEST._serialized_end=757
  _ATTRIBUTESREADRANGEFIELDSREQUEST._serialized_start=760
  _ATTRIBUTESREADRANGEFIELDSREQUEST._serialized_end=888
  _ATTRIBUTESREADRESPONSE._serialized_start=890
  _ATTRIBUTESREADRESPONSE._serialized_end=991
  _ATTRIBUTESWRITEREQUEST._serialized_start=994
  _ATTRIBUTESWRITEREQUEST._serialized_end=1125
  _ATTRIBUTESBULKWRITEREQUEST._serialized_start=1128
  _ATTRIBUTESBULKWRITEREQUEST._serialized_end=1375
  _ATTRIBUTESBULKWRITEREQUEST_KEYVALUECOLLECTIONENTRY._serialized_start=1318
  _ATTRIBUTESBULKWRITEREQUEST_KEYVALUECOLLECTIONENTRY._serialized_end=1375
  _ATTRIBUTESDELETEREQUEST._serialized_start=1377
  _ATTRIBUTESDELETEREQUEST._serialized_end=1453
  _ATTRIBUTESBULKDELETEREQUEST._serialized_start=1455
  _ATTRIBUTESBULKDELETEREQUEST._serialized_end=1536
  _COMPLETIONRESPONSE._serialized_start=1538
  _COMPLETIONRESPONSE._serialized_end=1591
  _BULKOPCOMPLETIONRESPONSE._serialized_start=1594
  _BULKOPCOMPLETIONRESPONSE._serialized_end=1739
  _BULKOPCOMPLETIONRESPONSE_STATUSENTRY._serialized_start=1694
  _BULKOPCOMPLETIONRESPONSE_STATUSENTRY._serialized_end=1739
  _LOOKUPREADREQUEST._serialized_start=1741
  _LOOKUPREADREQUEST._serialized_end=1820
  _LOOKUPBULKREADREQUEST._serialized_start=1822
  _LOOKUPBULKREADREQUEST._serialized_end=1906
  _LOOKUPREADRESPONSE._serialized_start=1908
  _LOOKUPREADRESPONSE._serialized_end=2005
  _LOOKUPWRITEREQUEST._serialized_start=2007
  _LOOKUPWRITEREQUEST._serialized_end=2115
  _LOOKUPBULKWRITEREQUEST._serialized_start=2118
  _LOOKUPBULKWRITEREQUEST._serialized_end=2338
  _LOOKUPBULKWRITEREQUEST_KEYVALUECOLLECTIONENTRY._serialized_start=1318
  _LOOKUPBULKWRITEREQUEST_KEYVALUECOLLECTIONENTRY._serialized_end=1375
  _LOOKUPDELETEREQUEST._serialized_start=2340
  _LOOKUPDELETEREQUEST._serialized_end=2392
  _LOOKUPBULKDELETEREQUEST._serialized_start=2394
  _LOOKUPBULKDELETEREQUEST._serialized_end=2451
  _SHURIOFSREADFEATUREREQUEST._serialized_start=2453
  _SHURIOFSREADFEATUREREQUEST._serialized_end=2557
  _SHURIOFSREADFEATUREGROUPSREQUEST._serialized_start=2559
  _SHURIOFSREADFEATUREGROUPSREQUEST._serialized_end=2651
  _SHURIOFSREADRESPONSE._serialized_start=2653
  _SHURIOFSREADRESPONSE._serialized_end=2690
  _SHURIOFSWRITEFEATUREGROUPREQUEST._serialized_start=2693
  _SHURIOFSWRITEFEATUREGROUPREQUEST._serialized_end=2901
  _SHURIOFSWRITEFEATUREGROUPREQUEST_DATAENTRY._serialized_start=2858
  _SHURIOFSWRITEFEATUREGROUPREQUEST_DATAENTRY._serialized_end=2901
  _SHURIOFSDELETEFEATUREFIELDREQUEST._serialized_start=2903
  _SHURIOFSDELETEFEATUREFIELDREQUEST._serialized_end=3014
  _COMPLETIONSTATUS._serialized_start=3016
  _COMPLETIONSTATUS._serialized_end=3053
  _RESOURCEMANAGERPROTOSERVICE._serialized_start=3167
  _RESOURCEMANAGERPROTOSERVICE._serialized_end=4861
# @@protoc_insertion_point(module_scope)
