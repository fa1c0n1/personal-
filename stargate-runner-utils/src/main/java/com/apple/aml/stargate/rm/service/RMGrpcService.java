package com.apple.aml.stargate.rm.service;

import com.apple.aml.stargate.common.options.ExternalFunctionOptions;
import com.apple.aml.stargate.common.utils.A3Utils;
import com.apple.aml.stargate.common.utils.ClassUtils;
import com.apple.aml.stargate.connector.athena.attributes.AthenaAttributeService;
import com.apple.aml.stargate.rm.rpc.proto.A3TokenRequest;
import com.apple.aml.stargate.rm.rpc.proto.A3TokenResponse;
import com.apple.aml.stargate.rm.rpc.proto.A3TokenValidateRequest;
import com.apple.aml.stargate.rm.rpc.proto.A3TokenValidateResponse;
import com.apple.aml.stargate.rm.rpc.proto.AttributesBulkDeleteRequest;
import com.apple.aml.stargate.rm.rpc.proto.AttributesBulkReadRequest;
import com.apple.aml.stargate.rm.rpc.proto.AttributesBulkWriteRequest;
import com.apple.aml.stargate.rm.rpc.proto.AttributesDeleteRequest;
import com.apple.aml.stargate.rm.rpc.proto.AttributesReadFieldsRequest;
import com.apple.aml.stargate.rm.rpc.proto.AttributesReadRangeFieldsRequest;
import com.apple.aml.stargate.rm.rpc.proto.AttributesReadRequest;
import com.apple.aml.stargate.rm.rpc.proto.AttributesReadResponse;
import com.apple.aml.stargate.rm.rpc.proto.AttributesWriteRequest;
import com.apple.aml.stargate.rm.rpc.proto.BulkOpCompletionResponse;
import com.apple.aml.stargate.rm.rpc.proto.CompletionResponse;
import com.apple.aml.stargate.rm.rpc.proto.CompletionStatus;
import com.apple.aml.stargate.rm.rpc.proto.DataType;
import com.apple.aml.stargate.rm.rpc.proto.ErrorRequest;
import com.apple.aml.stargate.rm.rpc.proto.ErrorResponse;
import com.apple.aml.stargate.rm.rpc.proto.LookupBulkDeleteRequest;
import com.apple.aml.stargate.rm.rpc.proto.LookupBulkReadRequest;
import com.apple.aml.stargate.rm.rpc.proto.LookupBulkWriteRequest;
import com.apple.aml.stargate.rm.rpc.proto.LookupDeleteRequest;
import com.apple.aml.stargate.rm.rpc.proto.LookupReadRequest;
import com.apple.aml.stargate.rm.rpc.proto.LookupReadResponse;
import com.apple.aml.stargate.rm.rpc.proto.LookupWriteRequest;
import com.apple.aml.stargate.rm.rpc.proto.ShuriOfsDeleteFeatureFieldRequest;
import com.apple.aml.stargate.rm.rpc.proto.ShuriOfsReadFeatureGroupsRequest;
import com.apple.aml.stargate.rm.rpc.proto.ShuriOfsReadFeatureRequest;
import com.apple.aml.stargate.rm.rpc.proto.ShuriOfsReadResponse;
import com.apple.aml.stargate.rm.rpc.proto.ShuriOfsWriteFeatureGroupRequest;
import com.apple.ist.idms.i3.a3client.pub.A3TokenInfo;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.beam.sdk.ts.ExternalFunction.externalFunctionOptions;
import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.utils.JsonUtils.fastJsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.rm.rpc.proto.ResourceManagerProtoServiceGrpc.ResourceManagerProtoServiceImplBase;
import static com.apple.jvm.commons.util.Strings.isBlank;

@Service
public class RMGrpcService extends ResourceManagerProtoServiceImplBase {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    @Override
    public void fetchA3Token(final A3TokenRequest request, final StreamObserver<A3TokenResponse> observer) {
        A3TokenResponse.Builder builder = A3TokenResponse.newBuilder();
        try {
            A3TokenInfo info = A3Utils.getCachedTokenInfo(request.getSourceAppId(), request.getTargetAppId(), request.getPassword(), request.getContextString(), null, null);
            builder.setToken(info.getToken()).setExpiry(A3Utils.tokenExpiry(info));
        } catch (Exception e) {
            LOGGER.error("Error in fetching A3 token", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            builder.setToken(EMPTY_STRING).setExpiry(-1);
        }
        observer.onNext(builder.build());
        observer.onCompleted();
    }

    @Override
    public void validateA3Token(final A3TokenValidateRequest request, final StreamObserver<A3TokenValidateResponse> observer) {
        A3TokenValidateResponse.Builder builder = A3TokenValidateResponse.newBuilder();
        try {
            builder.setValid(A3Utils.validate(request.getAppId(), request.getToken())).setMessage(EMPTY_STRING);
        } catch (Exception e) {
            LOGGER.warn("Invalid A3 token", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            builder.setValid(false).setMessage(String.valueOf(e.getMessage()));
        }
        observer.onNext(builder.build());
        observer.onCompleted();
    }

    @Override
    public void publishError(final ErrorRequest request, final StreamObserver<ErrorResponse> observer) {
        super.publishError(request, observer);
    }

    @Override
    public void readAttributes(final AttributesReadRequest request, final StreamObserver<AttributesReadResponse> observer) {
        String nodeName = request.getNodeName();
        AttributesReadResponse.Builder builder = AttributesReadResponse.newBuilder().setDataType(request.getDataType());
        try {
            AthenaAttributeService service = athenaAttributeService(nodeName);
            Object value = isBlank(request.getFieldName()) ? service.read(request.getKey()) : service.readField(request.getKey(), request.getFieldName());
            builder.setValue((value instanceof Map || value instanceof Collection) ? fastJsonString(value) : String.valueOf(value)).setStatus(true);
        } catch (Exception e) {
            LOGGER.error("Error in serving readAttributes", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName), e);
            builder.setStatus(false).setMessage(String.valueOf(e.getMessage()));
        }
        observer.onNext(builder.build());
        observer.onCompleted();
    }

    @Override
    public void bulkReadAttributes(AttributesBulkReadRequest request, StreamObserver<AttributesReadResponse> responseObserver) {
        String nodeName = request.getNodeName();
        AttributesReadResponse.Builder builder = AttributesReadResponse.newBuilder().setDataType(request.getDataType());
        try {
            AthenaAttributeService service = athenaAttributeService(nodeName);
            Map<String, String> value = service.bulkRead(request.getKeysList(), output -> (output instanceof Map || output instanceof Collection) ? fastJsonString(output) : String.valueOf(output));
            builder.setValue(fastJsonString(value)).setStatus(true);
        } catch (Exception e) {
            LOGGER.error("Error in serving bulkReadAttributes", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName), e);
            builder.setStatus(false).setMessage(String.valueOf(e.getMessage()));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void readAttributesFields(final AttributesReadFieldsRequest request, final StreamObserver<AttributesReadResponse> observer) {
        String nodeName = request.getNodeName();
        AttributesReadResponse.Builder builder = AttributesReadResponse.newBuilder().setDataType(request.getDataType());
        try {
            AthenaAttributeService service = athenaAttributeService(nodeName);
            Map<String, ?> map = service.readFields(request.getKey(), request.getFieldNamesList().stream().collect(Collectors.toList()));
            builder.setValue(fastJsonString(map)).setStatus(true);
        } catch (Exception e) {
            LOGGER.error("Error in serving readAttributesFields", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName), e);
            builder.setStatus(false).setMessage(String.valueOf(e.getMessage()));
        }
        observer.onNext(builder.build());
        observer.onCompleted();
    }

    @Override
    public void readAttributesRangeFields(final AttributesReadRangeFieldsRequest request, final StreamObserver<AttributesReadResponse> observer) {
        String nodeName = request.getNodeName();
        AttributesReadResponse.Builder builder = AttributesReadResponse.newBuilder().setDataType(request.getDataType());
        try {
            AthenaAttributeService service = athenaAttributeService(nodeName);
            Map<String, ?> map = service.readRangeFields(request.getKey(), request.getStartKey(), request.getEndKey());
            builder.setValue(fastJsonString(map)).setStatus(true);
        } catch (Exception e) {
            LOGGER.error("Error in serving readAttributesRangeFields", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName), e);
            builder.setStatus(false).setMessage(String.valueOf(e.getMessage()));
        }
        observer.onNext(builder.build());
        observer.onCompleted();
    }

    @Override
    public void writeAttributes(final AttributesWriteRequest request, final StreamObserver<CompletionResponse> observer) {
        String nodeName = request.getNodeName();
        CompletionResponse.Builder builder = CompletionResponse.newBuilder();
        try {
            AthenaAttributeService service = athenaAttributeService(nodeName);
            builder.setStatus(service.write(request.getKey(), translatedObject(request.getValue(), request.getDataType()), request.getTtl()));
        } catch (Exception e) {
            LOGGER.error("Error in serving writeAttributes", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName), e);
            builder.setStatus(false).setMessage(String.valueOf(e.getMessage()));
        }
        observer.onNext(builder.build());
        observer.onCompleted();
    }

    @Override
    public void bulkWriteAttributes(AttributesBulkWriteRequest request, StreamObserver<BulkOpCompletionResponse> responseObserver) {
        String nodeName = request.getNodeName();
        BulkOpCompletionResponse.Builder builder = BulkOpCompletionResponse.newBuilder();
        try {
            AthenaAttributeService service = athenaAttributeService(nodeName);
            Map<String, Object> tranformedMap = request.getKeyValueCollectionMap().entrySet().stream().map(entry -> Pair.of(entry.getKey(), translatedObject(entry.getValue(), request.getDataType()))).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
            builder.putAllStatus(service.bulkWrite(tranformedMap, request.getTtl()));

        } catch (Exception e) {
            LOGGER.error("Error in serving bulkWriteAttributes", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName), e);
            Map<String, Boolean> errorMap = request.getKeyValueCollectionMap().keySet().stream().map(key -> Pair.of(key, Boolean.FALSE)).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
            builder.putAllStatus(errorMap).setMessage(String.valueOf(e.getMessage()));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();

    }

    @Override
    public void writeAttributesField(final AttributesWriteRequest request, final StreamObserver<CompletionResponse> observer) {
        String nodeName = request.getNodeName();
        CompletionResponse.Builder builder = CompletionResponse.newBuilder();
        try {
            AthenaAttributeService service = athenaAttributeService(nodeName);
            builder.setStatus(service.writeField(request.getKey(), request.getFieldName(), translatedObject(request.getValue(), request.getDataType()), request.getTtl()));
        } catch (Exception e) {
            LOGGER.error("Error in serving writeAttributesField", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName), e);
            builder.setStatus(false).setMessage(String.valueOf(e.getMessage()));
        }
        observer.onNext(builder.build());
        observer.onCompleted();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writeAttributesFields(final AttributesWriteRequest request, final StreamObserver<CompletionResponse> observer) {
        String nodeName = request.getNodeName();
        CompletionResponse.Builder builder = CompletionResponse.newBuilder();
        try {
            AthenaAttributeService service = athenaAttributeService(nodeName);
            Map<String, ?> map = readJson(request.getValue(), Map.class);
            builder.setStatus(service.writeFields(request.getKey(), map, request.getTtl()));
        } catch (Exception e) {
            LOGGER.error("Error in serving writeAttributesFields", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName), e);
            builder.setStatus(false).setMessage(String.valueOf(e.getMessage()));
        }
        observer.onNext(builder.build());
        observer.onCompleted();
    }

    @Override
    public void deleteAttributes(final AttributesDeleteRequest request, final StreamObserver<CompletionResponse> observer) {
        String nodeName = request.getNodeName();
        CompletionResponse.Builder builder = CompletionResponse.newBuilder();
        try {
            AthenaAttributeService service = athenaAttributeService(nodeName);
            builder.setStatus(request.getFieldNamesCount() == 0 ? service.delete(request.getKey()) : service.deleteFields(request.getKey(), request.getFieldNamesList().stream().collect(Collectors.toList())));
        } catch (Exception e) {
            LOGGER.error("Error in serving deleteAttributes", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName), e);
            builder.setStatus(false).setMessage(String.valueOf(e.getMessage()));
        }
        observer.onNext(builder.build());
        observer.onCompleted();
    }

    @Override
    public void bulkDeleteAttributes(AttributesBulkDeleteRequest request, StreamObserver<BulkOpCompletionResponse> responseObserver) {
        String nodeName = request.getNodeName();
        BulkOpCompletionResponse.Builder builder = BulkOpCompletionResponse.newBuilder();
        try {
            AthenaAttributeService service = athenaAttributeService(nodeName);
            builder.putAllStatus(service.bulkDelete(request.getKeysList()));
        } catch (Exception e) {
            LOGGER.error("Error in serving bulkDeleteAttributes", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName), e);
            Map<String, Boolean> errorMap = request.getKeysList().stream().map(key -> Pair.of(key, Boolean.FALSE)).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
            builder.putAllStatus(errorMap).setMessage(String.valueOf(e.getMessage()));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void readLookup(final LookupReadRequest request, final StreamObserver<LookupReadResponse> observer) {
        String nodeName = request.getNodeName();
        LookupReadResponse.Builder builder = LookupReadResponse.newBuilder().setDataType(request.getDataType());
        try {
            AthenaAttributeService service = athenaAttributeService(nodeName);
            Object value = service.readLookup(request.getKey());
            builder.setValue((value instanceof Map || value instanceof Collection) ? fastJsonString(value) : String.valueOf(value)).setStatus(true);
        } catch (Exception e) {
            LOGGER.error("Error in serving readLookup", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName), e);
            builder.setStatus(false).setMessage(String.valueOf(e.getMessage()));
        }
        observer.onNext(builder.build());
        observer.onCompleted();
    }

    @Override
    public void bulkReadLookup(LookupBulkReadRequest request, StreamObserver<LookupReadResponse> responseObserver) {
        String nodeName = request.getNodeName();
        LookupReadResponse.Builder builder = LookupReadResponse.newBuilder().setDataType(request.getDataType());
        try {
            AthenaAttributeService service = athenaAttributeService(nodeName);
            Map<String, String> value = service.bulkReadLookup(request.getKeysList(), output -> (output instanceof Map || output instanceof Collection) ? fastJsonString(output) : String.valueOf(output));
            builder.setValue(fastJsonString(value)).setStatus(true);
        } catch (Exception e) {
            LOGGER.error("Error in serving bulkReadLookup", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName), e);
            builder.setStatus(false).setMessage(String.valueOf(e.getMessage()));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void writeLookup(final LookupWriteRequest request, final StreamObserver<CompletionResponse> observer) {
        String nodeName = request.getNodeName();
        CompletionResponse.Builder builder = CompletionResponse.newBuilder();
        try {
            AthenaAttributeService service = athenaAttributeService(nodeName);
            builder.setStatus(service.writeLookup(request.getKey(), translatedObject(request.getValue(), request.getDataType()), request.getTtl()));
        } catch (Exception e) {
            LOGGER.error("Error in serving writeLookup", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName), e);
            builder.setStatus(false).setMessage(String.valueOf(e.getMessage()));
        }
        observer.onNext(builder.build());
        observer.onCompleted();
    }

    @Override
    public void bulkWriteLookup(LookupBulkWriteRequest request, StreamObserver<BulkOpCompletionResponse> responseObserver) {
        String nodeName = request.getNodeName();
        BulkOpCompletionResponse.Builder builder = BulkOpCompletionResponse.newBuilder();
        try {
            AthenaAttributeService service = athenaAttributeService(nodeName);
            Map<String, Object> tranformedMap = request.getKeyValueCollectionMap().entrySet().stream().map(entry -> Pair.of(entry.getKey(), translatedObject(entry.getValue(), request.getDataType()))).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
            builder.putAllStatus(service.bulkWriteLookup(tranformedMap, request.getTtl()));
        } catch (Exception e) {
            LOGGER.error("Error in serving bulkWriteLookup", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName), e);
            Map<String, Boolean> errorMap = request.getKeyValueCollectionMap().keySet().stream().map(key -> Pair.of(key, Boolean.FALSE)).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
            builder.putAllStatus(errorMap).setMessage(String.valueOf(e.getMessage()));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteLookup(final LookupDeleteRequest request, final StreamObserver<CompletionResponse> observer) {
        String nodeName = request.getNodeName();
        CompletionResponse.Builder builder = CompletionResponse.newBuilder();
        try {
            AthenaAttributeService service = athenaAttributeService(nodeName);
            builder.setStatus(service.deleteLookup(request.getKey()));
        } catch (Exception e) {
            LOGGER.error("Error in serving deleteLookup", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName), e);
            builder.setStatus(false).setMessage(String.valueOf(e.getMessage()));
        }
        observer.onNext(builder.build());
        observer.onCompleted();
    }

    @Override
    public void bulkDeleteLookup(LookupBulkDeleteRequest request, StreamObserver<BulkOpCompletionResponse> responseObserver) {
        String nodeName = request.getNodeName();
        BulkOpCompletionResponse.Builder builder = BulkOpCompletionResponse.newBuilder();
        try {
            AthenaAttributeService service = athenaAttributeService(nodeName);
            builder.putAllStatus(service.bulkDeleteLookup(request.getKeysList()));
        } catch (Exception e) {
            LOGGER.error("Error in serving bulkDeleteLookup", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName), e);
            Map<String, Boolean> errorMap = request.getKeysList().stream().map(key -> Pair.of(key, Boolean.FALSE)).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
            builder.putAllStatus(errorMap).setMessage(String.valueOf(e.getMessage()));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void readShuriOfsFeature(final ShuriOfsReadFeatureRequest request, final StreamObserver<ShuriOfsReadResponse> observer) {
        String nodeName = request.getNodeName();
        ShuriOfsReadResponse.Builder builder = ShuriOfsReadResponse.newBuilder();
        try {
            AthenaAttributeService service = athenaAttributeService(nodeName);
            Object value = isBlank(request.getFieldName()) ? service.readFeatureGroup(request.getFeatureName(), request.getEntityId()) : service.readFeatureField(request.getFeatureName(), request.getEntityId(), request.getFieldName());
            builder.setValue((value instanceof Map || value instanceof Collection) ? fastJsonString(value) : String.valueOf(value));
        } catch (Exception e) {
            LOGGER.error("Error in serving readShuriOfsFeature", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName), e);
        }
        observer.onNext(builder.build());
        observer.onCompleted();
    }

    @Override
    public void readShuriOfsFeatureGroups(final ShuriOfsReadFeatureGroupsRequest request, final StreamObserver<ShuriOfsReadResponse> observer) {
        String nodeName = request.getNodeName();
        ShuriOfsReadResponse.Builder builder = ShuriOfsReadResponse.newBuilder();
        try {
            AthenaAttributeService service = athenaAttributeService(nodeName);
            builder.setValue(fastJsonString(service.readFeatureGroups(request.getFeatureName(), request.getEntityIdsList().stream().collect(Collectors.toList()))));
        } catch (Exception e) {
            LOGGER.error("Error in serving readShuriOfsFeatureGroups", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName), e);
        }
        observer.onNext(builder.build());
        observer.onCompleted();
    }

    @Override
    public void writeShuriOfsFeatureGroup(final ShuriOfsWriteFeatureGroupRequest request, final StreamObserver<CompletionStatus> observer) {
        String nodeName = request.getNodeName();
        CompletionStatus.Builder builder = CompletionStatus.newBuilder();
        try {
            AthenaAttributeService service = athenaAttributeService(nodeName);
            Map<String, ?> map = new HashMap<>();
            request.getDataMap().forEach((k, v) -> map.put(k, translatedObject(v)));
            builder.setSucceeded(service.writeFeatureGroup(request.getFeatureName(), request.getEntityId(), map, request.getTtl()));
        } catch (Exception e) {
            LOGGER.error("Error in serving writeShuriOfsFeatureGroup", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName), e);
        }
        observer.onNext(builder.build());
        observer.onCompleted();
    }

    @Override
    public void deleteShuriOfsFeatureField(final ShuriOfsDeleteFeatureFieldRequest request, final StreamObserver<CompletionStatus> observer) {
        String nodeName = request.getNodeName();
        CompletionStatus.Builder builder = CompletionStatus.newBuilder();
        try {
            AthenaAttributeService service = athenaAttributeService(nodeName);
            builder.setSucceeded(isBlank(request.getFieldName()) ? service.deleteFeatureGroup(request.getFeatureName(), request.getEntityId()) : service.deleteFeatureField(request.getFeatureName(), request.getEntityId(), request.getFieldName()));
        } catch (Exception e) {
            LOGGER.error("Error in serving deleteShuriOfsFeatureField", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName), e);
        }
        observer.onNext(builder.build());
        observer.onCompleted();
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public <O> O translatedObject(final String value) {
        try {
            String stripped = value.stripLeading();
            if (stripped.startsWith("{")) return (O) readJson(value, Map.class);
            if (stripped.startsWith("[")) return (O) readJson(value, List.class);
            return (O) value;
        } catch (Exception ignored) {
            return (O) value;
        }
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public Object translatedObject(final String value, final DataType dataType) {
        switch (dataType) {
            case MAP:
            case TUPLE:
                return readJson(value, Map.class);
            case LIST:
                return readJson(value, List.class);
            case SET:
                return readJson(value, Set.class);
            case INTEGER:
                return Integer.parseInt(value);
            case FLOAT:
                return Float.parseFloat(value);
            case BOOLEAN:
                return ClassUtils.parseBoolean(value);
            case STRING:
                return value;
            case OTHER:
            case UNRECOGNIZED:
                String stripped = value.stripLeading();
                if (stripped.startsWith("{")) return readJson(value, Map.class);
                if (stripped.startsWith("[")) return readJson(value, List.class);
                return value;
        }
        return null;
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static AthenaAttributeService athenaAttributeService(final String nodeName) {
        return AthenaAttributeService.attributeService(nodeName, n -> {
            ExternalFunctionOptions functionOptions = externalFunctionOptions(nodeName);
            if (functionOptions == null || functionOptions.getAttributesOptions() == null) throw new RuntimeException(String.format("%s node is not configured with required attribute details!!", nodeName));
            return functionOptions.getAttributesOptions();
        });
    }
}
