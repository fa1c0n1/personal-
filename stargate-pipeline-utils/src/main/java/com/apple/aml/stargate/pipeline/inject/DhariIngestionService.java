package com.apple.aml.stargate.pipeline.inject;

import com.apple.aml.dataplatform.client.rpc.dhari.proto.DhariProtoServiceGrpc;
import com.apple.aml.dataplatform.client.rpc.dhari.proto.DhariProtoServiceGrpc.DhariProtoServiceBlockingStub;
import com.apple.aml.dataplatform.client.rpc.dhari.proto.JsonPayloadWithHeaders;
import com.apple.aml.dataplatform.client.rpc.dhari.proto.PayloadResponse;
import com.apple.aml.dataplatform.util.GRpcClientUtils;
import com.apple.aml.stargate.common.constants.A3Constants;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.options.DhariOptions;
import com.apple.aml.stargate.common.services.DhariService;
import com.apple.aml.stargate.common.utils.AppConfig;
import com.apple.aml.stargate.common.utils.ContextHandler;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.INGEST_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.INGEST_SUCCESS;
import static com.apple.aml.stargate.common.converters.LazyJsonToUnstructuredGenericRecordConverter.DEFAULT_UNSTRUCTURED_FIELD_NAME;
import static com.apple.aml.stargate.common.utils.A3Utils.getA3Token;
import static com.apple.aml.stargate.common.utils.A3Utils.getCachedToken;
import static com.apple.aml.stargate.common.utils.AppConfig.appId;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readNullableJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class DhariIngestionService implements DhariService, Serializable {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final long DHARI_APP_ID = A3Constants.KNOWN_APP.DHARI.appId();
    private static final ConcurrentHashMap<String, GenericObjectPool<DhariProtoServiceBlockingStub>> POOL_MAP = new ConcurrentHashMap<>();
    private final DhariOptions options;
    private final String discoveryUrl;
    private final PipelineConstants.ENVIRONMENT environment;

    public DhariIngestionService() {
        this(null);
    }

    public DhariIngestionService(final DhariOptions options) {
        this.options = options == null ? new DhariOptions() : options;
        this.discoveryUrl = this.options.discoveryUrl();
        this.environment = isBlank(this.options.getA3Mode()) ? AppConfig.environment() : PipelineConstants.ENVIRONMENT.environment(this.options.getA3Mode());
        if (this.options.getA3AppId() <= 0) this.options.setA3AppId(appId());
    }

    @Override
    public Map<String, ?> ingest(final String publisherId, final String schemaId, final String payloadKey, final Object payload, final Map<String, String> headers) throws Exception {
        return publishPayload(publisherId, schemaId, payloadKey, payload, headers, false);
    }

    @Override
    public Map<String, ?> asyncIngest(final String publisherId, final String schemaId, final String payloadKey, final Object payload, final Map<String, String> headers) throws Exception {
        return publishPayload(publisherId, schemaId, payloadKey, payload, headers, true);
    }

    public Map<String, ?> publishPayload(final String publisherId, final String _schemaId, final String payloadKey, final Object payload, final Map<String, String> headers, final boolean async) throws Exception {
        long startTimeNanos = System.nanoTime();
        ContextHandler.Context ctx = ContextHandler.ctx();
        GenericObjectPool<DhariProtoServiceBlockingStub> pool = null;
        DhariProtoServiceBlockingStub client = null;
        final String ingestKey = isBlank(payloadKey) ? UUID.randomUUID().toString() : payloadKey;
        final String schemaId = isBlank(_schemaId) ? options.getSchemaId() : _schemaId;
        try {
            pool = grpcPool();
            client = pool.borrowObject();
            String a3Token = this.options.getA3Password() == null ? getA3Token(DHARI_APP_ID) : getCachedToken(this.options.getA3AppId(), DHARI_APP_ID, this.options.getA3Password(), this.options.getA3ContextString(), null, environment);
            JsonPayloadWithHeaders.Builder builder = JsonPayloadWithHeaders.newBuilder().setAppId((int) this.options.getA3AppId()).setToken(a3Token).setUnstructuredFieldName(DEFAULT_UNSTRUCTURED_FIELD_NAME).setPublisherId(publisherId == null ? this.options.getPublisherId() : publisherId).setSchemaId(schemaId).setPayloadKey(ingestKey);
            builder = builder.setPayload(jsonString(payload));
            if (headers != null) builder.putAllHeaders(headers);
            PayloadResponse status;
            if (async) {
                status = client.ingestPayload(builder.build());
            } else {
                status = client.ingestPayloadAsync(builder.build());
            }
            if (status.getCount() <= 0) throw new Exception(isBlank(status.getException()) ? "Payload could not be ingested. Invalid ingestion count received from Dhari server" : status.getException());
            histogramDuration(ctx.getNodeName(), ctx.getNodeType(), schemaId, INGEST_SUCCESS).observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            return Map.of("count", status.getCount(), "response", readNullableJsonMap(status.getResponseJson()), "payloadKey", status.getPayloadKey());
        } catch (Exception e) {
            histogramDuration(ctx.getNodeName(), ctx.getNodeType(), schemaId, INGEST_ERROR).observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            throw e;
        } finally {
            returnGrpcToPool(pool, client);
        }
    }

    private GenericObjectPool<DhariProtoServiceGrpc.DhariProtoServiceBlockingStub> grpcPool() {
        GenericObjectPool<DhariProtoServiceGrpc.DhariProtoServiceBlockingStub> grpcPool = POOL_MAP.computeIfAbsent(discoveryUrl, url -> GRpcClientUtils.dhariBlockingGrpcClientPool(url, this.options.getGrpcEndpoint()));
        if (grpcPool != null) return grpcPool;
        grpcPool = GRpcClientUtils.dhariBlockingGrpcClientPool(discoveryUrl, this.options.getGrpcEndpoint());
        POOL_MAP.put(discoveryUrl, grpcPool);
        return grpcPool;
    }

    private void returnGrpcToPool(GenericObjectPool<DhariProtoServiceGrpc.DhariProtoServiceBlockingStub> pool, DhariProtoServiceGrpc.DhariProtoServiceBlockingStub client) {
        try {
            if (pool == null) {
                return;
            }
            if (client != null) {
                pool.returnObject(client);
            }
        } catch (Exception ex) {
            LOGGER.trace("Could not return dhari grpc client to the pool. Reason : " + ex.getMessage());
        }
    }
}
