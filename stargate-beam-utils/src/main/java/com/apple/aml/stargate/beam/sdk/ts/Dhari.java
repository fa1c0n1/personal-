package com.apple.aml.stargate.beam.sdk.ts;

import com.apple.aml.dataplatform.client.rpc.dhari.proto.DhariPing;
import com.apple.aml.dataplatform.client.rpc.dhari.proto.DhariProtoServiceGrpc.DhariProtoServiceBlockingStub;
import com.apple.aml.dataplatform.client.rpc.dhari.proto.IngestCount;
import com.apple.aml.dataplatform.client.rpc.dhari.proto.PipelinePayload;
import com.apple.aml.dataplatform.util.ConfigUtils;
import com.apple.aml.dataplatform.util.GRpcClientUtils;
import com.apple.aml.stargate.beam.sdk.options.StargateOptions;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.DhariOptions;
import freemarker.template.Configuration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.beam.sdk.ts.JavaFunction.emitOutput;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.pipelineId;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.AppConfig.appId;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readNullableJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.web.clients.PipelineConfigClient.ingestJson;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class Dhari extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final ConcurrentHashMap<String, GenericObjectPool<DhariProtoServiceBlockingStub>> POOL_MAP = new ConcurrentHashMap<>();
    private String nodeName;
    private String nodeType;
    private String pipelineId;
    private String pipelineToken;
    private DhariOptions options;
    private ObjectToGenericRecordConverter converter;
    private ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap = new ConcurrentHashMap<>();
    private String discoveryUrl;
    private transient Configuration configuration;
    private String payloadTemplateName;

    public static DhariOptions resolveDhariOptions(final DhariOptions options) throws Exception {
        if (options == null) return null;
        if (isBlank(options.getGrpcEndpoint())) {
            Map.Entry<String, Integer> entry = ConfigUtils.fetchConnectionDetails(options.discoveryUrl());
            options.setGrpcEndpoint(String.format("%s:%d", entry.getKey(), entry.getValue()));
        }
        return options;
    }

    @SuppressWarnings("rawtypes")
    public void initWrite(final Pipeline pipeline, final StargateNode node) throws Exception {
        initCommon(pipeline, node);
    }

    @SuppressWarnings("rawtypes")
    public void initCommon(final Pipeline pipeline, final StargateNode node) throws Exception {
        DhariOptions options = (DhariOptions) node.getConfig();
        this.options = options;
        this.nodeName = node.getName();
        this.nodeType = node.getType();
        this.payloadTemplateName = nodeName + "~payload";
        this.pipelineId = pipelineId();
        this.pipelineToken = pipeline.getOptions().as(StargateOptions.class).getPipelineToken();
        this.discoveryUrl = options.discoveryUrl();
        if (!isBlank(this.options.getSchemaId())) {
            this.converter = converter(fetchSchemaWithLocalFallback(this.options.getSchemaReference(), this.options.getSchemaId()));
            LOGGER.debug("Converter created successfully for", Map.of(SCHEMA_ID, options.getSchemaId(), NODE_NAME, nodeName));
        }
    }

    @SuppressWarnings("rawtypes")
    public void initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        initCommon(pipeline, node);
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> inputCollection) throws Exception {
        return inputCollection.apply(node.getName(), this);
    }

    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> inputCollection) throws Exception {
        return inputCollection.apply(node.getName(), this);
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    @ProcessElement
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
        long startTime = System.nanoTime();
        log(options, nodeName, nodeType, kv);
        String key = kv.getKey();
        GenericRecord record = kv.getValue();
        Schema schema = record.getSchema();
        String recordSchemaId = schema.getFullName();
        counter(nodeName, nodeType, recordSchemaId, ELEMENTS_IN).inc();
        Map response;
        String jsonPayload;
        if (this.options.getPayload() == null) {
            jsonPayload = record.toString();
            response = readNullableJsonMap(jsonPayload);
        } else {
            jsonPayload = evaluateFreemarker(configuration(), payloadTemplateName, key, record, schema);
            response = readNullableJsonMap(jsonPayload);
            if (jsonPayload == null || response == null || jsonPayload.trim().isBlank()) {
                LOGGER.debug("Freemarker processing returned null. Will skip this record", Map.of(SCHEMA_ID, recordSchemaId, "key", key, NODE_NAME, nodeName));
                return;
            }
        }
        String statusMessage = null;
        long recordCount = 0;
        final String schemaId = this.options.getSchemaId() == null ? recordSchemaId : this.options.getSchemaId();
        if (options.isGrpc()) {
            GenericObjectPool<DhariProtoServiceBlockingStub> pool = null;
            DhariProtoServiceBlockingStub client = null;
            try {
                pool = grpcPool();
                client = pool.borrowObject();
                if (options.isValidateGrpc()) {
                    try {
                        if (!client.ping(DhariPing.newBuilder().setTime(System.nanoTime()).build()).getStatus()) {
                            throw new Exception("Ping failed");
                        }
                    } catch (Exception e) {
                        LOGGER.warn("Ping check failed on borrowed grpc connection. Will fetch once again. Reason - " + e.getMessage());
                        client = pool.borrowObject();
                    }
                }
                if ("eai".equalsIgnoreCase(options.getMethod())) {
                    PipelinePayload grpcPayload = PipelinePayload.newBuilder().setPayload(jsonPayload).setAppId((int) appId()).setPipelineId(this.pipelineId).setPipelineToken(this.pipelineToken).setPublisherId(EMPTY_STRING).setSchemaId(EMPTY_STRING).setPublishFormat(EMPTY_STRING).setPayloadKey(options.isRandomKey() ? UUID.randomUUID().toString() : kv.getKey()).build();
                    IngestCount ingestResponse = client.ingestPipelineEaiS3(grpcPayload);
                    recordCount = ingestResponse.getCount();
                    statusMessage = "Triggered EAI S3 processing successfully via gRPC";
                } else {
                    PipelinePayload grpcPayload = PipelinePayload.newBuilder().setPayload(jsonPayload).setAppId((int) appId()).setPipelineId(this.pipelineId).setPipelineToken(this.pipelineToken).setPublisherId(this.options.getPublisherId()).setSchemaId(schemaId).setPublishFormat(options.getPublishFormat()).setPayloadKey(options.isRandomKey() ? UUID.randomUUID().toString() : kv.getKey()).build();
                    IngestCount ingestResponse = client.ingestPipelineJson(grpcPayload);
                    recordCount = ingestResponse.getCount();
                    statusMessage = "Ingested payload successfully via gRPC";
                }
                histogramDuration(nodeName, nodeType, schemaId, "process_grpc_success").observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.debug(statusMessage, Map.of("recordCount", recordCount, "key", key, NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, schemaId));
            } catch (Exception e) {
                returnGrpcToPool(pool, client);
                histogramDuration(nodeName, nodeType, schemaId, "process_grpc_error").observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.warn("Error invoking Dhari grpc endpoint", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "key", key, NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, schemaId), e);
                counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
                ctx.output(ERROR_TAG, eRecord(nodeName, nodeType, "dhari_grpc_publish", kv, e));
                return;
            } finally {
                returnGrpcToPool(pool, client);
                histogramDuration(nodeName, nodeType, schemaId, "process").observe((System.nanoTime() - startTime) / 1000000.0);
            }
        } else {
            try {
                Map ingestResponse = ingestJson(pipelineId, pipelineToken, appId(), options.getPublisherId(), schemaId, options.isRandomKey() ? UUID.randomUUID().toString() : kv.getKey(), jsonPayload, options.getPublishFormat());
                recordCount = 1;
                statusMessage = jsonString(ingestResponse);
                histogramDuration(nodeName, nodeType, schemaId, "process_http_success").observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.debug("Ingested payload successfully via REST", ingestResponse, Map.of("recordCount", recordCount, "key", key, NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, schemaId));
            } catch (Exception e) {
                histogramDuration(nodeName, nodeType, schemaId, "process_http_error").observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.warn("Error invoking Dhari REST endpoint", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "key", key, NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, schemaId), e);
                counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
                ctx.output(ERROR_TAG, eRecord(nodeName, nodeType, "dhari_http_publish", kv, e));
                return;
            } finally {
                histogramDuration(nodeName, nodeType, schemaId, "process").observe((System.nanoTime() - startTime) / 1000000.0);
            }
        }
        response.put("dhariStatus", statusMessage);
        response.put("ingestCount", recordCount);
        emitOutput(kv, null, response, ctx, schema, recordSchemaId, this.options.getSchemaId(), this.converter, converterMap, nodeName, nodeType);
    }

    private freemarker.template.Configuration configuration() {
        if (configuration != null) {
            return configuration;
        }
        if (this.options.getPayload() != null) {
            loadFreemarkerTemplate(payloadTemplateName, this.options.getPayload());
        }
        configuration = freeMarkerConfiguration();
        return configuration;
    }

    private GenericObjectPool<DhariProtoServiceBlockingStub> grpcPool() {
        GenericObjectPool<DhariProtoServiceBlockingStub> grpcPool = POOL_MAP.computeIfAbsent(discoveryUrl, url -> GRpcClientUtils.dhariBlockingGrpcClientPool(url, options.getGrpcEndpoint()));
        if (grpcPool != null) {
            return grpcPool;
        }
        grpcPool = GRpcClientUtils.dhariBlockingGrpcClientPool(discoveryUrl, options.getGrpcEndpoint());
        POOL_MAP.put(discoveryUrl, grpcPool);
        return grpcPool;
    }

    private void returnGrpcToPool(GenericObjectPool<DhariProtoServiceBlockingStub> pool, DhariProtoServiceBlockingStub client) {
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
