package com.apple.aml.stargate.beam.sdk.ts;

import com.apple.aml.stargate.beam.inject.BeamContext;
import com.apple.aml.stargate.beam.sdk.io.file.AbstractWriter;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.DerivedSchemaOptions;
import com.apple.aml.stargate.common.options.ExternalBatchFunctionOptions;
import com.apple.aml.stargate.common.options.ExternalTransformOptions;
import com.apple.aml.stargate.common.utils.ClassUtils;
import com.apple.aml.stargate.common.utils.ContextHandler;
import com.apple.aml.stargate.external.rpc.proto.ExternalFunctionProtoServiceGrpc;
import com.apple.aml.stargate.external.rpc.proto.RequestPayload;
import com.apple.aml.stargate.external.rpc.proto.ResponsePayload;
import com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils;
import freemarker.template.Configuration;
import io.github.resilience4j.retry.Retry;
import lombok.SneakyThrows;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.joda.time.Instant;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.beam.sdk.ts.BatchFunction.emitBatchRecords;
import static com.apple.aml.stargate.beam.sdk.ts.Dhari.resolveDhariOptions;
import static com.apple.aml.stargate.beam.sdk.ts.ExternalFunction.grpcPool;
import static com.apple.aml.stargate.beam.sdk.ts.ExternalFunction.initializeStub;
import static com.apple.aml.stargate.beam.sdk.ts.ExternalFunction.returnGrpcToPool;
import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.applyWindow;
import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.batchPartitionKey;
import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.batchWriter;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.GENERIC_RECORD_GROUP_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.MEDIA_TYPE_APPLICATION_JSON;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_NULL;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.ClassUtils.parseBoolean;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.SneakyRetryFunction.applyWithRetry;
import static com.apple.aml.stargate.common.utils.WebUtils.newOkHttpClient;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.retrySettings;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.saveLocalSchema;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class ExternalBatchFunction extends AbstractWriter<KV<String, KV<String, GenericRecord>>, Void> implements Serializable {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final long serialVersionUID = 1L;
    private static final ConcurrentHashMap<String, Boolean> INIT_STATUS_MAP = new ConcurrentHashMap<>();
    private boolean emit = true;
    private String nodeName;
    private String nodeType;
    private ExternalBatchFunctionOptions options;
    private DerivedSchemaOptions schemaOptions;
    private String payloadUri;
    private boolean useGrpc;
    private Schema schema;
    private String schemaId;
    private ConcurrentHashMap<String, Schema> schemaMap = new ConcurrentHashMap<>();
    private ObjectToGenericRecordConverter converter;
    private ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap = new ConcurrentHashMap<>();
    private transient OkHttpClient okHttpClient;
    private transient Retry retry;
    private transient Configuration configuration;
    private String logContextTemplateName;

    public void initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        initCommon(pipeline, node, true);
    }

    public void initWrite(final Pipeline pipeline, final StargateNode node) throws Exception {
        initCommon(pipeline, node, false);
    }

    @SuppressWarnings("unchecked")
    public void initCommon(final Pipeline pipeline, final StargateNode node, final boolean emit) throws Exception {
        this.nodeName = node.getName();
        this.nodeType = node.getType();
        this.emit = emit;
        this.options = (ExternalBatchFunctionOptions) node.getConfig();
        if (options.connectionOptions() == null) options.connectionOptions(new ExternalTransformOptions());
        this.schemaOptions = ClassUtils.getAs(options, DerivedSchemaOptions.class);
        this.logContextTemplateName = isBlank(options.getLogContext()) ? null : (nodeName + "~logContext");
        PipelineConstants.EXTERNAL_FUNC funcType = PipelineConstants.EXTERNAL_FUNC.funcType(this.options.getRuntimeType(), nodeType);
        if (options.connectionOptions().getPort() <= 0) options.connectionOptions().setPort(funcType.portNo());
        if (isBlank(options.connectionOptions().getExecutionType())) options.connectionOptions().setExecutionType("lambda");
        this.payloadUri = options.connectionOptions().executeUri();
        this.useGrpc = isBlank(options.connectionOptions().getScheme()) || "grpc".equalsIgnoreCase(options.connectionOptions().getScheme());
        resolveDhariOptions(options.getDhariOptions());
        this.schemaOptions.initSchemaDeriveOptions();
        if (!isBlank(this.options.getSchemaId())) {
            this.schema = fetchSchemaWithLocalFallback(options.getSchemaReference(), options.getSchemaId());
            this.schemaId = schema.getFullName();
            this.converter = converter(this.schema);
            LOGGER.debug("External Batch Function schema converter created successfully for", Map.of(SCHEMA_ID, this.schemaId, NODE_NAME, nodeName));
        } else if (options.getSchema() != null && ("override".equalsIgnoreCase(options.getSchemaType()) || "replace".equalsIgnoreCase(options.getSchemaType()))) {
            this.schema = new Schema.Parser().parse(options.getSchema() instanceof String ? (String) options.getSchema() : jsonString(options.getSchema()));
            this.schemaId = schema.getFullName();
            this.converter = converter(schema, options);
            saveLocalSchema(schemaId, schema.toString());
            LOGGER.debug("External Function schema converter created successfully using schema override for", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName, NODE_TYPE, nodeType));
        }
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return invoke(pipeline, node, collection, true);
    }

    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return invoke(pipeline, node, collection, false);
    }

    public SCollection<KV<String, GenericRecord>> invoke(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection, final boolean emit) throws Exception {
        ExternalBatchFunctionOptions options = (ExternalBatchFunctionOptions) node.getConfig();
        SCollection<KV<String, GenericRecord>> window = applyWindow(collection, options, node.getName());
        return window.group(node.name("partition-key"), batchPartitionKey(options.getPartitions(), options.getPartitionStrategy()), GENERIC_RECORD_GROUP_TAG).apply(node.name("batch"), batchWriter(node.getName(), node.getType(), node.environment(), options, this));
    }

    private OkHttpClient initClient() throws Exception {
        if (okHttpClient == null) okHttpClient = newOkHttpClient(options.connectionOptions());
        return okHttpClient;
    }

    private freemarker.template.Configuration configuration() {
        if (configuration != null) return configuration;
        if (logContextTemplateName != null) loadFreemarkerTemplate(logContextTemplateName, this.options.getLogContext());
        configuration = freeMarkerConfiguration();
        return configuration;
    }

    @SuppressWarnings("unchecked")
    @Setup
    public void setup() throws Exception {
        if (retry == null && options.getRetryOptions() != null) retry = retrySettings(nodeName, options.getRetryOptions());
        INIT_STATUS_MAP.computeIfAbsent(nodeName, n -> {
            Map<String, Object> response = Map.of();
            try {
                response = initializeStub(useGrpc, nodeName, nodeType, options, this.schemaId, null);
                boolean initialized = response == null || parseBoolean(response.get("status"));
                if (!initialized) throw new InstantiationException(String.valueOf(response.get("message")));
                LOGGER.debug("External Batch Function initialized successfully!!", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType), response);
                return true;
            } catch (Exception e) {
                LOGGER.error("Error in initializing External Batch Function", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, ERROR_MESSAGE, String.valueOf(e.getMessage())), response, e);
                throw new RuntimeException(e);
            }
        });
        configuration();
    }

    @Override
    protected void tearDown(final WindowedContext nullableContext) throws Exception {
    }

    @Override
    protected KV<String, GenericRecord> process(final KV<String, GenericRecord> kv, final Void batch, final WindowedContext context, final PaneInfo paneInfo, final BoundedWindow window, final Instant timestamp) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean enableRawConsumption() {
        return true;
    }

    @SneakyThrows
    private String invokeExternalBatchFunction(final List<KV<String, GenericRecord>> iterable, final String batchSchemaId, final String batchId, final Map<String, String> logContext) {
        String responseString;
        GenericObjectPool<ExternalFunctionProtoServiceGrpc.ExternalFunctionProtoServiceBlockingStub> pool = null;
        ExternalFunctionProtoServiceGrpc.ExternalFunctionProtoServiceBlockingStub grpcClient = null;
        try {
            if (useGrpc) {
                pool = grpcPool(nodeName, options.connectionOptions());
                grpcClient = pool.borrowObject();
                RequestPayload requestPayload = RequestPayload.newBuilder().setNodeName(nodeName).setSchemaId(batchSchemaId).setPayloadKey(batchId).setPayload("[" + iterable.stream().map(o -> o.getValue().toString()).collect(Collectors.joining(",")) + "]").putAllLogContext(logContext).build();
                ResponsePayload responsePayload = grpcClient.apply(requestPayload);
                if (!isBlank(responsePayload.getException())) {
                    if (ErrorUtils.MISSING_INIT_CONFIG_ERROR.equalsIgnoreCase(responsePayload.getException())) {
                        INIT_STATUS_MAP.remove(nodeName);
                        setup();
                        return invokeExternalBatchFunction(iterable, batchSchemaId, batchId, logContext);
                    } else {
                        throw new InvocationTargetException(new Throwable(isBlank(responsePayload.getStackTrace()) ? responsePayload.getException() : responsePayload.getStackTrace()), responsePayload.getException());
                    }
                }
                responseString = (responsePayload.getSkip()) ? null : responsePayload.getResponse();
            } else {
                StringBuilder json = new StringBuilder("{");
                json.append(Arrays.asList(Pair.of(NODE_NAME, "\"" + nodeName + "\""), Pair.of(SCHEMA_ID, "\"" + batchSchemaId + "\""), Pair.of("batchSize", String.valueOf(iterable.size())), Pair.of("payload", "[" + iterable.stream().map(o -> o.getValue().toString()).collect(Collectors.joining(",")) + "]"), Pair.of("logContext", jsonString(logContext))).stream().map(p -> String.format("\"%s\":%s", p.getKey(), p.getValue())).collect(Collectors.joining(",")));
                json.append("}");
                RequestBody body = RequestBody.create(json.toString(), MEDIA_TYPE_APPLICATION_JSON);
                Request httpRequest = new Request.Builder().url(this.payloadUri).post(body).build();
                OkHttpClient client = initClient();
                Response httpResponse = client.newCall(httpRequest).execute();
                int responseCode = httpResponse.code();
                String responseMessage = httpResponse.message();
                ResponseBody responseBody = httpResponse.body();
                responseString = responseBody == null ? null : responseBody.string();
                if (!httpResponse.isSuccessful()) throw new InvocationTargetException(new Throwable(responseMessage), "Non 200-OK response received for POST http request for url " + payloadUri + ". Response Code : " + responseCode + ". ");
            }
        } finally {
            if (useGrpc) returnGrpcToPool(pool, grpcClient);
        }
        return responseString;
    }

    @Override

    protected int consumeRaw(final List<KV<String, GenericRecord>> iterable, final Void batch, final WindowedContext context, final PaneInfo paneInfo, final BoundedWindow window, final Instant timestamp) throws Exception {
        if (options.isAsync()) return CompletableFuture.supplyAsync(func(iterable, context)).get();
        else return func(iterable, context).get();
    }

    @SuppressWarnings("unchecked")
    private Supplier<Integer> func(final List<KV<String, GenericRecord>> iterable, final WindowedContext context) {
        return () -> {
            long startTime = System.nanoTime();
            GenericRecord firstRecord = iterable.get(0).getValue();
            Schema batchSchema = firstRecord.getSchema();
            String batchSchemaId = batchSchema.getFullName();
            String batchId = String.format("batch-%s", UUID.randomUUID());
            counter(nodeName, nodeType, batchSchemaId, ELEMENTS_IN).inc(iterable.size());
            String responseString;
            try {
                ContextHandler.setContext(BeamContext.builder().nodeName(nodeName).nodeType(nodeType).schema(batchSchema).windowedContext(context).inputSchemaId(batchSchemaId).outputSchemaId(this.schemaId).converter(converter).build());
                Map<String, String> logContext;
                if (logContextTemplateName != null) {
                    try {
                        logContext = readJson(evaluateFreemarker(configuration(), logContextTemplateName, batchId, Map.of(), batchSchema, "records", iterable), Map.class);
                    } catch (Exception ex) {
                        LOGGER.warn("Could not evaluate log context to a valid json; Will continue processing without logContext", Map.of(ERROR_MESSAGE, String.valueOf(ex.getMessage()), "key", batchId, NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, batchSchemaId), ex);
                        logContext = Map.of();
                    }
                } else {
                    logContext = Map.of();
                }
                if (retry == null) {
                    responseString = invokeExternalBatchFunction(iterable, batchSchemaId, batchId, logContext);
                } else {
                    Map<String, String> finalLogContext = logContext;
                    responseString = applyWithRetry(retry, o -> invokeExternalBatchFunction(iterable, batchSchemaId, batchId, finalLogContext)).apply(null);
                }
                histogramDuration(nodeName, nodeType, batchSchemaId, "success_lambda").observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception e) {
                histogramDuration(nodeName, nodeType, batchSchemaId, "error_lambda").observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.warn("Error invoking External Batch Function", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, batchSchemaId, "key", batchId), e);
                iterable.forEach(kv -> context.output(ERROR_TAG, eRecord(nodeName, nodeType, "error_lambda", kv, e)));
                return 0;
            } finally {
                ContextHandler.clearContext();
                histogramDuration(nodeName, nodeType, batchSchemaId, "evaluate_lambda").observe((System.nanoTime() - startTime) / 1000000.0);
            }
            if (!emit) {
                histogramDuration(nodeName, nodeType, batchSchemaId, "process").observe((System.nanoTime() - startTime) / 1000000.0);
                return 0;
            }
            startTime = System.nanoTime();
            if (isBlank(responseString)) {
                iterable.forEach(kv -> counter(nodeName, nodeType, kv.getValue().getSchema().getFullName(), ELEMENTS_NULL).inc());
                LOGGER.debug("Batch External Lambda returned null", Map.of(SCHEMA_ID, batchSchemaId, NODE_NAME, nodeName, "key", batchId));
                histogramDuration(nodeName, nodeType, batchSchemaId, "batch_null_skip").observe((System.nanoTime() - startTime) / 1000000.0);
                return 0;
            }
            List outputs;
            try {
                outputs = readJson(responseString, List.class);
                if (outputs.isEmpty()) return 0;
            } catch (Exception ex) {
                LOGGER.warn("Error in parsing json string using the emitted response string", Map.of(ERROR_MESSAGE, String.valueOf(ex.getMessage()), NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, batchSchemaId, "key", batchId), ex);
                iterable.forEach(kv -> context.output(ERROR_TAG, eRecord(nodeName, nodeType, "parse_output_json", kv, ex)));
                return 0;
            }
            return emitBatchRecords(nodeName, nodeType, context, startTime, firstRecord, batchSchema, batchSchemaId, outputs, this.schemaOptions, this.emit, schemaMap, converterMap, this.schemaId, this.converter);
        };
    }

    @Override
    protected void closeBatch(final Void unused, final WindowedContext context, final PipelineConstants.BATCH_WRITER_CLOSE_TYPE type) throws Exception {

    }

    @Override
    protected Void initBatch() throws Exception {
        if (retry == null && options.getRetryOptions() != null) retry = retrySettings(nodeName, options.getRetryOptions());
        setup();
        return null;
    }
}
