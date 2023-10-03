package com.apple.aml.stargate.beam.sdk.ts;

import com.apple.aml.stargate.beam.inject.BeamContext;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.DerivedSchemaOptions;
import com.apple.aml.stargate.common.options.ExternalFunctionOptions;
import com.apple.aml.stargate.common.options.ExternalTransformOptions;
import com.apple.aml.stargate.common.utils.ClassUtils;
import com.apple.aml.stargate.common.utils.ContextHandler;
import com.apple.aml.stargate.common.utils.WebUtils;
import com.apple.aml.stargate.external.rpc.proto.ExternalFunctionProtoServiceGrpc;
import com.apple.aml.stargate.external.rpc.proto.ExternalFunctionProtoServiceGrpc.ExternalFunctionProtoServiceBlockingStub;
import com.apple.aml.stargate.external.rpc.proto.ExternalFunctionProtoServiceGrpc.ExternalFunctionProtoServiceStub;
import com.apple.aml.stargate.external.rpc.proto.InitPayload;
import com.apple.aml.stargate.external.rpc.proto.InitStatus;
import com.apple.aml.stargate.external.rpc.proto.RequestPayload;
import com.apple.aml.stargate.external.rpc.proto.ResponsePayload;
import com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils;
import freemarker.template.Configuration;
import io.github.resilience4j.retry.Retry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.beam.sdk.ts.Dhari.resolveDhariOptions;
import static com.apple.aml.stargate.beam.sdk.ts.JavaFunction.emitRecords;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.KEY;
import static com.apple.aml.stargate.common.constants.CommonConstants.MEDIA_TYPE_APPLICATION_JSON;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_NULL;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.ClassUtils.getAs;
import static com.apple.aml.stargate.common.utils.ClassUtils.parseBoolean;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
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

public class ExternalFunction extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> implements Serializable {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final long serialVersionUID = 1L;
    private static final ConcurrentHashMap<String, ExternalFunctionOptions> FUNCTION_OPTIONS_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, GenericObjectPool<ExternalFunctionProtoServiceBlockingStub>> GRPC_BLOCKING_POOL_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, GenericObjectPool<ExternalFunctionProtoServiceStub>> GRPC_NON_BLOCKING_POOL_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Boolean> INIT_STATUS_MAP = new ConcurrentHashMap<>();
    private boolean emit = true;
    private ExternalFunctionOptions options;
    private DerivedSchemaOptions schemaOptions;
    private String nodeName;
    private String nodeType;
    private String expression;
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

    public static GenericObjectPool<ExternalFunctionProtoServiceBlockingStub> grpcPool(final String nodeName, final ExternalTransformOptions options) {
        return GRPC_BLOCKING_POOL_MAP.computeIfAbsent(nodeName, n -> grpcPool(options));
    }

    public static GenericObjectPool<ExternalFunctionProtoServiceStub> grpcNonBlockingPool(final String nodeName, final ExternalTransformOptions options) {
        return GRPC_NON_BLOCKING_POOL_MAP.computeIfAbsent(nodeName, n -> grpcNonBlockingPool(options));
    }

    @SuppressWarnings("unchecked")
    public static void returnGrpcToPool(GenericObjectPool pool, AbstractStub client) {
        try {
            if (pool == null) return;
            if (client != null) pool.returnObject(client);
        } catch (Exception ex) {
            LOGGER.trace("Could not return grpc client to the pool. Reason : " + ex.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> initializeStub(final boolean useGrpc, final String nodeName, final String nodeType, final ExternalFunctionOptions options, final String schemaId, final String expression) throws Exception {
        FUNCTION_OPTIONS_MAP.computeIfAbsent(nodeName, n -> options);
        HashMap<String, Object> details = new HashMap<>();
        details.put(NODE_NAME, nodeName);
        details.put(NODE_TYPE, nodeType);
        details.put("context", options.context());
        if (schemaId != null) details.put("schemaId", schemaId);
        details.put("isTemplate", expression != null);
        if (expression != null) details.put("template", expression);
        if (options.getFunctionOptions() != null) details.put("functionOptions", options.getFunctionOptions());
        if (options.getDhariOptions() != null) details.put("dhariOptions", resolveDhariOptions(options.getDhariOptions()));
        if (options.getAttributesOptions() != null) details.put("attributesOptions", options.getAttributesOptions());
        if (options.getAttributesOptions() != null) details.put("lookupOptions", options.getAttributesOptions());
        if (options.getAttributesOptions() != null) details.put("shuriOfsOptions", options.getAttributesOptions());
        if (options.getSolrOptions() != null) details.put("solrOptions", options.getSolrOptions());
        if (options.getSnowflakeOptions() != null) details.put("snowflakeOptions", options.getSnowflakeOptions());
        if (options.getJdbcOptions() != null) details.put("jdbcOptions", options.getJdbcOptions());
        if (useGrpc) {
            GenericObjectPool<ExternalFunctionProtoServiceBlockingStub> pool = null;
            ExternalFunctionProtoServiceBlockingStub client = null;
            try {
                pool = grpcPool(nodeName, options.connectionOptions());
                client = pool.borrowObject();
                String nodeConfig = jsonString(details);
                InitPayload.Builder builder = InitPayload.newBuilder().setNodeName(nodeName).setNodeConfig(nodeConfig);
                InitStatus status = client.init(builder.build());
                return Map.of("status", status.getStatus(), "message", status.getMessage());
            } finally {
                returnGrpcToPool(pool, client);
            }
        } else {
            return WebUtils.postData(options.connectionOptions().initializeUri(), details, MEDIA_TYPE_APPLICATION_JSON, Map.class);
        }
    }

    @SneakyThrows
    private static GenericObjectPool<ExternalFunctionProtoServiceBlockingStub> grpcPool(final ExternalTransformOptions _options) {
        ExternalTransformOptions options = _options == null ? new ExternalTransformOptions() : _options;
        ExternalFunctionProtoServicePoolConfig poolConfig = getAs(options.getGrpcPoolConfig() == null ? Map.of() : options.getGrpcPoolConfig(), ExternalFunctionProtoServicePoolConfig.class);
        return new GenericObjectPool<>(new ProtoServiceFactory(options.host(), options.port()), poolConfig);
    }

    @SneakyThrows
    private static GenericObjectPool<ExternalFunctionProtoServiceStub> grpcNonBlockingPool(final ExternalTransformOptions _options) {
        ExternalTransformOptions options = _options == null ? new ExternalTransformOptions() : _options;
        ExternalFunctionProtoServiceNonBlockingPoolConfig poolConfig = getAs(options.getGrpcPoolConfig() == null ? Map.of() : options.getGrpcPoolConfig(), ExternalFunctionProtoServiceNonBlockingPoolConfig.class);
        return new GenericObjectPool<>(new NonBlockingProtoServiceFactory(options.host(), options.port()), poolConfig);
    }

    @SneakyThrows
    public static GenericObjectPool<ExternalFunctionProtoServiceBlockingStub> grpcPool(final String host, final int port) {
        ExternalFunctionProtoServicePoolConfig poolConfig = new ExternalFunctionProtoServicePoolConfig();
        return new GenericObjectPool<>(new ProtoServiceFactory(host, port), poolConfig);
    }

    public static ExternalFunctionOptions externalFunctionOptions(final String nodeName) {
        return FUNCTION_OPTIONS_MAP.get(nodeName);
    }

    public static Collection<GenericObjectPool<ExternalFunctionProtoServiceBlockingStub>> grpcPools() {
        return GRPC_BLOCKING_POOL_MAP.values();
    }

    public void initCommon(final Pipeline pipeline, final StargateNode node, final boolean emit) throws Exception {
        this.nodeName = node.getName();
        this.nodeType = node.getType();
        this.emit = emit;
        this.options = (ExternalFunctionOptions) node.getConfig();
        if (options.connectionOptions() == null) options.connectionOptions(new ExternalTransformOptions());
        this.schemaOptions = ClassUtils.getAs(options, DerivedSchemaOptions.class);
        this.logContextTemplateName = isBlank(options.getLogContext()) ? null : (nodeName + "~logContext");
        PipelineConstants.EXTERNAL_FUNC funcType = PipelineConstants.EXTERNAL_FUNC.funcType(this.options.getRuntimeType(), nodeType);
        if (options.connectionOptions().getPort() <= 0) options.connectionOptions().setPort(funcType.portNo());
        this.expression = this.options.expression();
        if (isBlank(options.connectionOptions().getExecutionType())) options.connectionOptions().setExecutionType(isBlank(this.expression) ? "lambda" : "template");
        this.payloadUri = options.connectionOptions().executeUri();
        this.useGrpc = isBlank(options.connectionOptions().getScheme()) || "grpc".equalsIgnoreCase(options.connectionOptions().getScheme());
        resolveDhariOptions(options.getDhariOptions());
        this.schemaOptions.initSchemaDeriveOptions();
        if (!isBlank(this.schemaOptions.getSchemaId())) {
            this.schema = fetchSchemaWithLocalFallback(options.getSchemaReference(), schemaOptions.getSchemaId());
            this.schemaId = schema.getFullName();
            this.converter = converter(this.schema);
            LOGGER.debug("External Function schema converter created successfully for", Map.of(SCHEMA_ID, this.schemaId, NODE_NAME, nodeName));
        } else if (options.getSchema() != null && ("override".equalsIgnoreCase(options.getSchemaType()) || "replace".equalsIgnoreCase(options.getSchemaType()))) {
            this.schema = new Schema.Parser().parse(options.getSchema() instanceof String ? (String) options.getSchema() : jsonString(options.getSchema()));
            this.schemaId = schema.getFullName();
            this.converter = converter(schema, options);
            saveLocalSchema(schemaId, schema.toString());
            LOGGER.debug("External Function schema converter created successfully using schema override for", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName, NODE_TYPE, nodeType));
        }
    }

    public void initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        initCommon(pipeline, node, true);
    }

    public void initWrite(final Pipeline pipeline, final StargateNode node) throws Exception {
        initCommon(pipeline, node, false);
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> inputCollection) throws Exception {
        return inputCollection.apply(node.getName(), this);
    }

    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return collection.apply(node.getName(), this);
    }

    @SuppressWarnings("unchecked")
    @Setup
    public void setup() throws Exception {
        INIT_STATUS_MAP.computeIfAbsent(nodeName, n -> {
            Map<String, Object> response = Map.of();
            try {
                response = initializeStub(useGrpc, nodeName, nodeType, options, this.schemaId, expression);
                boolean initialized = response == null || parseBoolean(response.get("status"));
                if (!initialized) throw new InstantiationException(String.valueOf(response.get("message")));
                LOGGER.debug("External Function initialized successfully!!", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType), response);
                return true;
            } catch (Exception e) {
                LOGGER.error("Error in initializing External Function", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, ERROR_MESSAGE, String.valueOf(e.getMessage())), response, e);
                throw new RuntimeException(e);
            }
        });
        configuration();
    }

    @StartBundle
    public void startBundle() throws Exception {
        initClient();
        if (retry == null && options.getRetryOptions() != null) retry = retrySettings(nodeName, options.getRetryOptions());
        setup();
    }

    private freemarker.template.Configuration configuration() {
        if (configuration != null) return configuration;
        if (logContextTemplateName != null) loadFreemarkerTemplate(logContextTemplateName, this.options.getLogContext());
        configuration = freeMarkerConfiguration();
        return configuration;
    }

    private OkHttpClient initClient() throws Exception {
        if (okHttpClient == null) okHttpClient = newOkHttpClient(options.connectionOptions());
        return okHttpClient;
    }

    @SneakyThrows
    private String invokeExternalFunction(final KV<String, GenericRecord> kv, final String recordSchemaId, final Map<String, String> logContext) {
        if (useGrpc) return blockingResponse(getRequestPayload(kv, recordSchemaId, logContext));
        StringBuilder json = new StringBuilder("{");
        json.append(Arrays.asList(Pair.of(NODE_NAME, "\"" + nodeName + "\""), Pair.of(SCHEMA_ID, "\"" + recordSchemaId + "\""), Pair.of(KEY, "\"" + kv.getKey() + "\""), Pair.of("payload", kv.getValue().toString()), Pair.of("logContext", jsonString(logContext))).stream().map(p -> String.format("\"%s\":%s", p.getKey(), p.getValue())).collect(Collectors.joining(",")));
        json.append("}");
        RequestBody body = RequestBody.create(json.toString(), MEDIA_TYPE_APPLICATION_JSON);
        Request httpRequest = new Request.Builder().url(this.payloadUri).post(body).build();
        OkHttpClient client = initClient();
        Response httpResponse = client.newCall(httpRequest).execute();
        int responseCode = httpResponse.code();
        String responseMessage = httpResponse.message();
        ResponseBody responseBody = httpResponse.body();
        String responseString = responseBody == null ? null : responseBody.string();
        if (!httpResponse.isSuccessful()) throw new InvocationTargetException(new Throwable(responseMessage), "Non 200-OK response received for POST http request for url " + payloadUri + ". Response Code : " + responseCode + ". ");
        return responseString;
    }

    @NotNull
    private RequestPayload getRequestPayload(final KV<String, GenericRecord> kv, final String recordSchemaId, final Map<String, String> logContext) {
        return RequestPayload.newBuilder().setNodeName(nodeName).setSchemaId(recordSchemaId).setPayloadKey(kv.getKey()).setPayload(kv.getValue().toString()).putAllLogContext(logContext).build();
    }

    private String blockingResponse(final RequestPayload request) throws Exception {
        GenericObjectPool<ExternalFunctionProtoServiceBlockingStub> pool = null;
        ExternalFunctionProtoServiceBlockingStub client = null;
        ResponsePayload response;
        try {
            pool = grpcPool(nodeName, options.connectionOptions());
            client = pool.borrowObject();
            response = client.apply(request);
        } finally {
            returnGrpcToPool(pool, client);
        }
        if (isBlank(response.getException())) return (response.getSkip()) ? null : response.getResponse();
        if (!ErrorUtils.MISSING_INIT_CONFIG_ERROR.equalsIgnoreCase(response.getException())) throw new InvocationTargetException(new Throwable(isBlank(response.getStackTrace()) ? response.getException() : response.getStackTrace()), response.getException());
        INIT_STATUS_MAP.remove(nodeName);
        setup();
        return blockingResponse(request);
    }

    @SuppressWarnings("unchecked")
    @ProcessElement
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
        long startTime = System.nanoTime();
        log(options, nodeName, nodeType, kv);
        GenericRecord record = kv.getValue();
        Schema recordSchema = record.getSchema();
        String recordSchemaId = recordSchema.getFullName();
        counter(nodeName, nodeType, recordSchemaId, ELEMENTS_IN).inc();
        String responseString = null;
        boolean async = options.isAsync() && useGrpc;
        try {
            ContextHandler.setContext(BeamContext.builder().nodeName(nodeName).nodeType(nodeType).schema(recordSchema).windowedContext(ctx).inputSchemaId(recordSchemaId).outputSchemaId(this.schemaId).converter(converter).build());
            Map<String, String> logContext;
            if (logContextTemplateName != null) {
                try {
                    logContext = readJson(evaluateFreemarker(configuration(), logContextTemplateName, kv.getKey(), record, recordSchema), Map.class);
                } catch (Exception ex) {
                    LOGGER.warn("Could not evaluate log context to a valid json; Will continue processing without logContext", Map.of(ERROR_MESSAGE, String.valueOf(ex.getMessage()), "key", kv.getKey(), NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, recordSchemaId), ex);
                    logContext = Map.of();
                }
            } else {
                logContext = Map.of();
            }
            if (async) {
                asyncInvokeExternalFunction(kv, ctx, startTime, record, recordSchema, recordSchemaId, logContext);
                return;
            }
            if (retry == null) {
                responseString = invokeExternalFunction(kv, recordSchemaId, logContext);
            } else {
                Map<String, String> finalLogContext = logContext;
                responseString = applyWithRetry(retry, o -> invokeExternalFunction(kv, recordSchemaId, finalLogContext)).apply(null);
            }
            histogramDuration(nodeName, nodeType, recordSchemaId, "success_lambda").observe((System.nanoTime() - startTime) / 1000000.0);
        } catch (Exception e) {
            handleError(kv, ctx, startTime, recordSchemaId, e);
            return;
        } finally {
            ContextHandler.clearContext();
            if (!async) histogramDuration(nodeName, nodeType, recordSchemaId, "evaluate_lambda").observe((System.nanoTime() - startTime) / 1000000.0);
        }
        processResult(kv, ctx, startTime, record, recordSchema, recordSchemaId, responseString);
    }

    private void asyncInvokeExternalFunction(final KV<String, GenericRecord> kv, final DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>>.ProcessContext ctx, final long startTime, final GenericRecord record, final Schema recordSchema, final String recordSchemaId, final Map<String, String> logContext) throws Exception {
        GenericObjectPool<ExternalFunctionProtoServiceStub> pool = null;
        ExternalFunctionProtoServiceStub client = null;
        try {
            pool = grpcNonBlockingPool(nodeName, options.connectionOptions());
            client = pool.borrowObject();
            client.apply(getRequestPayload(kv, recordSchemaId, logContext), new StreamObserver<>() {
                @Override
                public void onNext(final ResponsePayload response) {
                    if (isBlank(response.getException())) {
                        String responseString = response.getSkip() ? null : response.getResponse();
                        histogramDuration(nodeName, nodeType, recordSchemaId, "success_lambda").observe((System.nanoTime() - startTime) / 1000000.0);
                        processResult(kv, ctx, startTime, record, recordSchema, recordSchemaId, responseString);
                        return;
                    }
                    if (!ErrorUtils.MISSING_INIT_CONFIG_ERROR.equalsIgnoreCase(response.getException())) {
                        handleError(kv, ctx, startTime, recordSchemaId, new InvocationTargetException(new Throwable(isBlank(response.getStackTrace()) ? response.getException() : response.getStackTrace()), response.getException()));
                        return;
                    }
                    INIT_STATUS_MAP.remove(nodeName);
                    try {
                        setup();
                        asyncInvokeExternalFunction(kv, ctx, startTime, record, recordSchema, recordSchemaId, logContext);
                    } catch (Exception e) {
                        onError(e);
                    }
                }

                @Override
                public void onError(final Throwable t) {
                    handleError(kv, ctx, startTime, recordSchemaId, new InvocationTargetException(t));
                }

                @Override
                public void onCompleted() {
                    histogramDuration(nodeName, nodeType, recordSchemaId, "evaluate_lambda").observe((System.nanoTime() - startTime) / 1000000.0);
                }
            });
        } finally {
            returnGrpcToPool(pool, client);
        }
    }

    private void handleError(final KV<String, GenericRecord> kv, final DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>>.ProcessContext ctx, final long startTime, final String recordSchemaId, final Exception e) {
        histogramDuration(nodeName, nodeType, recordSchemaId, "error_lambda").observe((System.nanoTime() - startTime) / 1000000.0);
        LOGGER.warn("Error invoking external Function", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "key", kv.getKey(), NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, recordSchemaId), e);
        counter(nodeName, nodeType, recordSchemaId, ELEMENTS_ERROR).inc();
        ctx.output(ERROR_TAG, eRecord(nodeName, nodeType, "invoke_lambda", kv, e));
    }

    private Void processResult(final KV<String, GenericRecord> kv, final DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>>.ProcessContext ctx, final long startTime, final GenericRecord record, final Schema recordSchema, final String recordSchemaId, final String responseString) {
        if (!emit) {
            histogramDuration(nodeName, nodeType, recordSchemaId, "process").observe((System.nanoTime() - startTime) / 1000000.0);
            return null;
        }
        if (isBlank(responseString)) {
            counter(nodeName, nodeType, recordSchemaId, ELEMENTS_NULL).inc();
            LOGGER.debug("External Lambda function returned null. Will skip this record", Map.of(SCHEMA_ID, recordSchemaId, NODE_NAME, nodeName));
            return null;
        }
        Object response;
        try {
            response = responseString.stripLeading().startsWith("[") ? readJson(responseString, List.class) : readJsonMap(responseString);
        } catch (Exception ex) {
            LOGGER.warn("Error in parsing json string using the emitted response string", Map.of(ERROR_MESSAGE, String.valueOf(ex.getMessage()), "key", kv.getKey(), NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, recordSchemaId), ex);
            counter(nodeName, nodeType, recordSchemaId, ELEMENTS_ERROR).inc();
            ctx.output(ERROR_TAG, eRecord(nodeName, nodeType, "parse_output_json", kv, ex));
            return null;
        }
        try {
            emitRecords(nodeName, nodeType, kv, ctx, record, recordSchema, recordSchemaId, response, this.emit, this.schemaOptions, schemaMap, converterMap, this.schemaId, this.converter);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return null;
    }

    public static final class ExternalFunctionProtoServicePoolConfig extends GenericObjectPoolConfig<ExternalFunctionProtoServiceBlockingStub> {

    }

    public static final class ExternalFunctionProtoServiceNonBlockingPoolConfig extends GenericObjectPoolConfig<ExternalFunctionProtoServiceStub> {

    }

    public static final class ProtoServiceFactory extends BasePooledObjectFactory<ExternalFunctionProtoServiceBlockingStub> {
        private final String host;
        private final int port;

        public ProtoServiceFactory(final String host, final int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public ExternalFunctionProtoServiceBlockingStub create() throws Exception {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
            return ExternalFunctionProtoServiceGrpc.newBlockingStub(channel);
        }

        @Override
        public PooledObject<ExternalFunctionProtoServiceBlockingStub> wrap(final ExternalFunctionProtoServiceBlockingStub obj) {
            return new DefaultPooledObject<>(obj);
        }
    }

    public static final class NonBlockingProtoServiceFactory extends BasePooledObjectFactory<ExternalFunctionProtoServiceStub> {
        private final String host;
        private final int port;

        public NonBlockingProtoServiceFactory(final String host, final int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public ExternalFunctionProtoServiceStub create() throws Exception {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
            return ExternalFunctionProtoServiceGrpc.newStub(channel);
        }

        @Override
        public PooledObject<ExternalFunctionProtoServiceStub> wrap(final ExternalFunctionProtoServiceStub obj) {
            return new DefaultPooledObject<>(obj);
        }
    }
}
