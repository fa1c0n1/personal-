package com.apple.aml.stargate.beam.sdk.ts;

import com.apple.aml.stargate.beam.inject.BeamContext;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.DerivedSchemaOptions;
import com.apple.aml.stargate.common.options.JavaFunctionOptions;
import com.apple.aml.stargate.common.pojo.AvroRecord;
import com.apple.aml.stargate.common.utils.ContextHandler;
import com.google.inject.Injector;
import io.github.resilience4j.retry.Retry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.beam.sdk.utils.BeamUtils.getInjector;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_NULL;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.SneakyRetryFunction.applyWithRetry;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.consumeOutput;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.consumeOutputRecords;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.retrySettings;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.saveLocalSchema;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class JavaFunction extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> implements Serializable {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final long serialVersionUID = 1L;
    private boolean emit = true;
    private JavaFunctionOptions options;
    private String nodeName;
    private String nodeType;
    private transient Triple<Function, Class, Boolean> functionInfo;
    private transient Injector injector;
    private transient Retry retry;
    private Schema schema;
    private String schemaId;
    private ConcurrentHashMap<String, Schema> schemaMap = new ConcurrentHashMap<>();
    private ObjectToGenericRecordConverter converter;
    private ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap = new ConcurrentHashMap<>();

    public void initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        initCommon(pipeline, node, true);
    }

    public void initWrite(final Pipeline pipeline, final StargateNode node) throws Exception {
        initCommon(pipeline, node, false);
    }

    public void initCommon(final Pipeline pipeline, final StargateNode node, final boolean emit) throws Exception {
        this.emit = emit;
        initJavaFunctionNode(pipeline, node, (JavaFunctionOptions) node.getConfig());
    }

    @SuppressWarnings("unchecked")
    public void initJavaFunctionNode(final Pipeline pipeline, final StargateNode node, final JavaFunctionOptions options) throws Exception {
        this.nodeName = node.getName();
        this.nodeType = node.getType();
        this.options = options;
        this.options.initSchemaDeriveOptions();
        if (!isBlank(this.options.getSchemaId())) {
            this.schema = fetchSchemaWithLocalFallback(options.getSchemaReference(), options.getSchemaId());
            this.schemaId = schema.getFullName();
            this.converter = converter(this.schema);
            LOGGER.debug("Java schema converter created successfully for", Map.of(SCHEMA_ID, this.schemaId, NODE_NAME, nodeName));
        } else if (options.getSchema() != null && ("override".equalsIgnoreCase(options.getSchemaType()) || "replace".equalsIgnoreCase(options.getSchemaType()))) {
            this.schema = new Schema.Parser().parse(options.getSchema() instanceof String ? (String) options.getSchema() : jsonString(options.getSchema()));
            this.schemaId = schema.getFullName();
            this.converter = converter(schema, options);
            saveLocalSchema(schemaId, schema.toString());
            LOGGER.debug("Java schema converter created successfully using schema override for", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName, NODE_TYPE, nodeType));
        }
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return collection.apply(node.getName(), this);
    }

    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return collection.apply(node.getName(), this);
    }

    @Setup
    public void setup() throws Exception {
        functionInfo();
    }

    @SuppressWarnings("unchecked")
    private Triple<Function, Class, Boolean> functionInfo() throws Exception {
        if (functionInfo != null) return functionInfo;
        LOGGER.debug("Creating java lambda instance of", Map.of(SCHEMA_ID, String.valueOf(this.schemaId), NODE_NAME, nodeName, "className", this.options.getClassName()));
        Function function = (Function) injector().getInstance(Class.forName(this.options.getClassName()));
        Method method = functionMethod();
        if (this.schemaId == null) {
            Class returnType = method.getReturnType();
            if (!(returnType.isAssignableFrom(GenericRecord.class) || GenericRecord.class.isAssignableFrom(returnType) || returnType.isAssignableFrom(Map.class) || Map.class.isAssignableFrom(returnType))) {
                this.schema = fetchSchemaWithLocalFallback(options.getSchemaReference(), returnType.getCanonicalName());
                this.schemaId = schema.getFullName();
                this.converter = converter(this.schema);
                LOGGER.debug("Java Lambda schema converter created successfully using method returnType for", Map.of(SCHEMA_ID, this.schemaId, NODE_NAME, nodeName));
            }
        }
        Class paramType = method.getParameterTypes()[0];
        boolean asis = paramType.equals(GenericRecord.class) || paramType.equals(AvroRecord.class);
        LOGGER.debug("Java Lambda instance created successfully", Map.of(SCHEMA_ID, String.valueOf(this.schemaId), NODE_NAME, nodeName, "className", this.options.getClassName()));
        if (retry == null && options.getRetryOptions() != null) retry = retrySettings(nodeName, options.getRetryOptions());
        functionInfo = Triple.of(function, paramType, asis);
        return functionInfo;
    }

    private Injector injector() throws Exception {
        if (injector != null) return injector;
        injector = getInjector(nodeName, options);
        return injector;
    }

    private Method functionMethod() throws Exception {
        return Arrays.stream(Class.forName(this.options.getClassName()).getMethods()).filter(m -> m.getName().equals("apply") && !m.isDefault() && Modifier.isPublic(m.getModifiers()) && m.getParameterTypes().length == 1).collect(Collectors.toList()).get(0);
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
        Object response;
        try {
            Triple<Function, Class, Boolean> functionInfo = functionInfo();
            ContextHandler.setContext(BeamContext.builder().nodeName(nodeName).nodeType(nodeType).schema(recordSchema).windowedContext(ctx).inputSchemaId(recordSchemaId).outputSchemaId(this.schemaId).converter(converter).build());
            Object input;
            if (functionInfo.getRight()) {
                input = record;
            } else {
                input = readJson(record.toString(), functionInfo.getMiddle());
                try {
                    if (input instanceof Map) {
                        Map map = (Map) input;
                        map.put("stargateKey", kv.getKey());
                        map.put("stargateSchemaId", recordSchemaId);
                    }
                } catch (Exception e) {
                }
            }
            if (retry == null) {
                response = functionInfo.getLeft().apply(input);
            } else {
                response = applyWithRetry(retry, functionInfo.getLeft()).apply(input);
            }
            histogramDuration(nodeName, nodeType, recordSchemaId, "success_lambda").observe((System.nanoTime() - startTime) / 1000000.0);
        } catch (Exception e) {
            histogramDuration(nodeName, nodeType, recordSchemaId, "error_lambda").observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.warn("Error invoking java Function", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "key", kv.getKey(), NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, recordSchemaId), e);
            counter(nodeName, nodeType, recordSchemaId, ELEMENTS_ERROR).inc();
            ctx.output(ERROR_TAG, eRecord(nodeName, nodeType, "invoke_lambda", kv, e));
            return;
        } finally {
            ContextHandler.clearContext();
            histogramDuration(nodeName, nodeType, recordSchemaId, "evaluate_lambda").observe((System.nanoTime() - startTime) / 1000000.0);
        }
        if (!emit) {
            histogramDuration(nodeName, nodeType, recordSchemaId, "process").observe((System.nanoTime() - startTime) / 1000000.0);
            return;
        }
        if (response == null) {
            counter(nodeName, nodeType, recordSchemaId, ELEMENTS_NULL).inc();
            LOGGER.debug("Java Lambda function returned null. Will skip this record", Map.of("className", this.options.getClassName(), SCHEMA_ID, recordSchemaId, NODE_NAME, nodeName));
            return;
        }
        emitRecords(nodeName, nodeType, kv, ctx, record, recordSchema, recordSchemaId, response, this.emit, this.options, schemaMap, converterMap, this.schemaId, this.converter);
    }


    public static KV<String, GenericRecord> emitOutput(final KV<String, GenericRecord> kv, final String responseKey, final Object response, final WindowedContext ctx, final Schema schema, final String recordSchemaKey, final String targetSchemaKey, final ObjectToGenericRecordConverter converter, final ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap, final String nodeName, final String nodeType) throws Exception {
        KV<String, GenericRecord> output = consumeOutput(kv, responseKey, response, schema, recordSchemaKey, targetSchemaKey, converter, converterMap, nodeName, nodeType, ctx == null ? skv -> {
        } : skv -> ctx.output(skv));
        return output;
    }

    @SuppressWarnings("unchecked")
    public static void emitRecords(final String nodeName, final String nodeType, @DoFn.Element final KV<String, GenericRecord> kv, final WindowedContext ctx, final GenericRecord record, final Schema recordSchema, final String recordSchemaId, final Object response, final boolean emit, final DerivedSchemaOptions options, final ConcurrentHashMap<String, Schema> schemaMap, final ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap, final String schemaId, final ObjectToGenericRecordConverter converter) throws Exception {
        consumeOutputRecords(nodeName, nodeType, kv, record, recordSchema, recordSchemaId, response, emit, options, schemaMap, converterMap, schemaId, converter, ctx == null ? skv -> {
        } : skv -> ctx.output(skv));
    }
}
