package com.apple.aml.stargate.beam.sdk.ts;

import com.apple.aml.stargate.beam.inject.BeamContext;
import com.apple.aml.stargate.beam.sdk.io.file.AbstractWriter;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.BatchFunctionOptions;
import com.apple.aml.stargate.common.options.DerivedSchemaOptions;
import com.apple.aml.stargate.common.options.JavaFunctionOptions;
import com.apple.aml.stargate.common.pojo.AvroRecord;
import com.apple.aml.stargate.common.utils.ClassUtils;
import com.apple.aml.stargate.common.utils.ContextHandler;
import com.google.inject.Injector;
import io.github.resilience4j.retry.Retry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.tuple.Triple;
import org.joda.time.Instant;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.beam.sdk.ts.JavaFunction.emitOutput;
import static com.apple.aml.stargate.beam.sdk.utils.BeamUtils.getInjector;
import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.applyWindow;
import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.batchPartitionKey;
import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.batchWriter;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.GENERIC_RECORD_GROUP_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_NULL;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_LATEST_VERSION;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.SneakyRetryFunction.applyWithRetry;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.fetchDerivedSchema;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eJsonRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.retrySettings;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.saveLocalSchema;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class BatchFunction extends AbstractWriter<KV<String, KV<String, GenericRecord>>, Void> implements Serializable {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final long serialVersionUID = 1L;
    private String nodeName;
    private String nodeType;
    private boolean emit = true;
    private JavaFunctionOptions options;
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

    @SuppressWarnings("unchecked")
    public void initCommon(final Pipeline pipeline, final StargateNode node, final boolean emit) throws Exception {
        this.nodeName = node.getName();
        this.nodeType = node.getType();
        this.emit = emit;
        BatchFunctionOptions batchOptions = (BatchFunctionOptions) node.getConfig();
        this.options = ClassUtils.getAs(batchOptions, JavaFunctionOptions.class);
        this.options.initSchemaDeriveOptions();
        if (!isBlank(this.options.getSchemaId())) {
            this.schema = fetchSchemaWithLocalFallback(options.getSchemaReference(), options.getSchemaId());
            this.schemaId = schema.getFullName();
            this.converter = converter(this.schema);
            LOGGER.debug("BatchFunction schema converter created successfully for", Map.of(SCHEMA_ID, this.schemaId, NODE_NAME, nodeName));
        } else if (options.getSchema() != null && ("override".equalsIgnoreCase(options.getSchemaType()) || "replace".equalsIgnoreCase(options.getSchemaType()))) {
            this.schema = new Schema.Parser().parse(options.getSchema() instanceof String ? (String) options.getSchema() : jsonString(options.getSchema()));
            this.schemaId = schema.getFullName();
            this.converter = converter(schema, options);
            saveLocalSchema(schemaId, schema.toString());
            LOGGER.debug("BatchFunction schema converter created successfully using schema override for", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName, NODE_TYPE, nodeType));
        }
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return invoke(pipeline, node, collection, true);
    }

    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return invoke(pipeline, node, collection, false);
    }

    public SCollection<KV<String, GenericRecord>> invoke(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection, final boolean emit) throws Exception {
        BatchFunctionOptions options = (BatchFunctionOptions) node.getConfig();
        SCollection<KV<String, GenericRecord>> window = applyWindow(collection, options, node.getName());
        return window.group(node.name("partition-key"), batchPartitionKey(options.getPartitions(), options.getPartitionStrategy()), GENERIC_RECORD_GROUP_TAG).apply(node.name("batch"), batchWriter(node.getName(), node.getType(), node.environment(), options, this));
    }

    @Override
    protected void setup() throws Exception {
        functionInfo();
    }

    @SuppressWarnings("unchecked")
    private Triple<Function, Class, Boolean> functionInfo() throws Exception {
        if (functionInfo != null) return functionInfo;
        LOGGER.debug("Creating batch java lambda instance of", Map.of(SCHEMA_ID, String.valueOf(this.schemaId), NODE_NAME, nodeName, "className", options.getClassName()));
        Function function = (Function) injector().getInstance(Class.forName(options.getClassName()));
        if (this.schemaId == null) {
            Class returnType = returnType();
            if (!(returnType.isAssignableFrom(GenericRecord.class) || GenericRecord.class.isAssignableFrom(returnType) || returnType.isAssignableFrom(Map.class) || Map.class.isAssignableFrom(returnType))) {
                this.schema = fetchSchemaWithLocalFallback(options.getSchemaReference(), returnType.getCanonicalName());
                this.schemaId = this.schema.getFullName();
                this.converter = converter(this.schema);
                LOGGER.debug("BatchFunction schema converter created successfully using method returnType for", Map.of(SCHEMA_ID, this.schemaId, NODE_NAME, nodeName));
            }
        }
        Class paramType = paramType(function);
        boolean asis = paramType.equals(GenericRecord.class) || paramType.equals(AvroRecord.class);
        LOGGER.debug("BatchFunction Lambda instance created successfully", Map.of(SCHEMA_ID, String.valueOf(this.schemaId), NODE_NAME, nodeName, "className", options.getClassName()));
        if (retry == null && options.getRetryOptions() != null) retry = retrySettings(nodeName, options.getRetryOptions());
        functionInfo = Triple.of(function, paramType, asis);
        return functionInfo;
    }

    private Injector injector() throws Exception {
        if (injector != null) return injector;
        injector = getInjector(nodeName, options);
        return injector;
    }

    @SuppressWarnings("unchecked")
    private Class returnType() throws Exception {
        List<Method> methods = Arrays.stream(Class.forName(options.getClassName()).getMethods()).filter(m -> m.getName().equals("returnType") && !m.isDefault() && Modifier.isPublic(m.getModifiers()) && m.getParameterTypes().length == 0).collect(Collectors.toList());
        return methods.isEmpty() ? Map.class : (Class) methods.get(0).invoke(injector().getInstance(Class.forName(options.getClassName())));
    }

    @SuppressWarnings("unchecked")
    private Class paramType(final Function function) throws ClassNotFoundException, InvocationTargetException, IllegalAccessException {
        List<Method> methods = Arrays.stream(Class.forName(options.getClassName()).getMethods()).filter(m -> m.getName().equals("paramType") && !m.isDefault() && Modifier.isPublic(m.getModifiers()) && m.getParameterTypes().length == 0).collect(Collectors.toList());
        return methods.isEmpty() ? Map.class : (Class) methods.get(0).invoke(function);
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

    @Override
    @SuppressWarnings("unchecked")
    protected int consumeRaw(final List<KV<String, GenericRecord>> iterable, final Void batch, final WindowedContext context, final PaneInfo paneInfo, final BoundedWindow window, final Instant timestamp) throws Exception {
        long startTime = System.nanoTime();
        GenericRecord firstRecord = iterable.iterator().next().getValue();
        Schema batchSchema = firstRecord.getSchema();
        String batchSchemaId = batchSchema.getFullName();
        List outputs = null;
        try {
            Triple<Function, Class, Boolean> functionInfo = functionInfo();
            ContextHandler.setContext(BeamContext.builder().nodeName(nodeName).nodeType(nodeType).schema(batchSchema).windowedContext(context).inputSchemaId(batchSchemaId).outputSchemaId(this.schemaId).converter(converter).build());
            List inputs = new ArrayList<>();
            iterable.iterator().forEachRemaining(kv -> {
                GenericRecord record = kv.getValue();
                Schema schema = record.getSchema();
                String recordSchemaId = schema.getFullName();
                counter(nodeName, nodeType, recordSchemaId, ELEMENTS_IN).inc();
                try {
                    inputs.add(functionInfo.getRight() ? record : readJson(record.toString(), functionInfo.getMiddle()));
                } catch (Exception e) {
                    context.output(ERROR_TAG, eRecord(nodeName, nodeType, "error_conversion", kv, e));
                }
            });
            histogramDuration(nodeName, nodeType, batchSchemaId, "batch_conversion").observe((System.nanoTime() - startTime) / 1000000.0);
            startTime = System.nanoTime();
            if (retry == null) {
                outputs = (List) functionInfo.getLeft().apply(inputs);
            } else {
                outputs = (List) applyWithRetry(retry, functionInfo.getLeft()).apply(inputs);
            }
            histogramDuration(nodeName, nodeType, batchSchemaId, "success_lambda").observe((System.nanoTime() - startTime) / 1000000.0);
        } catch (Exception e) {
            histogramDuration(nodeName, nodeType, batchSchemaId, "error_lambda").observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.warn("Error invoking batch java Function", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, batchSchemaId), e);
            iterable.forEach(kv -> context.output(ERROR_TAG, eRecord(nodeName, nodeType, "error_lambda", kv, e)));
        } finally {
            ContextHandler.clearContext();
        }
        if (!emit) {
            histogramDuration(nodeName, nodeType, batchSchemaId, "process").observe((System.nanoTime() - startTime) / 1000000.0);
            return 0;
        }
        startTime = System.nanoTime();
        if (outputs == null) {
            iterable.forEach(kv -> counter(nodeName, nodeType, kv.getValue().getSchema().getFullName(), ELEMENTS_NULL).inc());
            LOGGER.debug("BatchFunction Lambda function returned null", Map.of("className", options.getClassName(), SCHEMA_ID, batchSchemaId, NODE_NAME, nodeName));
            histogramDuration(nodeName, nodeType, batchSchemaId, "batch_null_skip").observe((System.nanoTime() - startTime) / 1000000.0);
            return 0;
        }
        if (outputs.isEmpty()) return 0;
        return emitBatchRecords(nodeName, nodeType, context, startTime, firstRecord, batchSchema, batchSchemaId, outputs, this.options, this.emit, schemaMap, converterMap, this.schemaId, this.converter);
    }

    @SuppressWarnings("unchecked")
    public static int emitBatchRecords(final String nodeName, final String nodeType, final DoFn<KV<String, KV<String, GenericRecord>>, KV<String, GenericRecord>>.WindowedContext context, final long startTime, final GenericRecord firstRecord, final Schema batchSchema, final String batchSchemaId, final List outputs, final DerivedSchemaOptions schemaOptions, final boolean emit, final ConcurrentHashMap<String, Schema> schemaMap, final ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap, final String schemaId, final ObjectToGenericRecordConverter converter) {
        int version = firstRecord instanceof AvroRecord ? ((AvroRecord) firstRecord).getSchemaVersion() : SCHEMA_LATEST_VERSION;
        String recordSchemaKey = String.format("%s:%d", batchSchemaId, version);
        Schema targetSchema;
        String targetSchemaKey;
        if (emit && schemaOptions.deriveSchema()) {
            targetSchema = fetchDerivedSchema(schemaOptions, nodeName, recordSchemaKey, version, schemaMap, converterMap);
            targetSchemaKey = String.format("%s:%d", targetSchema.getFullName(), version);
        } else {
            targetSchema = batchSchema;
            targetSchemaKey = schemaId == null ? batchSchemaId : schemaId;
        }
        ObjectToGenericRecordConverter resolvedConverter = converter != null ? converter : converterMap.computeIfAbsent(targetSchemaKey, s -> converter(fetchSchemaWithLocalFallback(schemaOptions.getSchemaReference(), targetSchemaKey)));
        outputs.forEach(o -> {
            try {
                KV<String, GenericRecord> returnKV = emitOutput(KV.of(UUID.randomUUID().toString(), null), null, o, null, targetSchema, recordSchemaKey, targetSchemaKey, resolvedConverter, converterMap, nodeName, nodeType);
                if (returnKV != null) context.output(returnKV);
            } catch (Exception e) {
                context.output(ERROR_TAG, eJsonRecord(nodeName, nodeType, "error_emit", KV.of(UUID.randomUUID().toString(), o), e));
            }
        });
        histogramDuration(nodeName, nodeType, batchSchemaId, "batch_emit").observe((System.nanoTime() - startTime) / 1000000.0);
        return outputs.size();
    }

    @Override
    protected void closeBatch(final Void unused, final WindowedContext context, final PipelineConstants.BATCH_WRITER_CLOSE_TYPE type) throws Exception {

    }

    @Override
    protected Void initBatch() throws Exception {
        return null;
    }
}
