package com.apple.aml.stargate.pipeline.sdk.ts;

import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.FreemarkerFunctionOptions;
import com.apple.aml.stargate.common.pojo.ErrorRecord;
import com.apple.aml.stargate.common.services.ErrorService;
import com.apple.aml.stargate.common.services.NodeService;
import com.apple.aml.stargate.common.utils.ContextHandler;
import com.apple.aml.stargate.pipeline.inject.FreemarkerFunctionService;
import com.google.inject.Injector;
import freemarker.template.Configuration;
import io.github.resilience4j.retry.Retry;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_NULL;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.SneakyRetryFunction.applyWithRetry;
import static com.apple.aml.stargate.pipeline.sdk.printers.BaseLogFns.log;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.consumeOutputRecords;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.getInjector;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.retrySettings;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.saveLocalSchema;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class BaseFreemarkerFunction implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final NodeService nodeService;
    private final ErrorService errorService;
    private boolean emit = true;
    private FreemarkerFunctionOptions options;
    private String nodeName;
    private String nodeType;
    private transient Injector injector;
    private Schema schema;
    private String schemaId;
    private transient ConcurrentHashMap<String, Schema> schemaMap = new ConcurrentHashMap<>();
    private ObjectToGenericRecordConverter converter;
    private transient ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap = new ConcurrentHashMap<>();
    private String expressionTemplateName;
    private transient Configuration configuration;
    private transient FreemarkerFunctionService service;
    private transient Retry retry;

    public BaseFreemarkerFunction(final NodeService nodeService, final ErrorService errorService) {
        this.nodeService = nodeService;
        this.errorService = errorService;
    }

    public void initCommon(final StargateNode node, final boolean emit) throws Exception {
        this.nodeName = node.getName();
        this.nodeType = node.getType();
        this.emit = emit;
        this.options = (FreemarkerFunctionOptions) node.getConfig();
        this.options.setClassName(FreemarkerFunctionService.class.getCanonicalName());
        this.options.initSchemaDeriveOptions();
        if (!isBlank(this.options.getSchemaId())) {
            this.schema = fetchSchemaWithLocalFallback(options.getSchemaReference(), options.getSchemaId());
            this.schemaId = schema.getFullName();
            this.converter = converter(this.schema);
            LOGGER.debug("Freemarker function schema converter created successfully for", Map.of(SCHEMA_ID, this.schemaId, NODE_NAME, nodeName, NODE_TYPE, nodeType));
        } else if (options.getSchema() != null && ("override".equalsIgnoreCase(options.getSchemaType()) || "replace".equalsIgnoreCase(options.getSchemaType()))) {
            this.schema = new Schema.Parser().parse(options.getSchema() instanceof String ? (String) options.getSchema() : jsonString(options.getSchema()));
            this.schemaId = schema.getFullName();
            this.converter = converter(schema, options);
            saveLocalSchema(schemaId, schema.toString());
            LOGGER.debug("Freemarker function schema converter created successfully using schema override for", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName, NODE_TYPE, nodeType));
        }
        this.expressionTemplateName = nodeName + "~EXPRESSION";
    }

    public void setup() throws Exception {
        if (converterMap == null) converterMap = new ConcurrentHashMap<>();
        if (schemaMap == null) schemaMap = new ConcurrentHashMap<>();
        configuration();
        service();
        if (retry == null && options.getRetryOptions() != null) retry = retrySettings(nodeName, options.getRetryOptions());
    }

    @SuppressWarnings("unchecked")
    private Configuration configuration() {
        if (configuration != null) return configuration;
        loadFreemarkerTemplate(expressionTemplateName, this.options.getExpression());
        configuration = freeMarkerConfiguration();
        return configuration;
    }

    @SneakyThrows
    private FreemarkerFunctionService service() {
        if (service != null) return service;
        service = injector().getInstance(FreemarkerFunctionService.class);
        return service;
    }

    private Injector injector() throws Exception {
        if (injector != null) return injector;
        injector = getInjector(nodeName, options, this.nodeService, this.errorService);
        return injector;
    }

    @SuppressWarnings({"unchecked"})
    public void processElement(final KV<String, GenericRecord> kv, final Consumer<KV<String, GenericRecord>> successConsumer, final Consumer<KV<String, ErrorRecord>> errorConsumer) throws Exception {
        long startTime = System.nanoTime();
        log(options, nodeName, nodeType, kv);
        GenericRecord record = kv.getValue();
        Schema recordSchema = record.getSchema();
        String recordSchemaId = recordSchema.getFullName();
        counter(nodeName, nodeType, recordSchemaId, ELEMENTS_IN).inc();
        Object response = null;
        try {
            ContextHandler.appendContext(ContextHandler.Context.builder().nodeName(nodeName).nodeType(nodeType).schema(recordSchema).inputSchemaId(recordSchemaId).build());
            String outputString;
            if (retry == null) {
                outputString = evaluateFreemarker(configuration(), expressionTemplateName, kv.getKey(), kv.getValue(), recordSchema, "services", service());
            } else {
                outputString = applyWithRetry(retry, o -> evaluateFreemarker(configuration(), expressionTemplateName, kv.getKey(), kv.getValue(), recordSchema, "services", service())).apply(null);
            }
            if (emit) {
                outputString = outputString.trim();
                if (!outputString.isBlank()) {
                    if (outputString.startsWith("{")) {
                        response = readJsonMap(outputString);
                    } else if (outputString.startsWith("[")) {
                        response = readJson(outputString, List.class);
                    }
                }
            }
            histogramDuration(nodeName, nodeType, recordSchemaId, "success_lambda").observe((System.nanoTime() - startTime) / 1000000.0);
        } catch (Exception e) {
            histogramDuration(nodeName, nodeType, recordSchemaId, "error_lambda").observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.warn("Error invoking freemarker Function", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "key", kv.getKey(), NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, recordSchemaId), e);
            counter(nodeName, nodeType, recordSchemaId, ELEMENTS_ERROR).inc();
            errorConsumer.accept(eRecord(nodeName, nodeType, "invoke_lambda", kv, e));
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
            histogramDuration(nodeName, nodeType, recordSchemaId, ELEMENTS_NULL).observe((System.nanoTime() - startTime) / 1000000.0);
            return;
        }
        consumeOutputRecords(nodeName, nodeType, kv, record, recordSchema, recordSchemaId, response, this.emit, this.options, schemaMap, converterMap, this.schemaId, this.converter, successConsumer);
    }
}
