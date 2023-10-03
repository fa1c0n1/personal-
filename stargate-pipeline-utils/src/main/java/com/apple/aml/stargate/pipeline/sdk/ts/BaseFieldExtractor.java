package com.apple.aml.stargate.pipeline.sdk.ts;

import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.FieldExtractorMappingOptions;
import com.apple.aml.stargate.common.options.FieldExtractorOptions;
import com.apple.aml.stargate.common.options.JavaFunctionOptions;
import com.apple.aml.stargate.common.pojo.AvroRecord;
import com.apple.aml.stargate.common.pojo.ErrorRecord;
import com.apple.aml.stargate.common.services.ErrorService;
import com.apple.aml.stargate.common.services.NodeService;
import com.apple.aml.stargate.common.utils.ContextHandler;
import com.apple.aml.stargate.pipeline.inject.FreemarkerFunctionService;
import com.google.inject.Injector;
import freemarker.template.Configuration;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.PROCESS;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.PROCESS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.PROCESS_SUCCESS;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_LATEST_VERSION;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.AvroUtils.extractField;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.JsonUtils.merge;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.printers.BaseLogFns.log;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchExtractorFields;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchTargetSchema;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.getInjector;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class BaseFieldExtractor implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private String nodeType;
    private String nodeName;
    private FieldExtractorOptions options;
    private List<FieldExtractorMappingOptions> fields;
    private transient ConcurrentHashMap<String, Schema> schemaMap = new ConcurrentHashMap<>();
    private transient ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap = new ConcurrentHashMap<>();
    private String expressionTemplateName;
    private transient Configuration configuration;
    private transient Injector injector;
    private transient FreemarkerFunctionService service;
    private final NodeService nodeService;
    private final ErrorService errorService;

    public BaseFieldExtractor(final NodeService nodeService, final ErrorService errorService) {
        this.nodeService = nodeService;
        this.errorService = errorService;
    }

    @SuppressWarnings("unchecked")
    public void initTransform(final StargateNode node) throws Exception {
        FieldExtractorOptions options = (FieldExtractorOptions) node.getConfig();
        this.options = options;
        this.nodeName = node.getName();
        this.nodeType = node.getType();
        this.fields = fetchExtractorFields(this.options.getFields(), this.options.getSchemaId());
        this.expressionTemplateName = isBlank(this.options.getExpression()) ? null : (nodeName + "~EXPRESSION");
    }

    public void setup() throws Exception {
        configuration();
        service();
    }

    @SuppressWarnings("unchecked")
    public void processElement(final KV<String, GenericRecord> kv, final Consumer<KV<String, GenericRecord>> successConsumer, final Consumer<KV<String, ErrorRecord>> errorConsumer) throws Exception {
        long startTime = System.nanoTime();
        log(options, nodeName, nodeType, kv);
        GenericRecord record = kv.getValue();
        Schema recordSchema = record.getSchema();
        String recordSchemaId = recordSchema.getFullName();
        counter(nodeName, nodeType, recordSchemaId, ELEMENTS_IN).inc();
        int version = record instanceof AvroRecord ? ((AvroRecord) record).getSchemaVersion() : SCHEMA_LATEST_VERSION;
        String recordSchemaKey = String.format("%s:%d", recordSchemaId, version);
        try {
            ContextHandler.appendContext(ContextHandler.Context.builder().nodeName(nodeName).nodeType(nodeType).schema(recordSchema).inputSchemaId(recordSchemaId).build());
            Schema targetSchema = schemaMap.computeIfAbsent(recordSchemaKey, s -> isBlank(this.options.getSchemaId()) ? fetchTargetSchema(this.fields, recordSchema) : fetchSchemaWithLocalFallback(null, this.options.getSchemaId()));
            String targetSchemaId = targetSchema.getFullName();
            String targetSchemaKey = String.format("%s:%d", targetSchemaId, version);
            GenericRecord output;
            if (expressionTemplateName == null) {
                output = new GenericData.Record(targetSchema);
                for (FieldExtractorMappingOptions field : this.fields) {
                    output.put(field.getFieldName(), extractField(record, field.getMapping()));
                }
            } else {
                String outputString = evaluateFreemarker(configuration(), expressionTemplateName, kv.getKey(), record, recordSchema, "services", service());
                Map merged = merge(readJsonMap(record.toString()), outputString);
                Map outputMap = new HashMap();
                for (FieldExtractorMappingOptions field : this.fields) {
                    outputMap.put(field.getFieldName(), extractField(merged, field.getMapping()));
                }
                output = converterMap.computeIfAbsent(targetSchemaKey, s -> converter(targetSchema)).convert(outputMap);
            }
            incCounters(nodeName, nodeType, targetSchemaId, ELEMENTS_OUT, SOURCE_SCHEMA_ID, recordSchemaId);
            KV<String, GenericRecord> returnKV = KV.of(kv.getKey(), new AvroRecord(output, version));
            histogramDuration(nodeName, nodeType, recordSchemaId, PROCESS_SUCCESS).observe((System.nanoTime() - startTime) / 1000000.0);
            successConsumer.accept(returnKV);
        } catch (Exception e) {
            histogramDuration(nodeName, nodeType, recordSchemaId, PROCESS_ERROR).observe((System.nanoTime() - startTime) / 1000000.0);
            counter(nodeName, nodeType, recordSchemaId, ELEMENTS_ERROR).inc();
            LOGGER.warn("Error in extracting fields", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "key", kv.getKey(), NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, recordSchemaId));
            errorConsumer.accept(eRecord(nodeName, nodeType, PROCESS_ERROR, kv, e));
            return;
        } finally {
            ContextHandler.clearContext();
            histogramDuration(nodeName, nodeType, recordSchemaId, PROCESS).observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

    @SuppressWarnings("unchecked")
    private Configuration configuration() {
        if (configuration != null) return configuration;
        if (this.expressionTemplateName != null) loadFreemarkerTemplate(expressionTemplateName, this.options.getExpression());
        configuration = freeMarkerConfiguration();
        return configuration;
    }

    private Injector injector() throws Exception {
        if (injector != null) return injector;
        JavaFunctionOptions javaFunctionOptions = new JavaFunctionOptions();
        javaFunctionOptions.setClassName(FreemarkerFunctionService.class.getCanonicalName());
        injector = getInjector(nodeName, javaFunctionOptions, this.nodeService, this.errorService);
        return injector;
    }

    @SneakyThrows
    private FreemarkerFunctionService service() {
        if (service != null) return service;
        service = injector().getInstance(FreemarkerFunctionService.class);
        return service;
    }
}
