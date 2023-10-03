package com.apple.aml.stargate.pipeline.sdk.ts;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.DerivedSchemaOptions;
import com.apple.aml.stargate.common.options.FieldExtractorMappingOptions;
import com.apple.aml.stargate.common.options.FreemarkerOptions;
import com.apple.aml.stargate.common.options.JavaFunctionOptions;
import com.apple.aml.stargate.common.pojo.AvroRecord;
import com.apple.aml.stargate.common.pojo.ErrorRecord;
import com.apple.aml.stargate.common.services.ErrorService;
import com.apple.aml.stargate.common.services.NodeService;
import com.apple.aml.stargate.common.utils.ContextHandler;
import com.apple.aml.stargate.common.utils.SchemaUtils;
import com.apple.aml.stargate.common.utils.TemplateUtils;
import com.apple.aml.stargate.pipeline.inject.FreemarkerFunctionService;
import com.apple.aml.stargate.pipeline.sdk.metrics.wrappers.CounterTemplateDirective;
import com.apple.aml.stargate.pipeline.sdk.metrics.wrappers.CounterWrapper;
import com.apple.aml.stargate.pipeline.sdk.metrics.wrappers.GaugeTemplateDirective;
import com.apple.aml.stargate.pipeline.sdk.metrics.wrappers.GaugeWrapper;
import com.apple.aml.stargate.pipeline.sdk.metrics.wrappers.HistogramTemplateDirective;
import com.apple.aml.stargate.pipeline.sdk.metrics.wrappers.HistogramWrapper;
import com.apple.aml.stargate.pipeline.sdk.metrics.wrappers.MetricTemplateDirective;
import com.google.inject.Injector;
import freemarker.ext.beans.BeansWrapper;
import freemarker.template.Configuration;
import freemarker.template.TemplateHashModel;
import freemarker.template.TemplateModel;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;

import java.io.Serializable;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.COUNTER;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.GAUGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.HISTOGRAM;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.KEY;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.METRIC;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.RECORD;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.SCHEMA;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.STATICS;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.UTIL;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_NULL;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_LATEST_VERSION;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.AvroUtils.avroSchema;
import static com.apple.aml.stargate.common.utils.AvroUtils.extractField;
import static com.apple.aml.stargate.common.utils.ClassUtils.getAs;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.HoconUtils.hoconToPojo;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.merge;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.JsonUtils.yamlToPojo;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.printers.BaseLogFns.log;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.consumeOutput;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchExtractorFields;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchTargetSchema;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.getInjector;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.saveLocalSchema;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class BaseFreemarkerEvaluator implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final Map<String, String> ALIAS_MAP = Map.of("hocon", "conf", "yaml", "yml");
    private static final MetricTemplateDirective METRIC_DIRECTIVE = new MetricTemplateDirective();
    private static final ConcurrentHashMap<Triple<String, String, String>, CounterTemplateDirective> counterDirectives = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Triple<String, String, String>, GaugeTemplateDirective> gaugeDirectives = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Triple<String, String, String>, HistogramTemplateDirective> histogramDirectives = new ConcurrentHashMap<>();
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final NodeService nodeService;
    private final ErrorService errorService;
    private boolean emit = true;
    private String nodeName;
    private String nodeType;
    private FreemarkerOptions options;
    private String expression;
    private String schemaId;
    private transient Configuration configuration;
    private ObjectToGenericRecordConverter converter;
    private transient ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap = new ConcurrentHashMap<>();
    private transient ConcurrentHashMap<String, Schema> schemaMap = new ConcurrentHashMap<>();
    private transient ConcurrentHashMap<String, Schema> fieldExtractorSchemaMap = new ConcurrentHashMap<>();
    private List<FieldExtractorMappingOptions> fields = null;
    private PipelineConstants.DATA_FORMAT outputType;
    private transient Class outputClass;
    private transient Function<String, Object> function;
    private transient Injector injector;
    private transient FreemarkerFunctionService service;
    private String keyTemplateName;
    private String expressionTemplateName;
    private String schemaIdTemplateName;

    public BaseFreemarkerEvaluator(final NodeService nodeService, final ErrorService errorService) {
        this.nodeService = nodeService;
        this.errorService = errorService;
    }

    @SuppressWarnings("unchecked")
    public void initFreemarkerNode(final StargateNode node, final boolean emit) throws Exception {
        this.emit = emit;
        FreemarkerOptions options = (FreemarkerOptions) node.getConfig();
        this.options = options;
        this.options.initSchemaDeriveOptions();
        this.expression = options.expression();
        this.nodeName = node.getName();
        this.nodeType = node.getType();
        this.schemaId = isBlank(options.getSchemaId()) ? null : options.getSchemaId().trim();
        if (this.schemaId != null) {
            Schema schema = fetchSchemaWithLocalFallback(this.options.getSchemaReference(), this.schemaId);
            this.converter = converter(schema, options);
            this.schemaId = schema.getFullName();
            LOGGER.debug("Freemarker schema converter created successfully for", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName, NODE_TYPE, nodeType));
        } else if (!isBlank(options.getClassName())) {
            Schema schema = avroSchema(Class.forName(this.options.getClassName().trim()));
            this.schemaId = schema.getFullName();
            this.converter = converter(schema, options);
            saveLocalSchema(schemaId, schema.toString());
            LOGGER.debug("Freemarker schema converter created successfully using className for", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName, NODE_TYPE, nodeType, "className", this.options.getClassName()));
        } else if (options.getSchema() != null && ("override".equalsIgnoreCase(options.getSchemaType()) || "replace".equalsIgnoreCase(options.getSchemaType()))) {
            Schema schema = new Schema.Parser().parse(options.getSchema() instanceof String ? (String) options.getSchema() : jsonString(options.getSchema()));
            this.schemaId = schema.getFullName();
            this.converter = converter(schema, options);
            saveLocalSchema(schemaId, schema.toString());
            LOGGER.debug("Freemarker schema converter created successfully using schema override for", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName, NODE_TYPE, nodeType));
        }
        if (this.options.getFields() != null) this.fields = fetchExtractorFields(this.options.getFields(), this.schemaId);
        this.outputType = options.getType() == null ? PipelineConstants.DATA_FORMAT.json : PipelineConstants.DATA_FORMAT.valueOf(ALIAS_MAP.getOrDefault(options.getType(), "json"));
        this.outputClass = outputClass();
        this.keyTemplateName = nodeName + "~KEY";
        this.expressionTemplateName = nodeName + "~EXPRESSION";
        this.schemaIdTemplateName = nodeName + "~SCHEMAID";
    }

    private Class outputClass() throws Exception {
        return (isBlank(options.getClassName())) ? Map.class : Class.forName(this.options.getClassName().trim());
    }

    @SuppressWarnings("unchecked")
    private Function<String, Object> function() throws Exception {
        if (this.function != null) {
            return this.function;
        }
        if (outputClass == null) outputClass = outputClass();
        if (this.outputType == PipelineConstants.DATA_FORMAT.json) {
            this.function = s -> {
                try {
                    return readJson(s, outputClass);
                } catch (Exception e) {
                    throw new GenericException("Error reading json", Map.of("json", String.valueOf(s)), e).wrap();
                }
            };
        } else if (this.outputType == PipelineConstants.DATA_FORMAT.yml) {
            this.function = s -> {
                try {
                    return yamlToPojo(s, outputClass);
                } catch (Exception e) {
                    throw new GenericException("Error reading yaml", Map.of("yaml", String.valueOf(s)), e).wrap();
                }
            };
        } else if (this.outputType == PipelineConstants.DATA_FORMAT.conf) {
            this.function = s -> {
                try {
                    return hoconToPojo(s, outputClass);
                } catch (Exception e) {
                    throw new GenericException("Error reading hocon", Map.of("hocon", String.valueOf(s)), e).wrap();
                }
            };
        } else {
            this.function = s -> {
                try {
                    return readJson(s, outputClass);
                } catch (Exception e) {
                    throw new GenericException("Error reading json", Map.of("json", String.valueOf(s)), e).wrap();
                }
            };
        }
        return this.function;
    }

    @SuppressWarnings("unchecked")
    private Configuration configuration() {
        if (configuration != null) {
            return configuration;
        }
        if (this.expression != null) {
            loadFreemarkerTemplate(expressionTemplateName, this.expression);
        }
        if (this.options.getKeyExpression() != null) {
            loadFreemarkerTemplate(keyTemplateName, this.options.getKeyExpression());
        }
        if (this.options.getSchemaIdExpression() != null) {
            loadFreemarkerTemplate(schemaIdTemplateName, this.options.getSchemaIdExpression());
        }
        configuration = freeMarkerConfiguration();
        return configuration;
    }

    public void setup() throws Exception {
        if (outputClass == null) outputClass = outputClass();
        if (converterMap == null) converterMap = new ConcurrentHashMap<>();
        if (schemaMap == null) schemaMap = new ConcurrentHashMap<>();
        if (fieldExtractorSchemaMap == null) fieldExtractorSchemaMap = new ConcurrentHashMap<>();
        function();
        configuration();
        service();
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

    @SuppressWarnings({"unchecked"})
    public void processElement(final KV<String, GenericRecord> kv, final Pair<String, Object> bean, final Consumer<KV<String, GenericRecord>> successConsumer, final Consumer<KV<String, ErrorRecord>> errorConsumer) throws Exception {
        long startTime = System.nanoTime();
        log(options, nodeName, nodeType, kv);
        GenericRecord record = kv.getValue();
        Schema recordSchema = record.getSchema();
        String recordSchemaId = recordSchema.getFullName();
        counter(nodeName, nodeType, recordSchemaId, ELEMENTS_IN).inc();
        Object response = null;
        Configuration configuration = configuration();
        String resolvedSchemaId = this.schemaId == null ? this.options.getSchemaIdExpression() == null ? recordSchemaId : evaluateFreemarker(configuration, schemaIdTemplateName, null, recordSchemaId, null) : this.schemaId;
        counter(nodeName, nodeType, resolvedSchemaId, "elements_in_resolved").inc();
        int version = record instanceof AvroRecord ? ((AvroRecord) record).getSchemaVersion() : SCHEMA_LATEST_VERSION;
        String recordSchemaKey = String.format("%s:%d", recordSchemaId, version);
        Schema targetSchema = null;
        String targetSchemaKey = null;
        if (this.expression == null) {
            response = record;
            if (emit && this.fields != null) {
                targetSchema = fieldExtractorSchemaMap.computeIfAbsent(recordSchemaKey, s -> fetchTargetSchema(fields, recordSchema));
                GenericRecord output = new GenericData.Record(targetSchema);
                for (FieldExtractorMappingOptions field : this.fields) {
                    output.put(field.getFieldName(), extractField(record, field.getMapping()));
                }
                response = output;
            }
        } else {
            try {
                ContextHandler.appendContext(ContextHandler.Context.builder().nodeName(nodeName).nodeType(nodeType).schema(recordSchema).inputSchemaId(recordSchemaId).outputSchemaId(resolvedSchemaId).build());
                if (isBlank(options.getReferenceType()) || "output".equalsIgnoreCase(options.getReferenceType())) {
                    String outputString = _evaluate(configuration, kv.getKey(), kv.getValue(), recordSchema, resolvedSchemaId, bean);
                    if (emit) response = function().apply(outputString);
                } else if ("merge".equalsIgnoreCase(options.getReferenceType()) || "enhance".equalsIgnoreCase(options.getReferenceType())) {
                    String outputString = _evaluate(configuration, kv.getKey(), kv.getValue(), recordSchema, resolvedSchemaId, bean);
                    if (emit) {
                        response = merge(readJsonMap(kv.getValue().toString()), outputString);
                        if (this.fields != null) {
                            targetSchema = fieldExtractorSchemaMap.computeIfAbsent(recordSchemaKey, s -> fetchTargetSchema(fields, recordSchema));
                            targetSchemaKey = String.format("%s:%d", targetSchema.getFullName(), version);
                            Map outputMap = new HashMap();
                            for (FieldExtractorMappingOptions field : this.fields) {
                                outputMap.put(field.getFieldName(), extractField((Map) response, field.getMapping()));
                            }
                            Schema finalTargetSchema = targetSchema;
                            response = converterMap.computeIfAbsent(targetSchemaKey, s -> converter(finalTargetSchema)).convert(outputMap);
                        }
                        response = getAs(response, outputClass);
                    }
                } else if ("inline".equalsIgnoreCase(options.getReferenceType()) || "direct".equalsIgnoreCase(options.getReferenceType())) {
                    response = readJsonMap(kv.getValue().toString());
                    _evaluate(configuration, kv.getKey(), response, recordSchema, resolvedSchemaId, bean);
                } else {
                    String outputString = _evaluate(configuration, kv.getKey(), kv.getValue(), recordSchema, resolvedSchemaId, bean);
                    if (emit) response = function().apply(outputString);
                }
                histogramDuration(nodeName, nodeType, resolvedSchemaId, "success_expression").observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception e) {
                histogramDuration(nodeName, nodeType, resolvedSchemaId, "error_expression").observe((System.nanoTime() - startTime) / 1000000.0);
                counter(nodeName, nodeType, resolvedSchemaId, ELEMENTS_ERROR).inc();
                LOGGER.warn("Error in evaluating freemarker", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "key", kv.getKey(), NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, recordSchemaId));
                errorConsumer.accept(eRecord(nodeName, nodeType, "eval_expression", kv, e));
                return;
            } finally {
                ContextHandler.clearContext();
                histogramDuration(nodeName, nodeType, resolvedSchemaId, "evaluate_expression").observe((System.nanoTime() - startTime) / 1000000.0);
            }
        }
        if (!emit) {
            histogramDuration(nodeName, nodeType, resolvedSchemaId, "process").observe((System.nanoTime() - startTime) / 1000000.0);
            return;
        }
        if (response == null) {
            histogramDuration(nodeName, nodeType, resolvedSchemaId, ELEMENTS_NULL).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.debug("Freemarker Evaluator returned null. Will skip this record", Map.of("className", this.outputClass, SCHEMA_ID, recordSchemaId, NODE_NAME, nodeName, "key", kv.getKey()));
            return;
        }
        String responseKey = null;
        if (options.getKeyExpression() != null) {
            long keyStartTime = System.nanoTime();
            responseKey = bean == null ? evaluateFreemarker(configuration, keyTemplateName, kv.getKey(), kv.getValue(), recordSchema) : evaluateFreemarker(configuration, keyTemplateName, kv.getKey(), kv.getValue(), recordSchema, bean.getKey(), bean.getValue());
            histogramDuration(nodeName, nodeType, resolvedSchemaId, "evaluate_key_expression").observe((System.nanoTime() - keyStartTime) / 1000000.0);
        }
        if (targetSchema == null) {
            if (this.emit && this.options.deriveSchema()) {
                targetSchema = fetchDerivedSchema(this.options, nodeName, recordSchemaKey, version, schemaMap, converterMap);
                targetSchemaKey = String.format("%s:%d", targetSchema.getFullName(), version);
            } else {
                targetSchema = recordSchema;
                targetSchemaKey = resolvedSchemaId;
            }
        }
        String finalTargetSchemaKey = targetSchemaKey;
        ObjectToGenericRecordConverter resolvedConverter = this.converter != null ? this.converter : converterMap.computeIfAbsent(targetSchemaKey, s -> converter(fetchSchemaWithLocalFallback(options.getSchemaReference(), finalTargetSchemaKey)));
        histogramDuration(nodeName, nodeType, resolvedSchemaId, "process").observe((System.nanoTime() - startTime) / 1000000.0);
        consumeOutput(kv, responseKey, response, targetSchema, recordSchemaKey, targetSchemaKey, resolvedConverter, converterMap, nodeName, nodeType, successConsumer);
    }

    @SneakyThrows
    public String _evaluate(final Configuration configuration, final String key, final Object record, final Schema schema, final String resolvedSchemaId, final Pair<String, Object> bean) {
        CounterTemplateDirective counterDirective = counterDirectives.computeIfAbsent(Triple.of(nodeName, nodeType, resolvedSchemaId), t -> new CounterTemplateDirective(new CounterWrapper(t.getLeft(), t.getMiddle(), t.getRight())));
        GaugeTemplateDirective gaugeDirective = gaugeDirectives.computeIfAbsent(Triple.of(nodeName, nodeType, resolvedSchemaId), t -> new GaugeTemplateDirective(new GaugeWrapper(t.getLeft(), t.getMiddle(), t.getRight())));
        HistogramTemplateDirective histogramDirective = histogramDirectives.computeIfAbsent(Triple.of(nodeName, nodeType, resolvedSchemaId), t -> new HistogramTemplateDirective(new HistogramWrapper(t.getLeft(), t.getMiddle(), t.getRight())));
        if (bean == null) {
            return evaluateFreemarker(configuration, expressionTemplateName, key, record, schema, COUNTER, counterDirective, GAUGE, gaugeDirective, HISTOGRAM, histogramDirective, "services", service());
        } else {
            return evaluateFreemarker(configuration, expressionTemplateName, key, record, schema, bean.getKey(), bean.getValue(), COUNTER, counterDirective, GAUGE, gaugeDirective, HISTOGRAM, histogramDirective, "services", service());
        }
    }

    @SuppressWarnings("deprecation")
    @SneakyThrows
    public static String evaluateFreemarker(final Configuration configuration, final String templateName, final String key, final Object record, final Schema schema, final Object... kvs) {
        StringWriter writer = new StringWriter();
        Map<String, Object> object = new HashMap<>();
        object.put(KEY, key == null ? "null" : key);
        object.put(RECORD, record == null ? Map.of() : record instanceof GenericRecord ? (record instanceof AvroRecord ? record : new AvroRecord((GenericRecord) record)) : record);
        if (schema != null) object.put(SCHEMA, freemarkerSchemaMap(schema, record instanceof AvroRecord ? ((AvroRecord) record).getSchemaVersion() : SCHEMA_LATEST_VERSION));
        TemplateHashModel hashModel = BeansWrapper.getDefaultInstance().getStaticModels();
        object.put(STATICS, hashModel);
        TemplateModel util = hashModel.get(TemplateUtils.class.getName());
        object.put(UTIL, util);
        object.put(METRIC, METRIC_DIRECTIVE);
        for (int i = 0; i < kvs.length; ) {
            String k = String.valueOf(kvs[i++]);
            Object v = kvs[i++];
            object.put(k, v);
        }
        configuration.getTemplate(templateName).process(object, writer);
        return writer.toString();
    }

    public static Map<String, String> freemarkerSchemaMap(final Schema schema, final int version) {
        return Map.of("name", schema.getName().replace('-', '_').toLowerCase(), "namespace", String.valueOf(schema.getNamespace()), "fullName", schema.getFullName(), "version", version <= -1 ? "latest" : String.valueOf(version));
    }

    public static Schema fetchDerivedSchema(final DerivedSchemaOptions options, final String nodeName, final String recordSchemaKey, final int version, final ConcurrentHashMap<String, Schema> schemaMap, final ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap) {
        return schemaMap.computeIfAbsent(recordSchemaKey, s -> {
            Schema existingSchema = fetchSchemaWithLocalFallback(options.getSchemaReference(), s);
            Schema derivedSchema = SchemaUtils.derivedSchema(existingSchema, nodeName, options.schemaOverride(), options.schemaIncludes(), options.schemaExcludes(), options.schemaRegex(), options.isEnableSimpleSchema(), options.isEnableHierarchicalSchemaFilters());
            String derivedSchemaId = derivedSchema.getFullName();
            String derivedSchemaKey = String.format("%s:%d", derivedSchemaId, version);
            saveLocalSchema(derivedSchemaKey, derivedSchema.toString());
            converterMap.computeIfAbsent(derivedSchemaKey, sid -> converter(derivedSchema));
            return derivedSchema;
        });
    }
}
