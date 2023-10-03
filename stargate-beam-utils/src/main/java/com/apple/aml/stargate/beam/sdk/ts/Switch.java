package com.apple.aml.stargate.beam.sdk.ts;

import com.apple.aml.stargate.beam.inject.BeamContext;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.beam.sdk.values.SMapCollection;
import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.JavaFunctionOptions;
import com.apple.aml.stargate.common.options.SwitchOptions;
import com.apple.aml.stargate.common.pojo.ErrorRecord;
import com.apple.aml.stargate.common.utils.ContextHandler;
import com.apple.aml.stargate.pipeline.inject.FreemarkerFunctionService;
import com.google.inject.Injector;
import freemarker.template.Configuration;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.beam.sdk.utils.BeamUtils.getInjector;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.OUTPUT_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.DEFAULT_DELIMITER;
import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_SKIPPED;
import static com.apple.aml.stargate.common.constants.CommonConstants.NOT_APPLICABLE_HYPHEN;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.utils.ClassUtils.parseBoolean;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class Switch extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private String nodeName;
    private String nodeType;
    private SwitchOptions options;
    private Map<String, String> rules;
    private Map<String, TupleTag<KV<String, GenericRecord>>> tags;
    private String defaultRule;
    private transient Configuration configuration;
    private transient Injector injector;
    private transient FreemarkerFunctionService service;
    private String expressionTemplateName;

    @SuppressWarnings("unchecked")
    public void initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        this.nodeName = node.getName();
        this.nodeType = node.getType();
        this.options = (SwitchOptions) node.getConfig();
        if (this.options.getRules() instanceof String) {
            rules = new HashMap<>();
            Arrays.stream((((String) this.options.getRules()).trim()).split(DEFAULT_DELIMITER)).forEach(s -> rules.put(s, null));
        } else if (this.options.getRules() instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) this.options.getRules();
            rules = new HashMap<>();
            map.forEach((k, v) -> {
                String expression = v.toString();
                if (isBlank(expression)) return;
                rules.put(k, expression);
            });
        } else {
            rules = new HashMap<>();
            ((List<Object>) this.options.getRules()).forEach(o -> {
                if (o instanceof String) {
                    rules.put((String) o, null);
                } else if (o instanceof Map) {
                    Map<String, Object> map = new HashMap<>((Map<String, Object>) o);
                    String rule = (String) map.remove("rule");
                    if (rule == null) rule = (String) map.remove("name");
                    if (isBlank(rule)) return;
                    if (map.keySet().isEmpty()) {
                        rules.put((String) o, null);
                        return;
                    }
                    String expression = (String) map.get(map.keySet().iterator().next());
                    if (isBlank(expression)) {
                        rules.put((String) o, null);
                    } else {
                        rules.put((String) o, expression);
                    }
                }
            });
        }
        if (rules.isEmpty()) throw new InvalidInputException("rules cannot be empty");
        if (rules.entrySet().stream().filter(a -> a.getValue() == null).findAny().isPresent()) {
            if (isBlank(options.getExpression())) throw new InvalidInputException("switch expression cannot be empty");
            this.expressionTemplateName = nodeName + "~EXPRESSION";
        }
        defaultRule = isBlank(options.getDefaultRule()) ? null : options.getDefaultRule();
        Set<String> allRules = new HashSet<>(rules.keySet());
        allRules.add(defaultRule);
        tags = new HashMap<>(allRules.size());
        allRules.forEach(rule -> {
            String tagName = String.format("%s_%s", nodeName, rule).replace('-', '_').toLowerCase();
            TupleTag<KV<String, GenericRecord>> tag = new TupleTag<>(tagName) {
            };
            tags.put(rule, tag);
        });
    }

    public SMapCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        PCollectionList<KV<String, ErrorRecord>> errors = collection.getErrors();
        PCollection<KV<String, GenericRecord>> root = collection.collection();
        List<TupleTag<?>> additionalTags = new ArrayList<>(tags.values());
        additionalTags.add(ERROR_TAG);
        PCollectionTuple tuple = root.apply(node.getName(), ParDo.of(this).withOutputTags(OUTPUT_TAG, TupleTagList.of(additionalTags)));
        errors = errors.and(tuple.get(ERROR_TAG));
        Map<String, SCollection<KV<String, GenericRecord>>> map = new HashMap<>();
        map.put(EMPTY_STRING, SCollection.of(pipeline, tuple.get(OUTPUT_TAG), errors));
        tags.forEach((rule, tag) -> map.put(rule, SCollection.of(pipeline, tuple.get(tag))));
        return SMapCollection.of(map);
    }

    @Setup
    public void setup() throws Exception {
        configuration();
        service();
    }

    @SuppressWarnings("unchecked")
    private Configuration configuration() {
        if (configuration != null) return configuration;
        if (this.expressionTemplateName == null) {
            rules.forEach((k, v) -> loadFreemarkerTemplate(String.format("%s~case~%s", nodeName, k), v));
        } else {
            loadFreemarkerTemplate(expressionTemplateName, this.options.getExpression());
        }
        configuration = freeMarkerConfiguration();
        return configuration;
    }

    private Injector injector() throws Exception {
        if (injector != null) return injector;
        JavaFunctionOptions javaFunctionOptions = new JavaFunctionOptions();
        javaFunctionOptions.setClassName(FreemarkerFunctionService.class.getCanonicalName());
        injector = getInjector(nodeName, javaFunctionOptions);
        return injector;
    }

    @SneakyThrows
    private FreemarkerFunctionService service() {
        if (service != null) return service;
        service = injector().getInstance(FreemarkerFunctionService.class);
        return service;
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    @ProcessElement
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
        long startTime = System.nanoTime();
        log(options, nodeName, nodeType, kv);
        String key = kv.getKey();
        GenericRecord record = kv.getValue();
        Schema recordSchema = record.getSchema();
        String recordSchemaId = recordSchema.getFullName();
        counter(nodeName, nodeType, recordSchemaId, ELEMENTS_IN).inc();
        Map<String, TupleTag<KV<String, GenericRecord>>> emitMap = new HashMap<>();
        try {
            ContextHandler.setContext(BeamContext.builder().nodeName(nodeName).nodeType(nodeType).schema(recordSchema).windowedContext(ctx).inputSchemaId(recordSchemaId).outputSchemaId(recordSchemaId).build());
            boolean accepted = false;
            if (this.expressionTemplateName == null) {
                Stream<Boolean> stream = rules.keySet().stream().map(rule -> {
                    TupleTag<KV<String, GenericRecord>> tag = tags.get(rule);
                    boolean emit;
                    try {
                        String expressionValue = evaluateFreemarker(configuration, String.format("%s~case~%s", nodeName, rule), key, record, recordSchema, "services", service());
                        emit = parseBoolean(expressionValue);
                    } catch (Exception e) {
                        LOGGER.debug("Exception evaluating rule. Will assume rule evaluated to false", Map.of("key", String.valueOf(key), "rule", rule, NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, recordSchemaId, ERROR_MESSAGE, String.valueOf(e.getMessage())));
                        emit = false;
                    }
                    if (emit) emitMap.put(rule, tag);
                    return emit;
                });
                accepted = options.isEnableMultiMatch() ? (stream.reduce((a, b) -> a || b).get()) : (stream.filter(a -> a.booleanValue()).findFirst().isPresent());
            } else {
                try {
                    String expressionValue = evaluateFreemarker(configuration, expressionTemplateName, key, record, recordSchema, "services", service());
                    expressionValue = expressionValue.trim();
                    TupleTag<KV<String, GenericRecord>> tag = tags.get(expressionValue);
                    if (tag != null) {
                        emitMap.put(expressionValue, tag);
                        accepted = true;
                    }
                } catch (Exception e) {
                    LOGGER.debug("Exception evaluating expression. Will emit to default output path", Map.of("key", String.valueOf(key), NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, recordSchemaId, ERROR_MESSAGE, String.valueOf(e.getMessage())));
                }
            }
            if (!accepted) {
                if (defaultRule == null) emitMap.put(EMPTY_STRING, OUTPUT_TAG);
                else emitMap.put(defaultRule, OUTPUT_TAG);
            }
            histogramDuration(nodeName, nodeType, recordSchemaId, "success_eval_rule").observe((System.nanoTime() - startTime) / 1000000.0);
        } catch (Exception e) { // this block should ideally never execute
            histogramDuration(nodeName, nodeType, recordSchemaId, "error_eval_rule").observe((System.nanoTime() - startTime) / 1000000.0);
            counter(nodeName, nodeType, recordSchemaId, ELEMENTS_ERROR).inc();
            LOGGER.warn("Error in evaluating rule", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "key", String.valueOf(key), NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, recordSchemaId));
            ctx.output(ERROR_TAG, eRecord(nodeName, nodeType, "evaluate_rule", kv, e));
            return;
        } finally {
            ContextHandler.clearContext();
            histogramDuration(nodeName, nodeType, recordSchemaId, "evaluate_rule").observe((System.nanoTime() - startTime) / 1000000.0);
        }
        if (emitMap.isEmpty()) { // this block should ideally never execute
            counter(nodeName, nodeType, recordSchemaId, ELEMENTS_SKIPPED).inc();
        } else {
            emitMap.forEach((rule, tag) -> {
                incCounters(nodeName, nodeType, recordSchemaId, ELEMENTS_OUT, "rule", isBlank(rule) ? NOT_APPLICABLE_HYPHEN : rule);
                ctx.output(tag, kv);
            });
        }
        histogramDuration(nodeName, nodeType, recordSchemaId, "process").observe((System.nanoTime() - startTime) / 1000000.0);
    }
}
