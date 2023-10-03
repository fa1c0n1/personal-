package com.apple.aml.stargate.beam.sdk.ts;

import com.apple.aml.stargate.beam.inject.BeamContext;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.JavaFunctionOptions;
import com.apple.aml.stargate.common.options.MetricNodeOptions;
import com.apple.aml.stargate.common.services.PrometheusService;
import com.apple.aml.stargate.common.utils.ContextHandler;
import com.apple.aml.stargate.common.utils.PrometheusUtils;
import com.apple.aml.stargate.pipeline.inject.FreemarkerFunctionService;
import com.google.inject.Injector;
import freemarker.template.Configuration;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Set;

import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.beam.sdk.utils.BeamUtils.getInjector;
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
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.lang.Double.parseDouble;

public class MetricNode extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> implements Serializable {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private String nodeName;
    private String nodeType;
    private boolean emit = true;
    private MetricNodeOptions options;
    private String expressionTemplateName;
    private transient Configuration configuration;
    private transient FreemarkerFunctionService service;
    private transient Injector injector;
    private Set<String> counters;
    private Set<String> histograms;

    @SuppressWarnings("unchecked")
    public void initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        this.options = (MetricNodeOptions) node.getConfig();
        this.nodeName = node.getName();
        this.nodeType = node.getType();
        this.expressionTemplateName = nodeName + "~EXPRESSION";
        this.counters = this.options.counters();
        this.histograms = this.options.histograms();
    }

    public void initWrite(final Pipeline pipeline, final StargateNode node) throws Exception {
        initTransform(pipeline, node);
        this.emit = false;
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return collection.apply(node.getName(), this);
    }

    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return collection.apply(node.getName(), this);
    }

    @Setup
    public void setup() throws Exception {
        configuration();
        service();
    }

    @SuppressWarnings("unchecked")
    private Configuration configuration() {
        if (configuration != null) return configuration;
        loadFreemarkerTemplate(expressionTemplateName, this.options.getExpression());
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

    @ProcessElement
    @SuppressWarnings("unchecked")
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
        long startTime = System.nanoTime();
        log(options, nodeName, nodeType, kv);
        GenericRecord record = kv.getValue();
        Schema recordSchema = record.getSchema();
        String recordSchemaId = recordSchema.getFullName();
        counter(nodeName, nodeType, recordSchemaId, ELEMENTS_IN).inc();
        try {
            ContextHandler.setContext(BeamContext.builder().nodeName(nodeName).nodeType(nodeType).schema(recordSchema).windowedContext(ctx).inputSchemaId(recordSchemaId).build());
            String outputString = evaluateFreemarker(configuration(), expressionTemplateName, kv.getKey(), record, recordSchema, "services", service());
            if (!isBlank(outputString)) {
                PrometheusService prometheusService = PrometheusUtils.metricsService();
                readJsonMap(outputString).forEach((k, v) -> {
                    if (v == null) return;
                    String metricName = k.toString().trim();
                    String metricType;
                    double metricValue = 0;
                    String[] labelNames;
                    String[] labelValues;
                    if (v instanceof Map) {
                        Map details = (Map) v;
                        metricType = (String) details.getOrDefault("type", "gauge");
                        Object value = details.get("value");
                        metricValue = value instanceof Number ? ((Number) value).doubleValue() : parseDouble(value.toString().trim());
                        Map<String, String> labels = (Map<String, String>) details.get("labels");
                        if (labels == null || labels.isEmpty()) {
                            labelNames = new String[0];
                            labelValues = new String[0];
                        } else {
                            labelNames = labels.keySet().toArray(new String[labels.size()]);
                            labelValues = labels.values().toArray(new String[labels.size()]);
                        }
                    } else {
                        metricType = counters.contains(metricName) ? "counter" : (histograms.contains(metricName) ? "histogram" : "gauge");
                        metricValue = parseDouble(v.toString().trim());
                        labelNames = new String[0];
                        labelValues = new String[0];
                    }
                    metricType = metricType.toLowerCase();
                    switch (metricType) {
                        case "histogram":
                            prometheusService.newPipelineHistogram(metricName, labelNames);
                            prometheusService.pipelineHistogram(metricName, labelValues).observe(metricValue);
                            break;
                        case "counter":
                            prometheusService.newPipelineCounter(metricName, labelNames);
                            prometheusService.pipelineCounter(metricName, labelValues).inc(metricValue);
                            break;
                        default:
                            prometheusService.newPipelineGauge(metricName, labelNames);
                            prometheusService.pipelineGauge(metricName, labelValues).set(metricValue);
                    }
                });
            }
            incCounters(nodeName, nodeType, recordSchemaId, ELEMENTS_OUT, SOURCE_SCHEMA_ID, recordSchemaId);
            histogramDuration(nodeName, nodeType, recordSchemaId, PROCESS_SUCCESS).observe((System.nanoTime() - startTime) / 1000000.0);
        } catch (Exception e) {
            histogramDuration(nodeName, nodeType, recordSchemaId, PROCESS_ERROR).observe((System.nanoTime() - startTime) / 1000000.0);
            counter(nodeName, nodeType, recordSchemaId, ELEMENTS_ERROR).inc();
            LOGGER.warn("Error in evaluating metric node; Will ignore this and continue", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "key", kv.getKey(), NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, recordSchemaId));
        } finally {
            ContextHandler.clearContext();
            histogramDuration(nodeName, nodeType, recordSchemaId, PROCESS).observe((System.nanoTime() - startTime) / 1000000.0);
        }
        ctx.output(kv);
    }
}
