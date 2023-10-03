package com.apple.aml.stargate.beam.sdk.ts;

import com.apple.aml.stargate.pipeline.sdk.metrics.wrappers.CounterTemplateDirective;
import com.apple.aml.stargate.pipeline.sdk.metrics.wrappers.CounterWrapper;
import com.apple.aml.stargate.pipeline.sdk.metrics.wrappers.GaugeTemplateDirective;
import com.apple.aml.stargate.pipeline.sdk.metrics.wrappers.GaugeWrapper;
import com.apple.aml.stargate.pipeline.sdk.metrics.wrappers.HistogramTemplateDirective;
import com.apple.aml.stargate.pipeline.sdk.metrics.wrappers.HistogramWrapper;
import com.fasterxml.jackson.annotation.JsonIgnore;
import freemarker.template.Configuration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.tuple.Triple;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.COUNTER;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.GAUGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.HISTOGRAM;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;

public class MetricsEvaluator extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> implements Serializable {
    private static final long serialVersionUID = 1L;
    private String nodeName;
    private String nodeType;
    @JsonIgnore
    private transient Configuration configuration;
    @JsonIgnore
    private ConcurrentHashMap<Triple<String, String, String>, CounterTemplateDirective> counterDirectives = new ConcurrentHashMap<>();
    @JsonIgnore
    private ConcurrentHashMap<Triple<String, String, String>, GaugeTemplateDirective> gaugeDirectives = new ConcurrentHashMap<>();
    @JsonIgnore
    private ConcurrentHashMap<Triple<String, String, String>, HistogramTemplateDirective> histogramDirectives = new ConcurrentHashMap<>();
    private String metricsExpression;
    private String metricsExpressionTemplate;

    public MetricsEvaluator(final String nodeName, final String nodeType, final String metricsExpression) {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.metricsExpression = metricsExpression;
        this.metricsExpressionTemplate = nodeName + "~METRICS-EXPRESSION";
    }

    @Setup
    public void setup() throws Exception {
        configuration();
    }

    @SuppressWarnings("unchecked")
    private Configuration configuration() {
        if (configuration != null) return configuration;
        if (this.metricsExpression != null) loadFreemarkerTemplate(metricsExpressionTemplate, this.metricsExpression);
        configuration = freeMarkerConfiguration();
        return configuration;
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    @ProcessElement
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
        try {
            GenericRecord record = kv.getValue();
            Schema schema = record.getSchema();
            String schemaId = schema.getFullName();
            CounterTemplateDirective counterDirective = counterDirectives.computeIfAbsent(Triple.of(nodeName, nodeType, schemaId), t -> new CounterTemplateDirective(new CounterWrapper(t.getLeft(), t.getMiddle(), t.getRight())));
            GaugeTemplateDirective gaugeDirective = gaugeDirectives.computeIfAbsent(Triple.of(nodeName, nodeType, schemaId), t -> new GaugeTemplateDirective(new GaugeWrapper(t.getLeft(), t.getMiddle(), t.getRight())));
            HistogramTemplateDirective histogramDirective = histogramDirectives.computeIfAbsent(Triple.of(nodeName, nodeType, schemaId), t -> new HistogramTemplateDirective(new HistogramWrapper(t.getLeft(), t.getMiddle(), t.getRight())));
            evaluateFreemarker(configuration, metricsExpressionTemplate, kv.getKey(), kv.getValue(), schema, COUNTER, counterDirective, GAUGE, gaugeDirective, HISTOGRAM, histogramDirective);
        } catch (Exception e) {

        }
        ctx.output(kv);
    }
}
