package com.apple.aml.stargate.beam.sdk.io.splunk;

import com.apple.aml.stargate.common.options.SplunkOptions;
import com.apple.aml.stargate.common.utils.AppConfig;
import freemarker.template.Configuration;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.splunk.SplunkEvent;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.utils.AvroUtils.getFieldValue;
import static com.apple.aml.stargate.common.utils.FormatUtils.parseDateTime;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.JsonUtils.filteredMap;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.NetworkUtils.hostName;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;

public class SplunkEventTransformer extends DoFn<KV<String, GenericRecord>, SplunkEvent> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final String nodeName;
    private final String nodeType;
    private final String expressionTemplateName;
    private final String expression;
    private final boolean convertToKVPairs;
    private final boolean skipNullValues;
    private final String timeAttribute;
    private final String hostAttribute;
    private final String sourceAttribute;
    private final String sourceTypeAttribute;
    private final String indexAttribute;
    private final String defaultHost;
    private final String defaultSource;
    private final String defaultSourceType;
    private final String defaultIndex;
    private Configuration configuration;

    public SplunkEventTransformer(final String nodeName, final String nodeType, final SplunkOptions options) throws Exception {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        expressionTemplateName = options.getExpression() == null ? null : (nodeName + "~EXPRESSION");
        expression = options.getExpression();
        convertToKVPairs = options.isKvPairs();
        skipNullValues = options.isSkipNullValues();
        timeAttribute = options.getTimeAttribute() == null || options.getTimeAttribute().trim().isBlank() ? "timestamp" : options.getTimeAttribute().trim();
        hostAttribute = options.getHostAttribute() == null || options.getHostAttribute().trim().isBlank() ? null : options.getHostAttribute().trim();
        sourceAttribute = options.getSourceAttribute() == null || options.getSourceAttribute().trim().isBlank() ? null : options.getSourceAttribute().trim();
        sourceTypeAttribute = options.getSourceTypeAttribute() == null || options.getSourceTypeAttribute().trim().isBlank() ? null : options.getSourceTypeAttribute().trim();
        indexAttribute = options.getIndexAttribute() == null || options.getIndexAttribute().trim().isBlank() ? null : options.getIndexAttribute().trim();
        defaultHost = options.getHost() == null || options.getHost().trim().isBlank() ? null : options.getHost().trim();
        defaultSource = options.getSource() == null || options.getSource().trim().isBlank() ? AppConfig.appName().toLowerCase() : options.getSource().trim();
        defaultSourceType = options.getSourceType() == null || options.getSourceType().trim().isBlank() ? null : options.getSourceType().trim();
        defaultIndex = options.getIndex() == null || options.getIndex().trim().isBlank() ? null : options.getIndex().trim();
    }

    @SuppressWarnings("unchecked")
    @ProcessElement
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
        String schemaId = kv.getValue().getSchema().getFullName();
        counter(nodeName, nodeType, schemaId, ELEMENTS_IN).inc();
        long startTime = System.nanoTime();
        try {
            ctx.output(fetchSplunkEvent(kv));
            histogramDuration(nodeName, nodeType, kv.getValue().getSchema().getFullName(), "process").observe((System.nanoTime() - startTime) / 1000000.0);
            incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, SOURCE_SCHEMA_ID, schemaId);
        } catch (Exception e) {
            histogramDuration(nodeName, nodeType, kv.getValue().getSchema().getFullName(), "error").observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.warn("Could not convert/emit SplunkEvent", kv.getKey(), Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "key", kv.getKey()), e);
            counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
            ctx.output(ERROR_TAG, eRecord(nodeName, nodeType, "splunk_event_conversion", kv, e));
        }
    }

    @SuppressWarnings("unchecked")
    public SplunkEvent fetchSplunkEvent(final KV<String, GenericRecord> kv) throws Exception {
        Map<String, Object> map = fetchSplunkEventMap(kv);
        SplunkEvent.Builder builder = SplunkEvent.newBuilder().withTime((Long) map.get("time")).withEvent((String) map.get("event")).withHost((String) map.get("host")).withSource((String) map.get("source")).withIndex((String) map.get("index"));
        String sourceType = (String) map.get("sourceType");
        if (sourceType != null) {
            builder = builder.withSourceType(sourceType);
        }
        return builder.create();
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> fetchSplunkEventMap(final KV<String, GenericRecord> kv) throws Exception {
        GenericRecord record = kv.getValue();
        Object time = getFieldValue(record, timeAttribute);
        String event;
        String host;
        String source;
        String sourceType;
        String index;
        Map<String, Object> splunkEvent = new HashMap<>();
        if (expressionTemplateName == null) {
            if (convertToKVPairs) {
                event = record.getSchema().getFields().stream().map(f -> Pair.of(f.name(), record.get(f.name()))).filter(p -> p.getValue() != null).map(e -> String.format("%s=\"%s\"", e.getKey(), String.valueOf(e.getValue()).replace('"', '\''))).collect(Collectors.joining(" "));
            } else if (skipNullValues) {
                event = jsonString(filteredMap(record, o -> o == null));
            } else {
                event = record.toString();
            }
            if (hostAttribute == null) {
                host = defaultHost;
            } else {
                Object _host = getFieldValue(record, hostAttribute);
                host = _host == null ? defaultHost : _host.toString();
            }
            if (sourceAttribute == null) {
                source = defaultSource;
            } else {
                Object _source = getFieldValue(record, sourceAttribute);
                source = _source == null ? defaultSource : _source.toString();
            }
            if (sourceTypeAttribute == null) {
                sourceType = defaultSourceType;
            } else {
                Object _sourceType = getFieldValue(record, sourceTypeAttribute);
                sourceType = _sourceType == null ? defaultSourceType : _sourceType.toString();
            }
            if (indexAttribute == null) {
                index = defaultIndex;
            } else {
                Object _index = getFieldValue(record, indexAttribute);
                index = _index == null ? defaultIndex : _index.toString();
            }
        } else {
            Configuration configuration = configuration();
            String str = evaluateFreemarker(configuration, expressionTemplateName, kv.getKey(), record, record.getSchema());
            if (convertToKVPairs) {
                Map<Object, Object> map = readJsonMap(str);
                event = map.entrySet().stream().filter(e -> e.getValue() != null).map(e -> String.format("%s=\"%s\"", e.getKey().toString(), String.valueOf(e.getValue()).replace('"', '\''))).collect(Collectors.joining(" "));
                host = hostAttribute == null ? defaultHost : (String) map.getOrDefault(hostAttribute, defaultHost);
                source = sourceAttribute == null ? defaultSource : (String) map.getOrDefault(sourceAttribute, defaultSource);
                sourceType = sourceTypeAttribute == null ? defaultSourceType : (String) map.getOrDefault(sourceTypeAttribute, defaultSourceType);
                index = indexAttribute == null ? defaultIndex : (String) map.getOrDefault(indexAttribute, defaultIndex);
                time = map.get(timeAttribute);
            } else {
                event = str.trim();
                host = defaultHost;
                source = defaultSource;
                sourceType = defaultSourceType;
                index = defaultIndex;
            }
        }
        splunkEvent.put("time", (time == null ? Instant.now() : parseDateTime(getFieldValue(record, timeAttribute))).toEpochMilli());
        splunkEvent.put("event", event);
        splunkEvent.put("host", host == null ? hostName() : host);
        splunkEvent.put("source", source);
        splunkEvent.put("index", index);
        if (sourceType != null) {
            splunkEvent.put("sourceType", sourceType);
        }
        return splunkEvent;
    }

    @SuppressWarnings("unchecked")
    private Configuration configuration() {
        if (configuration != null) {
            return configuration;
        }
        if (expressionTemplateName != null) {
            loadFreemarkerTemplate(expressionTemplateName, expression);
        }
        configuration = freeMarkerConfiguration();
        return configuration;
    }
}
