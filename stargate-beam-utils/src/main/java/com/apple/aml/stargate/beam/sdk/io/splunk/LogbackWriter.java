package com.apple.aml.stargate.beam.sdk.io.splunk;

import com.apple.aml.stargate.common.options.SplunkOptions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.apple.aml.stargate.beam.sdk.io.splunk.SplunkWriter.logEventInfo;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.INDEX_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.nodes.StargateNode.nodeName;
import static com.apple.aml.stargate.common.utils.AppConfig.env;
import static com.apple.aml.stargate.common.utils.JsonUtils.fastJsonString;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eDynamicRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class LogbackWriter<O> extends DoFn<KV<String, O>, KV<String, O>> implements Serializable {
    public static final String LOGBACK_WRITER_LOGGER_PREFIX = "stargate.splunk.forwarder";
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final String nodeName;
    private final String nodeType;
    private final SplunkEventTransformer transformer;
    private final boolean emit;
    private final String newLineReplacement;
    private final boolean emitHECEvent;
    private Logger splunkLogger;

    public LogbackWriter(final String nodeName, final String nodeType, final SplunkOptions options, final boolean emit) throws Exception {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.emit = emit;
        this.emitHECEvent = options.isEmitHECEvent();
        newLineReplacement = isBlank(options.getNewLineReplacement()) ? env("APP_LOG_NEWLINE_REPLACEMENT", "\r") : options.getNewLineReplacement();
        transformer = new SplunkEventTransformer(nodeName(nodeName, "transform"), nodeType, options);
    }

    @Setup
    public void setup() throws Exception {
        if (splunkLogger == null) splunkLogger = LoggerFactory.getLogger(String.format("%s.%s", LOGBACK_WRITER_LOGGER_PREFIX, nodeName));
    }

    @SuppressWarnings("unchecked")
    @ProcessElement
    public void processElement(@Element final KV<String, O> kv, final ProcessContext ctx) {
        Object record = kv.getValue();
        boolean isAvro = record instanceof GenericRecord;
        String schemaId = isAvro ? ((GenericRecord) record).getSchema().getFullName() : UNKNOWN;
        long startTime = System.nanoTime();
        Map se = null;
        String splunkIndex = UNKNOWN;
        try {
            counter(nodeName, nodeType, schemaId, ELEMENTS_IN).inc();
            String event;
            try {
                if (isAvro) {
                    se = transformer.fetchSplunkEventMap((KV<String, GenericRecord>) kv);
                    splunkIndex = String.valueOf(se.get("index"));
                    event = emitHECEvent ? fastJsonString(se) : (String) se.get("event");
                } else {
                    event = String.valueOf(record);
                }
            } catch (Exception e) {
                histogramDuration(nodeName, nodeType, schemaId, "process_error").observe((System.nanoTime() - startTime) / 1000000.0);
                counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
                LOGGER.warn("Error in converting to log record", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), NODE_NAME, nodeName, NODE_TYPE, nodeType, "key", kv.getKey(), SCHEMA_ID, schemaId), logEventInfo(se), e);
                ctx.output(ERROR_TAG, eDynamicRecord(nodeName, nodeType, "event_conversion", kv, e));
                return;
            }
            splunkLogger.info(event.replace("\n", newLineReplacement));
            histogramDuration(nodeName, nodeType, schemaId, "process").observe((System.nanoTime() - startTime) / 1000000.0);
            incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, INDEX_NAME, splunkIndex, SOURCE_SCHEMA_ID, schemaId);
            if (emit) ctx.output(kv);
        } catch (Exception e) {
            histogramDuration(nodeName, nodeType, schemaId, "process_error").observe((System.nanoTime() - startTime) / 1000000.0);
            counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
            LOGGER.warn("Error in converting to splunk event", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, "key", kv.getKey(), SCHEMA_ID, schemaId, ERROR_MESSAGE, String.valueOf(e.getMessage())), logEventInfo(se), e);
            ctx.output(ERROR_TAG, eDynamicRecord(nodeName, nodeType, "log_write", kv, e));
        }
    }
}
