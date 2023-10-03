package com.apple.aml.stargate.beam.sdk.io.splunk;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.options.SplunkOptions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.splunk.SplunkWriteError;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.UUID;

import static com.apple.aml.stargate.beam.sdk.io.http.HttpInvoker.getHttpResponseInternalSchema;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.REDACTED_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.ClassUtils.duplicate;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;


public class SplunkWriteErrorTransformer extends DoFn<SplunkWriteError, KV<String, GenericRecord>> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final SplunkOptions options;
    private final ObjectToGenericRecordConverter converter;
    private final String schemaId;
    private final Schema schema;
    private final String nodeName;
    private final String nodeType;

    public SplunkWriteErrorTransformer(final String nodeName, final String nodeType, final PipelineConstants.ENVIRONMENT env, final SplunkOptions options) throws Exception {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.options = duplicate(options, SplunkOptions.class);
        this.options.setToken(REDACTED_STRING);
        if (this.options.getSchemaId() == null) {
            this.schema = getHttpResponseInternalSchema(env);
            this.schemaId = this.schema.getFullName();
        } else {
            this.schemaId = this.options.getSchemaId();
            this.schema = fetchSchemaWithLocalFallback(this.options.getSchemaReference(), schemaId);
        }
        this.converter = converter(this.schema);
        LOGGER.debug("Converter created successfully for", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName));
    }

    @SuppressWarnings("unchecked")
    @ProcessElement
    public void processElement(@Element final SplunkWriteError doc, final ProcessContext ctx) throws Exception {
        counter(nodeName, nodeType, UNKNOWN, ELEMENTS_IN).inc();
        long startTime = System.nanoTime();
        try {
            Map map = Map.of("status", false, "code", doc.statusCode(), "message", String.valueOf(doc.statusMessage()), "payload", String.valueOf(doc.payload()));
            GenericRecord response = converter.convert(map);
            ctx.output(KV.of(UUID.randomUUID().toString(), response));
            String schemaId = response.getSchema().getFullName();
            histogramDuration(nodeName, nodeType, schemaId, "process").observe((System.nanoTime() - startTime) / 1000000.0);
            incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, SOURCE_SCHEMA_ID, schemaId);
        } catch (Exception e) {
            histogramDuration(nodeName, nodeType, UNKNOWN, "error").observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.warn("Could not convert/emit splunk error doc to kv of String/GenericRecord", doc, Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            counter(nodeName, nodeType, UNKNOWN, ELEMENTS_ERROR).inc();
        }
    }
}
