package com.apple.aml.stargate.beam.sdk.ts;

import com.apple.aml.stargate.common.options.ErrorOptions;
import com.apple.aml.stargate.common.options.JavaFunctionOptions;
import com.apple.aml.stargate.common.pojo.ErrorRecord;
import com.google.inject.Injector;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.apple.aml.stargate.beam.sdk.utils.BeamUtils.getInjector;
import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.RECORD;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.STAGE;
import static com.apple.aml.stargate.common.utils.ClassUtils.parseBoolean;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.rootNodeName;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class ErrorInterceptor extends DoFn<KV<String, ErrorRecord>, KV<String, ErrorRecord>> implements Serializable {
    public static final String INTERCEPTOR_NODE_NAME = "stargate_error_interceptor";
    private static final Logger LOGGER = LoggerFactory.getLogger("stargate.errors");
    private static final ConcurrentHashMap<String, Function<Map<String, Object>, Map<String, Object>>> INTERCEPTORS = new ConcurrentHashMap<>();
    private final Map<String, ErrorOptions> errorDefinition;

    public ErrorInterceptor(final Map<String, ErrorOptions> errorDefinition) {
        this.errorDefinition = errorDefinition;
        if (this.errorDefinition != null) {
            errorDefinition.values().forEach(d -> {
                if (d.getErrorOptions() == null) d.setErrorOptions(new JavaFunctionOptions());
                if (isBlank(d.getErrorOptions().getClassName())) d.getErrorOptions().setClassName(d.getErrorInterceptor());
            });
        }
    }

    @SuppressWarnings("unchecked")
    @ProcessElement
    public void processElement(@Element final KV<String, ErrorRecord> kv, final ProcessContext ctx) throws Exception {
        ErrorRecord record = kv.getValue();
        GenericRecord genericRecord = record.getRecord();
        Schema schema = genericRecord.getSchema();
        String schemaId = schema.getFullName();
        counter(record.getNodeName(), record.getNodeType(), schemaId, "stargate_errors_in").inc();
        String rootNodeName = rootNodeName(record.getNodeName());
        ErrorOptions options = errorDefinition.get(rootNodeName);
        if (options == null) throw exception(record);
        if (!isBlank(options.getErrorOptions().getClassName())) {
            Function<Map<String, Object>, Map<String, Object>> function = INTERCEPTORS.computeIfAbsent(rootNodeName, nodeName -> {
                try {
                    Injector injector = getInjector(nodeName, options.getErrorOptions());
                    return (Function<Map<String, Object>, Map<String, Object>>) injector.getInstance(Class.forName(options.getErrorOptions().getClassName()));
                } catch (Exception e) {
                    LOGGER.error("Error in creating interceptor class!!", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                    throw new RuntimeException(e);
                }
            });
            Map<String, Object> output;
            try {
                output = function.apply(record.asMap());
            } catch (Exception e) {
                if (options.logOnError()) logError(kv, record, schemaId);
                throw exception(record);
            }
            if (output == null || parseBoolean(output.getOrDefault("skip", "false"))) {
                if (options.logOnError()) logError(kv, record, schemaId);
                return;
            }
            record.applyOverride(output);
            if (record.getRecord() != genericRecord) { // purposefully not using .equals
                genericRecord = record.getRecord();
                schema = genericRecord.getSchema();
                schemaId = schema.getFullName();
            }
        } else if (options.failOnError()) {
            if (options.logOnError()) logError(kv, record, schemaId);
            throw exception(record);
        }
        if (options.logOnError()) logError(kv, record, schemaId);
        counter(record.getNodeName(), record.getNodeType(), schemaId, "stargate_errors_out").inc();
        ctx.output(kv);
    }

    private Exception exception(final ErrorRecord record) {
        LOGGER.error(EMPTY_STRING, Map.of(NODE_NAME, record.getNodeName(), NODE_TYPE, record.getNodeType(), ERROR_MESSAGE, record.getErrorMessage()), record.getException());
        return record.getException();
    }

    private void logError(final KV<String, ErrorRecord> kv, final ErrorRecord record, final String schemaId) {
        LOGGER.warn(record.getErrorMessage(), Map.of(NODE_NAME, record.getNodeName(), NODE_TYPE, record.getNodeType(), SCHEMA_ID, schemaId, "key", kv.getKey(), STAGE, record.getStage(), ERROR_MESSAGE, String.valueOf(record.getException().getMessage()), RECORD, record.getRecord().toString()));
    }
}
