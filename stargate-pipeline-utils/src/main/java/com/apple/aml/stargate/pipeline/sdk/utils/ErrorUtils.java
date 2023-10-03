package com.apple.aml.stargate.pipeline.sdk.utils;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.pojo.ErrorRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.Charset;

import static com.apple.aml.stargate.common.utils.AppConfig.environment;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;

public final class ErrorUtils {
    private static final Schema STRING_PAYLOAD_SCHEMA;

    public static final String MISSING_INIT_CONFIG_ERROR = "MISSING_INIT_CONFIG_ERROR";

    static {
        try {
            STRING_PAYLOAD_SCHEMA = stringPayloadSchema(environment());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private ErrorUtils() {
    }

    public static Schema stringPayloadSchema(final PipelineConstants.ENVIRONMENT environment) throws IOException {
        Schema.Parser parser = new Schema.Parser();
        String schemaString = IOUtils.resourceToString("/stringpayload.avsc", Charset.defaultCharset()).replaceAll("\\#\\{ENV\\}", environment.name().toLowerCase());
        return parser.parse(schemaString);
    }

    @SuppressWarnings("unchecked")
    public static <O> KV<String, ErrorRecord> eDynamicRecord(final String nodeName, final String nodeType, final String stage, final KV<String, O> kv, final Exception exception) {
        if (kv.getValue() instanceof GenericRecord) return eRecord(nodeName, nodeType, stage, (KV<String, GenericRecord>) kv, exception);
        return eJsonRecord(nodeName, nodeType, stage, (KV<String, GenericRecord>) kv, exception);
    }

    public static KV<String, ErrorRecord> eRecord(final String nodeName, final String nodeType, final String stage, final KV<String, GenericRecord> kv, final Exception exception) {
        ErrorRecord errorRecord = new ErrorRecord();
        errorRecord.setNodeName(nodeName);
        errorRecord.setNodeType(nodeType);
        errorRecord.setStage(stage);
        errorRecord.setKey(kv.getKey());
        errorRecord.setRecord(kv.getValue());
        errorRecord.setException(exception);
        errorRecord.setErrorMessage(String.valueOf(exception.getLocalizedMessage()));
        return KV.of(errorRecord.getKey(), errorRecord);
    }

    public static <O> KV<String, ErrorRecord> eJsonRecord(final String nodeName, final String nodeType, final String stage, final KV<String, O> kv, final Exception exception) {
        ErrorRecord errorRecord = new ErrorRecord();
        errorRecord.setNodeName(nodeName);
        errorRecord.setNodeType(nodeType);
        errorRecord.setStage(stage);
        errorRecord.setKey(kv.getKey());
        GenericRecord record = new GenericData.Record(STRING_PAYLOAD_SCHEMA);
        String payload;
        try {
            payload = kv.getValue() == null ? null : jsonString(kv.getValue());
        } catch (Exception e) {
            try {
                payload = String.valueOf(kv.getValue());
            } catch (Exception ignored) {
                payload = null;
            }
        }
        record.put("payload", payload);
        errorRecord.setRecord(record);
        errorRecord.setException(exception);
        errorRecord.setErrorMessage(String.valueOf(exception.getLocalizedMessage()));
        return KV.of(errorRecord.getKey(), errorRecord);
    }
}
