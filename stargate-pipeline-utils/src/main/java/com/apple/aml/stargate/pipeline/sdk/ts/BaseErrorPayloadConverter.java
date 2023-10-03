package com.apple.aml.stargate.pipeline.sdk.ts;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_LATEST_VERSION;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.pipelineId;
import static com.apple.aml.stargate.common.constants.PipelineConstants.PIPELINE_ID;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.pojo.AvroRecord;
import com.apple.aml.stargate.common.pojo.ErrorRecord;
import static com.apple.aml.stargate.common.utils.AppConfig.appId;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import com.apple.pie.queue.client.ext.schema.avro.AvroSerDe;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class BaseErrorPayloadConverter {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final String nodeName;
    private final Schema errorSchema;


    public BaseErrorPayloadConverter(final String errorNodeName, final PipelineConstants.ENVIRONMENT environment) {
        try {
            this.nodeName = errorNodeName;
            this.errorSchema = errorSchema(environment);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Schema errorSchema(final PipelineConstants.ENVIRONMENT environment) throws IOException {
        Schema.Parser parser = new Schema.Parser();
        String schemaString = IOUtils.resourceToString("/errorpayload.avsc", Charset.defaultCharset()).replaceAll("\\#\\{ENV\\}", environment.name().toLowerCase());
        return parser.parse(schemaString);
    }




    public KV<String, GenericRecord> convert(KV<String, ErrorRecord> kv) {
        ErrorRecord errorRecord = kv.getValue();
        GenericRecord genericRecord = errorRecord.getRecord();
        Schema schema = genericRecord.getSchema();
        final String schemaId = schema.getFullName();
        final int version = genericRecord instanceof AvroRecord ? ((AvroRecord) genericRecord).getSchemaVersion() : SCHEMA_LATEST_VERSION;
        Exception exception = errorRecord.getException();
        counter(errorRecord.getNodeName(), errorRecord.getNodeType(), schemaId, "error_elements_in").inc();
        GenericRecord record = new GenericData.Record(errorSchema);
        record.put(PIPELINE_ID, pipelineId());
        record.put("appId", appId());
        record.put(NODE_NAME, errorRecord.getNodeName());
        record.put(NODE_TYPE, errorRecord.getNodeType());
        record.put("stage", errorRecord.getStage());
        record.put("key", errorRecord.getKey());
        record.put("schemaId", schemaId);
        record.put("schemaVersion", version);
        record.put("json", genericRecord.toString());
        record.put("errorClass", exception.getClass().getName());
        record.put(ERROR_MESSAGE, errorRecord.getErrorMessage());
        record.put("errorStackTrace", ExceptionUtils.getStackTrace(exception));
        Map<String, String> errorInfo = null;
        if (exception instanceof GenericException) {
            Map<String, ?> info = ((GenericException) exception).getDebugInfo();
            if (info != null) {
                errorInfo = new HashMap<>();
                for (Map.Entry<String, ?> entry : info.entrySet()) {
                    try {
                        errorInfo.put(entry.getKey(), jsonString(entry.getValue()));
                    } catch (Exception e) {
                        errorInfo.put(entry.getKey(), String.valueOf(entry.getValue()));
                    }
                }
            }
        }
        record.put("errorInfo", errorInfo);
        String byteString = null;
        try {
            ByteBuffer byteBuffer = AvroSerDe.serialize(record);
            final byte[] buffer = new byte[byteBuffer.limit()];
            byteBuffer.get(buffer);
            byteString = new String(Base64.getEncoder().encode(buffer), StandardCharsets.UTF_8);
        } catch (Exception e) {
            LOGGER.error("Could not encode the error payload", Map.of(ERROR_MESSAGE, e.getMessage(), SCHEMA_ID, schemaId, "version", version));
        }
        record.put("payload", byteString);
        counter(nodeName, errorRecord.getNodeType(), schemaId, "error_elements_out").inc();
        return KV.of(kv.getKey(), record);

    }


}
