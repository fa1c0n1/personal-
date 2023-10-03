package com.apple.aml.stargate.beam.sdk.transforms;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Map;

import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.SchemaUtils.getLocalSchema;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eJsonRecord;

/**
 * This DoFn will Convert ByteArrayData into GenericRecord ByteBuffer object
 */
public final class ByteArrayToGenericRecord extends DoFn<KV<String, byte[]>, KV<String, GenericRecord>> {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final long serialVersionUID = 1L;
    private final String nodeName;
    private final String nodeType;
    private final Schema schema;

    public ByteArrayToGenericRecord(final String nodeName, final String nodeType, final PipelineConstants.ENVIRONMENT environment) throws IOException {
        this(nodeName, nodeType, getLocalSchema(environment, "bytearraydata.avsc"));
    }

    public ByteArrayToGenericRecord(final String nodeName, final String nodeType, final Schema schema) {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.schema = schema;
        LOGGER.debug("Object to AVRO transformer created successfully for", Map.of("schema", schema));
    }

    @ProcessElement
    public void processElement(@Element final KV<String, byte[]> kv, final ProcessContext ctx) throws Exception {
        try {
            KV<String, GenericRecord> genericRecordKV = KV.of(kv.getKey(), convert(kv.getValue(), schema));
            ctx.output(genericRecordKV);
        } catch (Exception e) {
            LOGGER.warn("Error converting payload to genericRecord. Skip enabled!!. Will skip this record", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "key", String.valueOf(kv.getKey())), e);
            ctx.output(ERROR_TAG, eJsonRecord(nodeName, nodeType, "avro_conversion", kv, e));
        }
    }

    public static GenericRecord convert(final byte[] data, final Schema schema) {
        GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);
        genericRecordBuilder.set(schema.getField("data"), ByteBuffer.wrap(data));
        return genericRecordBuilder.build();
    }
}
