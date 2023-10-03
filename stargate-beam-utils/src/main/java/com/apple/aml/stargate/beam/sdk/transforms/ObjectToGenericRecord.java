package com.apple.aml.stargate.beam.sdk.transforms;

import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.AvroUtils.avroSchema;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eJsonRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;

public final class ObjectToGenericRecord<O> extends DoFn<KV<String, O>, KV<String, GenericRecord>> {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final long serialVersionUID = 1L;
    private final String nodeName;
    private final String nodeType;
    private final Schema schema;
    private final ObjectToGenericRecordConverter converter;

    public ObjectToGenericRecord(final String nodeName, final String nodeType, final Class className) throws JsonMappingException {
        this(nodeName, nodeType, avroSchema(className));
    }

    public ObjectToGenericRecord(final String nodeName, final String nodeType, final Schema schema) {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.schema = schema;
        this.converter = converter(this.schema);
        LOGGER.debug("Object to AVRO transformer created successfully for", Map.of("schema", schema));
    }

    public ObjectToGenericRecord(final String nodeName, final String nodeType, final String schemaReference, final String schemaId) {
        this(nodeName, nodeType, fetchSchemaWithLocalFallback(schemaReference, schemaId));
    }

    @ProcessElement
    public void processElement(@Element final KV<String, O> kv, final ProcessContext ctx) throws Exception {
        try {
            ctx.output(KV.of(kv.getKey(), converter.convert(kv.getValue())));
        } catch (Exception e) {
            LOGGER.warn("Error converting payload to genericRecord. Skip enabled!!. Will skip this record", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "key", String.valueOf(kv.getKey())), e);
            ctx.output(ERROR_TAG, eJsonRecord(nodeName, nodeType, "avro_conversion", kv, e));
        }
    }
}
