package com.apple.aml.stargate.beam.sdk.coders;

import com.apple.aml.stargate.common.pojo.AvroRecord;
import com.apple.aml.stargate.common.utils.SchemaUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_DELIMITER;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_LATEST_VERSION;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.getLocalSchema;
import static java.lang.Integer.parseInt;

public final class AvroCoder extends AtomicCoder<GenericRecord> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final ConcurrentHashMap<String, org.apache.beam.sdk.extensions.avro.coders.AvroCoder<GenericRecord>> CODERS = new ConcurrentHashMap<>();
    private final String schemaReference;

    private AvroCoder(final String schemaReference) {
        this.schemaReference = schemaReference;
    }

    public static AvroCoder of(final String schemaReference) {
        return new AvroCoder(schemaReference);
    }

    public static CoderProvider coderProvider(final String schemaReference) {
        return new CoderProvider() {

            @Override
            public <T> Coder<T> coderFor(TypeDescriptor<T> typeDescriptor, List<? extends Coder<?>> componentCoders) throws CannotProvideCoderException {
                return null;
            }
        };
    }

    @Override
    public void encode(final GenericRecord value, final OutputStream outStream) throws IOException {
        String schemaId = value.getSchema().getFullName();
        GenericRecord record = value;
        int version = SCHEMA_LATEST_VERSION;
        if (value instanceof AvroRecord) {
            AvroRecord avroRecord = (AvroRecord) value;
            version = avroRecord.getSchemaVersion();
            record = avroRecord.record();
        }
        String schemaKey = String.format("%s%s%d", schemaId, SCHEMA_DELIMITER, version);
        StringUtf8Coder.of().encode(schemaKey, outStream);
        org.apache.beam.sdk.extensions.avro.coders.AvroCoder<GenericRecord> coder = CODERS.computeIfAbsent(schemaKey, s -> org.apache.beam.sdk.extensions.avro.coders.AvroCoder.of(value.getSchema()));
        try {
            coder.encode(record, outStream);
        } catch (Exception e) {
            LOGGER.warn("Could not encode avro generic record", Map.of(ERROR_MESSAGE, e.getMessage(), SCHEMA_ID, schemaId, "version", version));
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Could not encode avro generic record", Map.of(ERROR_MESSAGE, e.getMessage(), SCHEMA_ID, schemaId, "version", version), e);
            }
            throw e;
        }
    }

    @Override
    public GenericRecord decode(final InputStream inStream) throws IOException {
        String schemaKey = StringUtf8Coder.of().decode(inStream);
        String schemaTokens[] = schemaKey.split(SCHEMA_DELIMITER);
        String schemaId = schemaTokens[0];
        boolean includesVersion = schemaTokens.length >= 2;
        int version = includesVersion ? parseInt(schemaTokens[1]) : SCHEMA_LATEST_VERSION;
        org.apache.beam.sdk.extensions.avro.coders.AvroCoder<GenericRecord> coder = CODERS.computeIfAbsent(schemaKey, s -> {
            Schema schema = SchemaUtils.fetchSchema(schemaReference, schemaId, version, () -> getLocalSchema(schemaKey));
            return org.apache.beam.sdk.extensions.avro.coders.AvroCoder.of(schema);
        });
        try {
            GenericRecord record = coder.decode(inStream);
            return record instanceof AvroRecord ? record : includesVersion ? new AvroRecord(record, version) : record;
        } catch (Exception e) {
            LOGGER.warn("Could not decode to avro generic record", Map.of(ERROR_MESSAGE, e.getMessage(), SCHEMA_ID, schemaId, "version", version));
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Could not decode to avro generic record", Map.of(ERROR_MESSAGE, e.getMessage(), SCHEMA_ID, schemaId, "version", version), e);
            }
            throw e;
        }
    }
}
