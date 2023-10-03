package com.apple.aml.stargate.common.utils;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import com.linkedin.avro.fastserde.FastGenericDatumReader;
import lombok.SneakyThrows;
import org.apache.avro.Conversion;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.EnvironmentVariables.APP_AVRO_SERDE_USE_FAST_READER;
import static com.apple.aml.stargate.common.constants.CommonConstants.EnvironmentVariables.APP_KAFKA_AVRO_SERDE_USE_FAST_READER;
import static com.apple.aml.stargate.common.utils.AppConfig.env;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.JsonUtils.setObjectMapperDefaults;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public final class AvroUtils {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final AvroMapper AVRO_MAPPER = _avroMapper();
    private static final GenericData GENERIC_DATA = new GenericData();
    private static final boolean USE_FAST_AVRO_READER = ClassUtils.parseBoolean(env(APP_KAFKA_AVRO_SERDE_USE_FAST_READER, env(APP_AVRO_SERDE_USE_FAST_READER, "true")));

    static {
        try {
            Class.forName("org.apache.avro.Conversions");
            Class.forName("org.apache.avro.Conversion");
            Conversion decimalConversion = new org.apache.avro.Conversions.DecimalConversion();
            Conversion uuidConversion = new org.apache.avro.Conversions.UUIDConversion();
            GENERIC_DATA.addLogicalTypeConversion(decimalConversion);
            GENERIC_DATA.addLogicalTypeConversion(uuidConversion);
        } catch (ClassNotFoundException e) {
            LOGGER.info("Running avro version lower than 1.8 disabling UUID and Decimal TypeConversion");
        }
    }

    private AvroUtils() {

    }

    public static Schema avroSchema(final Class className) throws JsonMappingException {
        AvroMapper mapper = _avroMapper();
        AvroSchemaGenerator generator = new AvroSchemaGenerator();
        mapper.acceptJsonFormatVisitor(className, generator);
        AvroSchema schemaWrapper = generator.getGeneratedSchema();
        return schemaWrapper.getAvroSchema();
    }

    private static AvroMapper _avroMapper() {
        AvroMapper mapper = setObjectMapperDefaults(new AvroMapper());
        return mapper;
    }

    public static AvroMapper avroMapper() {
        return AVRO_MAPPER;
    }

    public static Object getFieldValue(final GenericRecord record, final String fieldName) {
        try {
            Object value = record.get(fieldName);
            if (value == null) {
                return null;
            }
            if (value instanceof Utf8) {
                return value.toString();
            }
            return value;
        } catch (Exception | Error e) {
            return null;
        }
    }

    public static Object getFieldValue(final GenericRecord record, final int fieldIndex) {
        try {
            Object value = record.get(fieldIndex);
            if (value == null) {
                return null;
            }
            if (value instanceof Utf8) {
                return value.toString();
            }
            return value;
        } catch (Exception | Error e) {
            return null;
        }
    }

    public static Object getFieldValue(final Map record, final String fieldName) {
        try {
            Object value = record.get(fieldName);
            if (value == null) {
                return null;
            }
            if (value instanceof Utf8) {
                return value.toString();
            }
            return value;
        } catch (Exception | Error e) {
            return null;
        }
    }

    @SneakyThrows
    public static Object extractField(final GenericRecord record, final String fieldName) {
        Object object = record;
        for (String token : fieldName.split("\\.")) {
            if (object instanceof GenericRecord) {
                try {
                    object = ((GenericRecord) object).get(token);
                } catch (Exception | Error e) {
                    return null;
                }
            } else if (object instanceof Map) {
                object = ((Map) object).get(token);
            } else if (object instanceof String && object.toString().startsWith("{")) {
                object = (readJsonMap((String) object)).get(token); // TODO : should we provide this option at all ? Is it safe ?
            } else {
                throw new UnsupportedOperationException(String.format("Cannot extract %s of field %s", token, fieldName));
            }
            if (object == null) return null;
        }
        return object;
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static Object extractField(final Map<String, Object> record, final String fieldName) {
        Object object = record;
        for (String token : fieldName.split("\\.")) {
            if (object instanceof Map) {
                object = ((Map) object).get(token);
            } else if (object instanceof String && object.toString().startsWith("{")) {
                object = (readJsonMap((String) object)).get(token);
            } else {
                throw new UnsupportedOperationException(String.format("Cannot extract %s of field %s", token, fieldName));
            }
            if (object == null) return null;
        }
        return object;
    }

    public static GenericRecord deserialize(final ByteBuffer bytes, final Schema schema) throws IOException {
        return deserializeGenericRecord(bytes, schema, schema, false);
    }

    @SuppressWarnings("unchecked")
    public static GenericRecord deserializeGenericRecord(final ByteBuffer bytes, final Schema writeSchema, final Schema readSchema, final boolean validate) throws IOException {
        ByteBufferBackedInputStream input = new ByteBufferBackedInputStream(bytes);
        final DatumReader<GenericRecord> datumReader = USE_FAST_AVRO_READER ? new FastGenericDatumReader<>(writeSchema, readSchema) : GENERIC_DATA.createDatumReader(writeSchema, readSchema);
        return datumReader.read(new GenericData.Record(readSchema), DecoderFactory.get().binaryDecoder(input, null));
    }
}
