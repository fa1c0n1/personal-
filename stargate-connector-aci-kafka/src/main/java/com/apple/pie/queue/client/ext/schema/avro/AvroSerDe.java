package com.apple.pie.queue.client.ext.schema.avro;

import com.apple.aml.stargate.common.utils.AvroUtils;
import org.apache.avro.Conversion;
import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author andy coates
 * created 26/10/2016.
 */
@SuppressWarnings("WeakerAccess")
public final class AvroSerDe {

    static final int SERIALIZER_INITIAL_CAPACITY = 256;
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroSerDe.class);
    private static final SpecificData SPECIFIC_DATA = new SpecificData();
    private static final GenericData GENERIC_DATA = new GenericData();
    private static final List<SchemaValidator> SCHEMA_VALIDATORS_LIST = new ArrayList<>();

    static {
        try {
            Class.forName("org.apache.avro.Conversions");
            Class.forName("org.apache.avro.Conversion");

            Conversion decimalConversion = new org.apache.avro.Conversions.DecimalConversion();
            Conversion uuidConversion = new org.apache.avro.Conversions.UUIDConversion();

            // Add default conversions to generic data
            getGenericData().addLogicalTypeConversion(decimalConversion);
            getGenericData().addLogicalTypeConversion(uuidConversion);

            // Add default conversions to specific data
            getSpecificData().addLogicalTypeConversion(decimalConversion);
            getSpecificData().addLogicalTypeConversion(uuidConversion);

            SCHEMA_VALIDATORS_LIST.add(new DecimalCompatibilityChecker(DecimalCompatibilityChecker.PrecisionValidation.Ignore));
        } catch (ClassNotFoundException e) {
            LOGGER.info("Running avro version lower than 1.8 disabling UUID and Decimal TypeConversion");
        }
    }

    private AvroSerDe() {
    }

    /**
     * Provide access to the GenericData this is only to be used to do some static initialization.
     */
    public static GenericData getGenericData() {
        return GENERIC_DATA;
    }

    /**
     * Provide access to the SpecificData this is only to be used to do some static initialization.
     */
    public static SpecificData getSpecificData() {
        return SPECIFIC_DATA;
    }

    /**
     * Serialize the supplied {@code record}.
     *
     * @param record the record to serialise
     * @return the serialised form
     * @throws IOException on any error.
     */
    @SuppressWarnings("unchecked")
    public static ByteBuffer serialize(IndexedRecord record) throws IOException {
        ByteBufferOutputStream output = new ByteBufferOutputStream(ByteBuffer.allocate(SERIALIZER_INITIAL_CAPACITY));
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);

        // We only use the SpecificData serializer here as it works with both generic and non-generic data.
        DatumWriter<IndexedRecord> datumWriter = SPECIFIC_DATA.createDatumWriter(record.getSchema());
        datumWriter.write(record, encoder);
        encoder.flush();

        ByteBuffer buffer = output.buffer();
        buffer.flip();
        return buffer;
    }

    /**
     * Deserialise the supplied {@code bytes} into a instance of the {@code expectedClass}.
     *
     * @param bytes         the bytes to deserialise
     * @param writeSchema   the schema used to serialize the data. This must be the exact version of the schema used to serialize the data.
     * @param expectedClass the expected type of the de-serialized entity, which must be a subclass of {@link SpecificRecord}
     * @param <V>           the expected type
     * @return the deserialized entity
     * @throws IllegalAccessException if {@code expectedClass} can not accessible
     * @throws InstantiationException if {@code expectedClass} can not be instantiated
     * @throws IOException            on buffer related errors
     */
    @SuppressWarnings("unchecked")
    public static <V extends SpecificRecord> V deserializeSpecificRecord(ByteBuffer bytes, Schema writeSchema, Class<V> expectedClass) throws IllegalAccessException, InstantiationException, IOException {
        return deserializeSpecificRecord(bytes, writeSchema, expectedClass, true);
    }

    /**
     * Deserialise the supplied {@code bytes} into a instance of the {@code expectedClass}.
     *
     * @param bytes         the bytes to deserialise
     * @param writeSchema   the schema used to serialize the data. This must be the exact version of the schema used to serialize the data.
     * @param expectedClass the expected type of the de-serialized entity, which must be a subclass of {@link SpecificRecord}
     * @param validate      if {@code true} the call will perform additional checks to ensure the read and write schemas have compatible decimal types.
     *                      This validation is expensive and should only be done once per read/write schema version pair.
     * @param <V>           the expected type
     * @return the deserialized entity
     * @throws IllegalAccessException if {@code expectedClass} can not accessible
     * @throws InstantiationException if {@code expectedClass} can not be instantiated
     * @throws IOException            on buffer related errors
     */
    @SuppressWarnings({"unchecked", "deprecation"})
    public static <V extends SpecificRecord> V deserializeSpecificRecord(ByteBuffer bytes, Schema writeSchema, Class<V> expectedClass, boolean validate) throws IllegalAccessException, InstantiationException, IOException {
        ByteBufferInputStream input = new ByteBufferInputStream(bytes);
        V theInstance = expectedClass.newInstance();
        final Schema readSchema = theInstance.getSchema();
        if (validate) {
            validateSchemas(writeSchema, readSchema);
        }
        SpecificDatumReader<V> datumReader = new SpecificDatumReader<>(writeSchema, readSchema);
        return datumReader.read(theInstance, DecoderFactory.get().binaryDecoder(input, null));
    }

    private static void validateSchemas(Schema writeSchema, Schema readSchema) {
        for (SchemaValidator schemaValidator : SCHEMA_VALIDATORS_LIST) {
            try {
                schemaValidator.validate(writeSchema, Collections.singletonList(readSchema));
            } catch (SchemaValidationException e) {
                throw new AvroDeserializationException("Schema validation failed", e);
            }
        }
    }

    /**
     * Deserialise the supplied {@code bytes} into a instance of {@link GenericRecord}.
     *
     * @param bytes       the bytes to deserialise
     * @param writeSchema the schema used to serialize the data. This must be the exact version of the schema used to serialize the data.
     * @param readSchema  the latest available version of the schema.
     * @return the deserialized entity
     * @throws IOException on buffer related errors
     */
    @SuppressWarnings("unchecked")
    public static GenericRecord deserializeGenericRecord(ByteBuffer bytes, Schema writeSchema, Schema readSchema) throws IOException {
        return deserializeGenericRecord(bytes, writeSchema, readSchema, true);
    }

    /**
     * Deserialise the supplied {@code bytes} into a instance of {@link GenericRecord}.
     *
     * @param bytes       the bytes to deserialise
     * @param writeSchema the schema used to serialize the data. This must be the exact version of the schema used to serialize the data.
     * @param readSchema  the latest available version of the schema.
     * @param validate    if {@code true} the call will perform additional checks to ensure the read and write schemas have compatible decimal types.
     *                    This validation is expensive and should onley be done once per read/write schema version pair.
     * @return the deserialized entity
     * @throws IOException on buffer related errors
     */
    @SuppressWarnings("unchecked")
    public static GenericRecord deserializeGenericRecord(ByteBuffer bytes, Schema writeSchema, Schema readSchema, boolean validate) throws IOException {
        return AvroUtils.deserializeGenericRecord(bytes, writeSchema, readSchema, validate);
    }
}