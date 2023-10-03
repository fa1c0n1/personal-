package com.apple.aml.stargate.common.converters;

import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;

import static com.apple.aml.stargate.common.utils.AvroUtils.avroMapper;
import static com.apple.aml.stargate.common.utils.AvroUtils.deserialize;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonNode;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static java.nio.charset.StandardCharsets.UTF_8;

public class JacksonGenericRecordConverter implements ObjectToGenericRecordConverter, Serializable {
    private static final long serialVersionUID = 1L;
    private final Schema schema;
    private final ObjectWriter writer;

    public JacksonGenericRecordConverter(final Schema schema) {
        this.schema = schema;
        this.writer = avroMapper().writer(new AvroSchema(this.schema));
    }

    public static JacksonGenericRecordConverter converter(final Schema schema) {
        return new JacksonGenericRecordConverter(schema);
    }

    @Override
    public GenericRecord convert(final Object object) throws Exception {
        if (object == null) throw new InvalidInputException("null object found. Did you send valid json string ?");
        byte[] bytes;
        if (object instanceof String) {
            bytes = writer.writeValueAsBytes(jsonNode((String) object));
        } else if (object instanceof JsonNode) {
            bytes = writer.writeValueAsBytes(object);
        } else if (object instanceof ByteBuffer) {
            bytes = writer.writeValueAsBytes(jsonNode(new String(((ByteBuffer) object).array(), UTF_8)));
        } else {
            bytes = writer.writeValueAsBytes(jsonNode(jsonString(object)));
        }
        return deserialize(ByteBuffer.wrap(bytes), this.schema);
    }

    @Override
    public GenericRecord convert(final Object object, final String unstructuredFieldName) throws Exception {
        return convert(object);
    }

    @Override
    public Iterator<GenericRecord> iterator(final Object object) throws Exception {
        throw new UnsupportedOperationException("iterator support is not available in this class !");
    }
}
