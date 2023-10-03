package com.apple.aml.stargate.common.converters;

import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.jsoniter.output.JsonStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;
import java.util.Optional;

public class XmlToGenericRecordConverter extends LazyJsonToUnstructuredGenericRecordConverter {
    public XmlToGenericRecordConverter(Schema schema) {
        super(schema);
    }

    public static XmlToGenericRecordConverter converter(final Schema schema) {
        return new XmlToGenericRecordConverter(schema);
    }

    @Override
    public GenericRecord convert(final Object object, final String unstructuredFieldName) throws InvalidInputException, IOException {
        if (object == null) {
            throw new InvalidInputException("null object found. Did you send valid xml ?");
        }
        XmlMapper m = new XmlMapper();
        JsonNode node = m.readTree(new StringReader((String) object));

        Pair<GenericRecord, Map<String, Object>> pair = unstructured(node);
        if (pair.getRight() == null) {
            return pair.getLeft();
        }
        Optional<Schema.Field> field = schema.getFields().stream().filter(f -> f.name().equalsIgnoreCase(unstructuredFieldName)).findFirst();
        if (!field.isPresent()) {
            return pair.getLeft();
        }
        GenericRecord record = pair.getLeft();
        record.put(field.get().pos(), JsonStream.serialize(pair.getRight()));
        return record;
    }


}
