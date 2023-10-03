package com.apple.aml.stargate.connector.aci.kafka;

import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.Serializable;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.SerializationConstants.CONFIG_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.SerializationConstants.CONFIG_SCHEMA_REFERENCE;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;

public class AvroStringDeserializer extends AvroDeserializer implements Deserializer<GenericRecord> {
    @Override
    public Pair<Deserializer<GenericRecord>, Deserializer<GenericRecord>> deserializerInstances(final Map<String, ?> configs, final boolean isKey) {
        return Pair.of(new StringToAvroConverter((String) configs.get(CONFIG_SCHEMA_ID), (String) configs.get(CONFIG_SCHEMA_REFERENCE)), null);
    }

    @Override
    public Pair<String, Integer> schemaDetails(final Headers headers) {
        return Pair.of(UNKNOWN, -1);
    }

    private static final class StringToAvroConverter implements Deserializer<GenericRecord>, Serializable {
        private final Schema schema;
        private final ObjectToGenericRecordConverter converter;
        private String encoding = "UTF8";

        public StringToAvroConverter(final String schemaId, final String schemaReference) {
            this.schema = fetchSchemaWithLocalFallback(schemaReference, schemaId);
            this.converter = converter(this.schema);
        }

        public void configure(Map<String, ?> configs, boolean isKey) {
            String propertyName = isKey ? "key.deserializer.encoding" : "value.deserializer.encoding";
            Object encodingValue = configs.get(propertyName);
            if (encodingValue == null) encodingValue = configs.get("deserializer.encoding");
            if (encodingValue instanceof String) encoding = (String) encodingValue;
        }

        @Override
        public GenericRecord deserialize(final String topic, final byte[] data) {
            try {
                return this.converter.convert(new String(data, encoding));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
