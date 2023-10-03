package com.apple.aml.stargate.connector.aci.kafka;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.utils.SchemaUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class AvroByteArrayDeserializer implements Deserializer<GenericRecord> {
    final PipelineConstants.ENVIRONMENT environment;

    public AvroByteArrayDeserializer(PipelineConstants.ENVIRONMENT environment) {
        this.environment = environment;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public GenericRecord deserialize(String topic, byte[] data) {
        GenericRecord genericRecord = null;
        try {
            Schema schema = SchemaUtils.getLocalSchema(environment, "bytearraydata.avsc");
            GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);
            genericRecordBuilder.set(schema.getField("data"), ByteBuffer.wrap(data));
            genericRecord = genericRecordBuilder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return genericRecord;
    }

    @Override
    public GenericRecord deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
