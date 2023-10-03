package com.apple.aml.stargate.connector.aci.kafka;

import com.apple.pie.queue.client.ext.schema.avro.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serializer;

public class ACIAvroSerializer extends ACISerializer<GenericRecord> implements Serializer<GenericRecord> {
    @Override
    public Class getSerializerClass() {
        return KafkaAvroSerializer.class;
    }
}
