package com.apple.aml.stargate.beam.sdk.io.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.header.Headers;

import java.io.Serializable;

import static com.apple.aml.stargate.beam.sdk.io.kafka.RawDeserializer.headerKVs;


@EqualsAndHashCode(callSuper = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaMessage extends RawMessage implements Serializable {
    private int partition;
    private long offset;

    public static KafkaMessage kafkaMessage(final String topic, final Headers headers, final byte[] bytes, final int partition, final long offset) {
        KafkaMessage message = new KafkaMessage();
        message.topic = topic;
        message.partition = partition;
        message.offset = offset;
        message.bytes = bytes;
        message.headers = headerKVs(headers);
        return message;
    }
}
