package org.apache.beam.sdk.io.kafka;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.apache.kafka.common.header.Headers;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;

/**
 * KafkaRecord contains key and value of the record as well as metadata for the record (topic name,
 * partition id, and offset).
 */
@SuppressWarnings({"nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class KafkaRecord<K, V> {
    // This is based on {@link ConsumerRecord} received from Kafka Consumer.
    // The primary difference is that this contains deserialized key and value, and runtime
    // Kafka version agnostic (e.g. Kafka version 0.9.x does not have timestamp fields).

    private final String topic;
    private final int partition;
    private final long offset;
    private final @Nullable Headers headers;
    private final KV<K, V> kv;
    private final long timestamp;
    private final KafkaTimestampType timestampType;

    public KafkaRecord(final String topic, final int partition, final long offset, final long timestamp, final KafkaTimestampType timestampType, final @Nullable Headers headers, final K key, final V value) {
        this(topic, partition, offset, timestamp, timestampType, headers, KV.of(key, value));
    }

    public KafkaRecord(final String topic, final int partition, final long offset, final long timestamp, final KafkaTimestampType timestampType, final @Nullable Headers headers, final KV<K, V> kv) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.timestampType = timestampType;
        this.headers = new HeadersWrapper(headers);
        this.kv = kv;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public @Nullable Headers getHeaders() {
        if (!ConsumerSpEL.hasHeaders()) {
            throw new RuntimeException("The version kafka-clients does not support record headers, please use version 0.11.0.0 or newer");
        }
        return headers;
    }

    public KV<K, V> getKV() {
        return kv;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public KafkaTimestampType getTimestampType() {
        return timestampType;
    }

    @Override
    public int hashCode() {
        return Arrays.deepHashCode(new Object[]{topic, partition, offset, timestamp, headers, kv});
    }

    @Override
    public boolean equals(final @Nullable Object obj) {
        if (obj instanceof KafkaRecord) {
            @SuppressWarnings("unchecked") KafkaRecord<Object, Object> other = (KafkaRecord<Object, Object>) obj;
            return topic.equals(other.topic) && partition == other.partition && offset == other.offset && timestamp == other.timestamp && Objects.equal(headers, other.headers) && kv.equals(other.kv);
        } else {
            return false;
        }
    }
}
