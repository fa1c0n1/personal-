package com.apple.aml.stargate.connector.aci.kafka;

import com.apple.pie.queue.kafka.config.ConfigUtils;
import com.apple.pie.queue.kafka.crypto.EncryptionSerializer;
import com.apple.pie.queue.kafka.crypto.EncryptionSerializerConfigDef;
import com.apple.pie.queue.kafka.crypto.extension.CryptoExtensionSerDeserializer;
import com.apple.pie.queue.kafka.envelope.EnvelopeConfig;
import com.apple.pie.queue.kafka.envelope.PieSerializer;
import io.prometheus.client.Histogram;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.STATUS;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SUCCESS;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.KafkaConstants.TOPIC;
import static com.apple.aml.stargate.common.constants.KafkaConstants.getCompatibleACIHeaders;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.PrometheusUtils.bucketBuilder;

abstract class ACISerializer<O> implements Serializer<O> {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final RecordHeaders PIE_HEADERS = getCompatibleACIHeaders();
    private static final Histogram serializeLatency = bucketBuilder("message_serialize_latency_ms", Histogram.build().help("Time taken to serialize kafka message in milli seconds").labelNames(TOPIC, SCHEMA_ID, STATUS)).register();
    private static final Histogram serializeSizeBytes = bucketBuilder("message_serialize_size_bytes", Histogram.build().help("Kafka message size after serialization in bytes").labelNames(TOPIC, SCHEMA_ID)).register();
    @SuppressWarnings("deprecation")
    private final PieSerializer<O> pieSerializer = new PieSerializer<O>();

    @Override
    @SuppressWarnings("unchecked")
    public void configure(final Map<String, ?> iconfigs, final boolean isKey) {
        Map<String, Object> configs = (Map<String, Object>) iconfigs;
        Object encryptionEnabled = configs.get(EncryptionSerializerConfigDef.ENCRYPT_MESSAGES_CONFIG);
        if (encryptionEnabled != null && Boolean.parseBoolean(encryptionEnabled + "")) {
            configs.put(EnvelopeConfig.PAYLOAD_SERIALIZER_CONFIG, EncryptionSerializer.class);
            configs.put(EncryptionSerializerConfigDef.PAYLOAD_SERIALIZER_CONFIG, getSerializerClass());
            ConfigUtils.appendToClassList(configs, EnvelopeConfig.EXTENSION_SERDE_CONFIG, CryptoExtensionSerDeserializer.class);
        } else {
            configs.put(EnvelopeConfig.PAYLOAD_SERIALIZER_CONFIG, getSerializerClass());
        }
        pieSerializer.configure(configs, isKey);
    }

    abstract public Class getSerializerClass();

    @Override
    public byte[] serialize(final String topic, final O data) {
        byte[] bytes;
        String schemaId = (data instanceof GenericRecord) ? ((GenericRecord) data).getSchema().getFullName() : UNKNOWN;
        long startTimeNanos = System.nanoTime();
        try {
            bytes = pieSerializer.serialize(topic, PIE_HEADERS, data);
            serializeLatency.labels(topic, schemaId, SUCCESS).observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            serializeSizeBytes.labels(topic, schemaId).observe(bytes.length);
        } catch (Exception e) {
            serializeLatency.labels(topic, schemaId, ERROR).observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            LOGGER.error("Could not serialize payload", Map.of(TOPIC, topic, ERROR_MESSAGE, String.valueOf(e.getMessage())));
            throw e;
        }
        return bytes;
    }

    @Override
    public byte[] serialize(final String topic, final Headers headers, final O data) {
        byte[] bytes;
        String schemaId = (data instanceof GenericRecord) ? ((GenericRecord) data).getSchema().getFullName() : UNKNOWN;
        long startTimeNanos = System.nanoTime();
        try {
            bytes = pieSerializer.serialize(topic, headers == null ? PIE_HEADERS : headers, data);
            serializeLatency.labels(topic, schemaId, SUCCESS).observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            serializeSizeBytes.labels(topic, schemaId).observe(bytes.length);
        } catch (Exception e) {
            serializeLatency.labels(topic, schemaId, ERROR).observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            LOGGER.error("Could not serialize payload with headers", Map.of(TOPIC, topic, ERROR_MESSAGE, String.valueOf(e.getMessage())));
            throw e;
        }
        return bytes;
    }

    @Override
    public void close() {
        pieSerializer.close();
    }
}

