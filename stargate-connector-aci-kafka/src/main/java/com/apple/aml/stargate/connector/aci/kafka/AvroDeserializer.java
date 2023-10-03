package com.apple.aml.stargate.connector.aci.kafka;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.pojo.AvroRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.SerializationConstants.CONFIG_NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.SerializationConstants.CONFIG_NODE_TYPE;
import static com.apple.aml.stargate.common.constants.KafkaConstants.TOPIC;
import static com.apple.aml.stargate.common.constants.KafkaConstants.getCompatibleACIHeaders;
import static com.apple.aml.stargate.common.utils.AppConfig.environment;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramBytes;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static java.nio.charset.StandardCharsets.UTF_8;

abstract class AvroDeserializer implements Deserializer<GenericRecord> {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final RecordHeaders PIE_HEADERS = getCompatibleACIHeaders();
    private static final Schema ERROR_SCHEMA;

    static {
        try {
            ERROR_SCHEMA = errorSchema(environment());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Deserializer<GenericRecord> backingDeserializer;
    private Deserializer<GenericRecord> fallbackDeserializer;
    private String nodeName = UNKNOWN;
    private String nodeType = UNKNOWN;

    public static Schema errorSchema(final PipelineConstants.ENVIRONMENT environment) throws IOException {
        Schema.Parser parser = new Schema.Parser();
        String schemaString = IOUtils.resourceToString("/kafkadeserializererror.avsc", Charset.defaultCharset()).replaceAll("\\#\\{ENV\\}", environment.name().toLowerCase());
        return parser.parse(schemaString);
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public void configure(final Map<String, ?> cMap, final boolean isKey) {
        Map<String, Object> configs = (Map<String, Object>) cMap;
        nodeName = (String) configs.getOrDefault(CONFIG_NODE_NAME, UNKNOWN);
        nodeType = (String) configs.getOrDefault(CONFIG_NODE_TYPE, UNKNOWN);
        Pair<Deserializer<GenericRecord>, Deserializer<GenericRecord>> pair = deserializerInstances(configs, isKey);
        backingDeserializer = pair.getKey();
        fallbackDeserializer = pair.getValue();
    }

    abstract public Pair<Deserializer<GenericRecord>, Deserializer<GenericRecord>> deserializerInstances(final Map<String, ?> configs, final boolean isKey);

    @Override
    public GenericRecord deserialize(final String topic, final byte[] data) {
        return deserialize(topic, PIE_HEADERS, data);
    }

    @Override
    public GenericRecord deserialize(final String topic, final Headers headers, final byte[] data) {
        if (data == null) {
            counter(nodeName, nodeType, "NULL_MESSAGE", "null_message").inc();
            return null;
        }
        GenericRecord object;
        String schemaId = null;
        long startTimeNanos = System.nanoTime();
        try {
            Pair<String, GenericRecord> pair = _deserializePayload(backingDeserializer, topic, headers, data);
            schemaId = pair.getKey();
            object = pair.getValue();
            double timeTaken = (System.nanoTime() - startTimeNanos) / 1000000.0;
            histogramDuration(nodeName, nodeType, schemaId, "deserialize_success", TOPIC, topic).observe(timeTaken);
            histogramBytes(nodeName, nodeType, schemaId, "deserialize_success", TOPIC, topic).observe(data.length);
        } catch (Exception e) {
            Map<String, String> headersMap;
            if (headers != null) {
                headersMap = new HashMap<>();
                headers.forEach(h -> headersMap.put(h.key(), h.value() == null ? "null" : new String(h.value(), UTF_8)));
            } else {
                headersMap = Map.of();
            }
            if (schemaId == null) {
                Pair<String, Integer> schemaDetails = schemaDetails(headers);
                headersMap.put("schemaName", schemaDetails.getLeft());
                headersMap.put("schemaVersion", String.valueOf(schemaDetails.getRight()));
                schemaId = schemaDetails.getLeft();
            }
            double timeTaken = (System.nanoTime() - startTimeNanos) / 1000000.0;
            histogramDuration(nodeName, nodeType, schemaId, "deserialize_error", TOPIC, topic).observe(timeTaken);
            histogramBytes(nodeName, nodeType, schemaId, "deserialize_error", TOPIC, topic).observe(data.length);
            LOGGER.warn("Could not deserialize kafka payload", Map.of(TOPIC, topic, SCHEMA_ID, schemaId, "payloadSize", data.length, ERROR_MESSAGE, String.valueOf(e.getMessage()), "headers", headersMap));
            GenericRecord record = new GenericData.Record(ERROR_SCHEMA);
            record.put(TOPIC, topic);
            record.put("headers", headersMap);
            record.put(ERROR_MESSAGE, String.valueOf(e.getMessage()));
            record.put("data", data == null ? null : new String(Base64.getEncoder().encode(data), StandardCharsets.UTF_8));
            record.put("exception", new String(Base64.getEncoder().encode(SerializationUtils.serialize(e)), StandardCharsets.UTF_8));
            return record;
        } finally {
            histogramDuration(nodeName, nodeType, schemaId == null ? UNKNOWN : schemaId, "deserialize", TOPIC, topic).observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            histogramBytes(nodeName, nodeType, schemaId == null ? UNKNOWN : schemaId, "deserialize", TOPIC, topic).observe(data.length);
        }
        return object;
    }

    @Override
    public void close() {
        backingDeserializer.close();
        if (fallbackDeserializer != null) fallbackDeserializer.close();
    }

    abstract public Pair<String, Integer> schemaDetails(final Headers headers);

    private Pair<String, GenericRecord> _deserializePayload(final Deserializer<GenericRecord> deserializer, final String topic, final Headers headers, final byte[] data) {
        GenericRecord object;
        String schemaId;
        long startTimeNanos = System.nanoTime();
        try {
            object = deserializer.deserialize(topic, headers == null ? PIE_HEADERS : headers, data);
            if (object instanceof GenericRecord && !(object instanceof AvroRecord)) {
                Pair<String, Integer> schemaDetails = schemaDetails(headers);
                if (schemaDetails != null && schemaDetails.getRight() >= 0) {
                    object = new AvroRecord(object, schemaDetails.getRight());
                }
            }
            schemaId = (object instanceof GenericRecord) ? (object).getSchema().getFullName() : UNKNOWN;
            histogramDuration(nodeName, nodeType, schemaId, "deserializer_success", TOPIC, topic).observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            return Pair.of(schemaId, object);
        } catch (Exception e) {
            if (fallbackDeserializer == null || fallbackDeserializer == deserializer) throw e;
            LOGGER.warn("Failed to deserialize using standard deserializer. Will try using fallback deserializer now", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), TOPIC, topic, "payloadSize", data.length));
            try {
                return _deserializePayload(fallbackDeserializer, topic, headers, data);
            } catch (Exception fe) {
                LOGGER.warn("Failed to deserialize even using fallback deserializer. Will throw original exception", Map.of("fallbackException", String.valueOf(e.getMessage()), TOPIC, topic, "payloadSize", data.length));
                throw e;
            }
        }
    }
}
