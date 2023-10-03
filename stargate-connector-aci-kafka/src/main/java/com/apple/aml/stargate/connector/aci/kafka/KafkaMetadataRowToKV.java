package com.apple.aml.stargate.connector.aci.kafka;

import com.apple.aml.stargate.beam.sdk.transforms.ByteArrayToGenericRecord;
import com.apple.aml.stargate.common.constants.PipelineConstants.DATA_FORMAT;
import com.apple.aml.stargate.common.constants.PipelineConstants.ENVIRONMENT;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.options.ACIKafkaOptions;
import com.apple.aml.stargate.common.pojo.AvroRecord;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.KEY;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.SCHEMA;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.TARGET_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_NULL;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_LATEST_VERSION;
import static com.apple.aml.stargate.common.constants.KafkaConstants.TOPIC;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.SchemaUtils.getLocalSchema;
import static com.apple.aml.stargate.common.utils.SchemaUtils.kafkaRecordSchema;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.freemarkerSchemaMap;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eJsonRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramBytes;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.saveLocalSchema;
import static java.nio.charset.StandardCharsets.UTF_8;

public class KafkaMetadataRowToKV extends DoFn<Row, KV<String, GenericRecord>> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final boolean isTraceEnabled = LOGGER.isTraceEnabled();
    private final String nodeName;
    private final String nodeType;
    private final ACIKafkaOptions options;
    private final DATA_FORMAT format;
    private final Map<String, Object> configs;
    private final Schema schema;
    private final ObjectToGenericRecordConverter converter;
    private final ConcurrentHashMap<String, Schema> kafkaSchemaMap = new ConcurrentHashMap<>();
    private transient Deserializer<String> keyDeserializer;
    private transient Deserializer valueDeserializer;

    @SneakyThrows
    public KafkaMetadataRowToKV(final String nodeName, final String nodeType, final ACIKafkaOptions options, final Map<String, Object> configs, final ENVIRONMENT environment) {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.options = options;
        this.format = options.dataFormat();
        this.configs = configs;
        switch (format) {
            case avro:
                this.schema = null;
                this.converter = null;
                break;
            case bytearray:
                this.schema = getLocalSchema(environment, "bytearraydata.avsc");
                this.converter = null;
                break;
            default:
                this.schema = fetchSchemaWithLocalFallback(options.getSchemaReference(), options.getSchemaId());
                this.converter = converter(this.schema);
        }
    }

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        if (keyDeserializer == null) {
            Deserializer<String> _deserializer = new StringDeserializer();
            _deserializer.configure(configs, true);
            keyDeserializer = _deserializer;
        }
        if (valueDeserializer == null) {
            Deserializer _deserializer;
            switch (format) {
                case avro:
                    _deserializer = new AvroACIDeserializer();
                    break;
                case bytearraystring:
                    _deserializer = new ByteArrayStringDeserializer();
                    break;
                case bytearray:
                    _deserializer = new ByteArrayDeserializer();
                    break;
                default:
                    _deserializer = new StringDeserializer();
                    break;
            }
            _deserializer.configure(configs, false);
            valueDeserializer = _deserializer;
        }
    }

    @SuppressWarnings("unchecked")
    @ProcessElement
    public void processElement(@Element final Row row, final ProcessContext ctx) throws Exception {
        long startTime = System.nanoTime();
        String topic = row.getString("topic");
        incCounters(nodeName, nodeType, UNKNOWN, ELEMENTS_IN, TOPIC, topic);
        String key = null;
        Map offsetDetails = Map.of();
        try {
            int recordPartition = row.getInt32("partition");
            long recordOffset = row.getInt64("offset");
            long recordTimestamp = row.getInt64("timestamp");
            offsetDetails = Map.of("recordPartition", recordPartition, "recordOffset", recordOffset, "recordTimestamp", recordTimestamp);
            try {
                key = keyDeserializer.deserialize(topic, row.getBytes("key"));
            } catch (Exception e) {
                LOGGER.warn("Could not deserialize kafka message key", offsetDetails, Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), NODE_NAME, nodeName, NODE_TYPE, nodeType, "key", String.valueOf(row.getBytes("key"))), e);
            }
            RecordHeaders headers = new RecordHeaders();
            Iterable headersIterable = row.getIterable("headers");
            if (headersIterable != null) {
                headersIterable.forEach(o -> {
                    if (o == null) return;
                    if (o instanceof Row) {
                        Row header = (Row) o;
                        headers.add(new RecordHeader(header.getString("key"), header.getBytes("value")));
                        return;
                    }
                    throw new UnsupportedOperationException(String.format("%s is not supported for header", o.getClass().getName()));
                });
            }
            Map<String, String> recordHeaders = Arrays.stream(headers.toArray()).map(h -> Pair.of(h.key(), h.value() == null ? "null" : new String(h.value(), UTF_8))).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
            byte[] valueBytes;
            try {
                valueBytes = row.getBytes("value");
            } catch (Exception e) {
                LOGGER.warn("Could not fetch kafka message bytes; Possible null/blank message!!", offsetDetails, recordHeaders, Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), NODE_NAME, nodeName, NODE_TYPE, nodeType, "key", String.valueOf(key)));
                valueBytes = null;
            }
            histogramBytes(nodeName, nodeType, UNKNOWN, "raw_msg_size", TOPIC, topic).observe(valueBytes == null ? 0 : valueBytes.length);
            Object value;
            try {
                value = valueBytes == null ? null : valueDeserializer.deserialize(topic, headers, valueBytes);
            } catch (Exception e) {
                Map logDetails = Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), NODE_NAME, nodeName, NODE_TYPE, nodeType, "key", String.valueOf(key));
                if (isTraceEnabled) LOGGER.trace("Could not deserialize kafka message value", offsetDetails, recordHeaders, logDetails, Map.of("payloadBytesAsString", String.valueOf(valueBytes)), e);
                else LOGGER.warn("Could not deserialize kafka message value", offsetDetails, recordHeaders, logDetails, e);
                counter(nodeName, nodeType, UNKNOWN, ELEMENTS_ERROR).inc();
                throw e;
            }
            if (value == null) {
                incCounters(nodeName, nodeType, UNKNOWN, ELEMENTS_NULL, TOPIC, topic, TARGET_SCHEMA_ID, UNKNOWN);
                histogramDuration(nodeName, nodeType, UNKNOWN, "row_deserialize_success", TOPIC, topic).observe((System.nanoTime() - startTime) / 1000000.0);
                ctx.output(KV.of(key, null));
                return;
            }
            GenericRecord record;
            if (converter != null) {
                record = converter.convert(value);
            } else if (schema != null) {
                record = ByteArrayToGenericRecord.convert((byte[]) value, schema);
            } else {
                record = (GenericRecord) value;
            }
            Schema recordSchema = record.getSchema();
            String recordSchemaId = recordSchema.getFullName();
            int version = record instanceof AvroRecord ? ((AvroRecord) record).getSchemaVersion() : SCHEMA_LATEST_VERSION;
            String recordSchemaKey = String.format("%s:%d", recordSchemaId, version);
            Schema kafkaSchema = kafkaSchemaMap.computeIfAbsent(recordSchemaKey, sKey -> {
                Schema metaSchema = kafkaRecordSchema(recordSchema);
                String metaSchemaId = metaSchema.getFullName();
                String metaSchemaKey = String.format("%s:%d", metaSchemaId, version);
                saveLocalSchema(metaSchemaKey, metaSchema.toString());
                return metaSchema;
            });
            String kafkaSchemaId = kafkaSchema.getFullName();
            GenericRecordBuilder recordBuilder = new GenericRecordBuilder(kafkaSchema);
            recordBuilder.set(kafkaSchema.getField(KEY), key);
            recordBuilder.set(kafkaSchema.getField("payload"), record);
            recordBuilder.set(kafkaSchema.getField(SCHEMA), freemarkerSchemaMap(recordSchema, version));
            recordBuilder.set(kafkaSchema.getField(TOPIC), topic);
            recordBuilder.set(kafkaSchema.getField("partition"), recordPartition);
            recordBuilder.set(kafkaSchema.getField("offset"), recordOffset);
            recordBuilder.set(kafkaSchema.getField("timestamp"), recordTimestamp);
            recordBuilder.set(kafkaSchema.getField("timestampTypeName"), row.getString("timestampTypeName"));
            recordBuilder.set(kafkaSchema.getField("headers"), recordHeaders);
            GenericRecord kafkaRecord = new AvroRecord(recordBuilder.build(), version);
            incCounters(nodeName, nodeType, recordSchemaId, ELEMENTS_OUT, TOPIC, topic, TARGET_SCHEMA_ID, kafkaSchemaId);
            histogramDuration(nodeName, nodeType, recordSchemaId, "row_deserialize_success", TOPIC, topic).observe((System.nanoTime() - startTime) / 1000000.0);
            ctx.output(KV.of(key, kafkaRecord));
        } catch (Exception e) {
            histogramDuration(nodeName, nodeType, UNKNOWN, "row_deserialize_error", TOPIC, topic).observe((System.nanoTime() - startTime) / 1000000.0);
            counter(nodeName, nodeType, UNKNOWN, ELEMENTS_ERROR, TOPIC, topic).inc();
            LOGGER.warn("Error in converting beam row to kv", offsetDetails, Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "key", String.valueOf(key), NODE_NAME, nodeName, NODE_TYPE, nodeType));
            ctx.output(ERROR_TAG, eJsonRecord(nodeName, nodeType, "row_deserialize", KV.of(key, row), e));
        } finally {
            histogramDuration(nodeName, nodeType, UNKNOWN, "row_deserialize", TOPIC, topic).observe((System.nanoTime() - startTime) / 1000000.0);
        }

    }
}
