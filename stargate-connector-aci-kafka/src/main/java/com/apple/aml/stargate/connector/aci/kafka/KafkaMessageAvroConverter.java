package com.apple.aml.stargate.connector.aci.kafka;

import com.apple.aml.stargate.beam.sdk.io.kafka.KafkaMessage;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.Serializable;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.TARGET_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.KafkaConstants.TOPIC;
import static com.apple.aml.stargate.common.utils.AppConfig.environment;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;

public class KafkaMessageAvroConverter extends DoFn<KV<String, KafkaMessage>, KV<String, GenericRecord>> implements Serializable {
    private final String nodeName;
    private final String nodeType;
    private final Map<String, Object> configs;
    private final PipelineConstants.DATA_FORMAT format;
    private transient Deserializer<GenericRecord> deserializer;

    public KafkaMessageAvroConverter(final String nodeName, final String nodeType, final Map<String, Object> configs, final PipelineConstants.DATA_FORMAT format) throws Exception {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.configs = configs;
        this.format = format;
    }

    @Setup
    public void setup() throws Exception {
        if (deserializer == null) {
            Deserializer<GenericRecord> _deserializer = null;
            switch (format) {
                case avro:
                    _deserializer = new AvroACIDeserializer();
                    break;
                case bytearray:
                    _deserializer = new AvroByteArrayDeserializer(environment());
                    break;
                default:
                    _deserializer = new AvroStringDeserializer();
            }

            _deserializer.configure(configs, false);
            deserializer = _deserializer;
        }
    }

    @SuppressWarnings("unchecked")
    @ProcessElement
    public void processElement(@Element final KV<String, KafkaMessage> kv, final ProcessContext ctx) throws Exception {
        KafkaMessage message = kv.getValue();
        incCounters(nodeName, nodeType, UNKNOWN, ELEMENTS_IN, TOPIC, message.getTopic());
        RecordHeaders headers = new RecordHeaders();
        message.getHeaders().forEach(h -> headers.add(new RecordHeader(h.getKey(), h.getValue())));
        GenericRecord record = deserializer.deserialize(message.getTopic(), headers, message.getBytes());
        String schemaId = record == null ? UNKNOWN : record.getSchema().getFullName();
        ctx.output(KV.of(kv.getKey(), record));
        incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, TOPIC, message.getTopic(), TARGET_SCHEMA_ID, schemaId);
    }
}