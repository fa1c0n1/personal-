package com.apple.aml.stargate.connector.aci.kafka;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.ACIKafkaOptions;
import freemarker.template.Configuration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.KafkaConstants.TOPIC;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.getInternalSchema;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;
import static com.apple.jvm.commons.util.Strings.isBlank;

public final class KafkaPublisher extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final long serialVersionUID = 1L;
    private final boolean emit;
    private final boolean randomPayloadKey;
    private final Map<String, Object> configs;
    private final Map<String, String> schemaIdTopicMappings;
    private final String topicExpression;
    private final String topic;
    private final String topicPrefix;
    private final Integer partition;
    private final int totalPartitions;
    private final PipelineConstants.DATA_FORMAT format;
    private final boolean emitMetadata;
    private final ObjectToGenericRecordConverter converter;
    private final String nodeName;
    private final String nodeType;
    private final Level logPayloadLevel;
    private final String topicTemplateName;
    private transient Producer<String, Object> producer = null;
    private transient Configuration configuration;

    public KafkaPublisher(final boolean emit, final ACIKafkaOptions options, final StargateNode node, final Map<String, Object> configs) {
        this.emit = emit;
        this.configs = configs;
        this.randomPayloadKey = options.isRandomPayloadKey();
        this.schemaIdTopicMappings = options.getSchemaIdTopicMappings() == null ? new HashMap<>() : options.getSchemaIdTopicMappings();
        this.topicExpression = options.getTopicExpression();
        this.topic = options.getTopic();
        this.topicPrefix = options.isFullyQualifiedTopicName() ? "" : String.format("%s.%s.", options.getGroup(), options.getNamespace());
        this.partition = (options.getPartition() == null || options.getPartition() < 0) ? null : options.getPartition();
        this.emitMetadata = options.isEmitMetadata();
        this.format = options.dataFormat();
        this.totalPartitions = (options.getTotalPartitions() == null || options.getTotalPartitions() < 0) ? 0 : options.getTotalPartitions();
        this.converter = this.emitMetadata ? converter(getKafkaMetadataInternalSchema(node.environment())) : null;
        this.nodeName = node.getName();
        this.nodeType = node.getType();
        this.logPayloadLevel = options.logPayloadLevel();
        topicTemplateName = nodeName + "~TOPIC";
    }

    public static Schema getKafkaMetadataInternalSchema(PipelineConstants.ENVIRONMENT environment) {
        return getInternalSchema(environment, "kafkametadata.avsc", "{\"type\":\"record\",\"name\":\"KafkaMetadata\",\"namespace\":\"com.apple.aml.stargate.#{ENV}.internal\",\"fields\":[{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"offset\",\"type\":\"long\"},{\"name\":\"partition\",\"type\":\"int\"},{\"name\":\"topic\",\"type\":\"string\"},{\"name\":\"serializedKeySize\",\"type\":\"int\"},{\"name\":\"serializedValueSize\",\"type\":\"int\"},{\"name\":\"key\",\"type\":\"string\"},{\"name\":\"schemaId\",\"type\":\"string\"}]}");
    }

    @Setup
    public void setup() {
        if (producer == null) {
            producer = new KafkaProducer<>(configs);
            LOGGER.info("Created a new kafka producer successfully!!", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType));
        }
        configuration();
    }

    @SuppressWarnings("unchecked")
    private Configuration configuration() {
        if (configuration != null) return configuration;
        if (topicExpression != null) loadFreemarkerTemplate(topicTemplateName, topicExpression);
        configuration = freeMarkerConfiguration();
        return configuration;
    }

    @ProcessElement
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
        log(logPayloadLevel, nodeName, nodeType, kv);
        Schema schema = kv.getValue().getSchema();
        String schemaId = schema.getFullName();
        counter(nodeName, nodeType, schemaId, ELEMENTS_IN).inc();
        String topicName;
        if (topicExpression == null) {
            topicName = schemaIdTopicMappings.getOrDefault(schemaId, topic);
        } else {
            Configuration configuration = configuration();
            try {
                topicName = evaluateFreemarker(configuration, topicTemplateName, kv.getKey(), kv.getValue(), schema);
            } catch (Exception e) {
                counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
                ctx.output(ERROR_TAG, eRecord(nodeName, nodeType, "eval_topic_name", kv, e));
                return;
            }
            if (isBlank(topicName)) {
                topicName = topic;
            }
        }
        topicName = topicPrefix + topicName.trim();
        long startTime = System.nanoTime();
        String kafkaKey = randomPayloadKey ? UUID.randomUUID().toString() : kv.getKey();
        Integer kafkaPartition = partition == null ? (totalPartitions <= 0 ? null : Math.abs(kafkaKey.hashCode() % totalPartitions)) : partition;
        String finalTopicName = topicName;
        producer.send(new ProducerRecord<>(topicName, kafkaPartition, kafkaKey, format == PipelineConstants.DATA_FORMAT.avro ? kv.getValue() : kv.getValue().toString()), (metadata, exception) -> {
            if (exception != null) {
                counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
                histogramDuration(nodeName, nodeType, schemaId, "process_error", TOPIC, finalTopicName).observe((System.nanoTime() - startTime) / 1000000.0);
                counter(nodeName, nodeType, schemaId, "process_error").inc();
                LOGGER.warn("Error in publishing payload to kafka", Map.of(ERROR_MESSAGE, String.valueOf(exception.getMessage()), NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, schemaId, "key", kv.getKey()), exception);
                ctx.output(ERROR_TAG, eRecord(nodeName, nodeType, "publish_record", kv, exception));
                return;
            }
            histogramDuration(nodeName, nodeType, schemaId, "process", TOPIC, finalTopicName).observe((System.nanoTime() - startTime) / 1000000.0);
            counter(nodeName, nodeType, schemaId, "process").inc();
            if (!emit) return;
            if (emitMetadata) {
                Map data = Map.of("timestamp", metadata.timestamp(), "offset", metadata.offset(), "partition", metadata.partition(), TOPIC, metadata.topic(), "serializedKeySize", metadata.serializedKeySize(), "serializedValueSize", metadata.serializedValueSize(), "key", kv.getKey(), SCHEMA_ID, schemaId);
                try {
                    incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, TOPIC, finalTopicName, SOURCE_SCHEMA_ID, schemaId);
                    ctx.output(KV.of(kv.getKey(), converter.convert(data)));
                } catch (Exception e) {
                    incCounters(nodeName, nodeType, schemaId, "elements_metadata_error", TOPIC, finalTopicName);
                    LOGGER.warn("Error in forwarding kafka success metadata to next node", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), NODE_NAME, nodeName, NODE_TYPE, nodeType), data, e);
                    ctx.output(ERROR_TAG, eRecord(nodeName, nodeType, "emit_success_metadata", kv, e));
                }
            } else {
                incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, TOPIC, finalTopicName, SOURCE_SCHEMA_ID, schemaId);
                ctx.output(kv);
            }
        });
    }

    @FinishBundle
    public void finishBundle() throws IOException {
        producer.flush();
    }

    @Teardown
    public void teardown() {
        producer.close();
    }
}
