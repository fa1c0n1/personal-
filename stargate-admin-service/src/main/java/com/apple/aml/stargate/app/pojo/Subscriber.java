package com.apple.aml.stargate.app.pojo;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import com.apple.aml.stargate.common.utils.ACIPrivateKeyProvider;
import com.apple.aml.stargate.common.utils.AppConfig;
import com.apple.pie.queue.client.ext.schema.avro.KafkaAvroDeserializer;
import com.apple.pie.queue.client.ext.schema.avro.schema.resolver.UseWriteSchema;
import com.apple.pie.queue.kafka.client.KafkaClientUtils;
import com.apple.pie.queue.kafka.client.configinterceptors.ConsumerGroupIdConfigurationInterceptor;
import com.apple.pie.queue.kafka.client.configinterceptors.KaffeConsumerConfigurationInterceptor;
import com.apple.pie.queue.kafka.client.configinterceptors.PieDeserializerConfigurationInterceptor;
import com.fasterxml.jackson.annotation.JsonMerge;
import lombok.Data;
import lombok.ToString;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.KafkaConstants.appendSchemaStoreEndpointDetails;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SCHEMA_REFERENCE_TYPE.AppleSchemaStore;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SCHEMA_REFERENCE_TYPE.schemaReferenceType;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static com.apple.pie.queue.client.ext.schema.store.apple.AppleSchemaStoreConfig.SCHEMA_STORE_CLIENT_SERVICE_NAME_CONFIG;
import static java.util.Collections.singletonList;

@Data
public class Subscriber implements Closeable {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private String appId;
    private String subscriberId;
    private String type;
    private String uri;
    private String apiUri;
    private String groupId;
    @ToString.Exclude
    private String apiToken;
    private String group;
    private String namespace;
    private String topic;
    private String clientId;
    @ToString.Exclude
    private String privateKey; // Base64 encoded privateKey
    @JsonMerge
    private Map<String, Object> props; // other Kafka Properties
    private String schemaReferenceType; // defaults to use in case it is not supplied in consume-call headers
    private String schemaReference; // defaults to use in case it is not supplied in consume-call headers
    private String schemaId; // defaults to use in case it is not supplied in consume-call headers
    @JsonMerge
    private Map<String, String> logHeadersMapping;
    private Map<String, Object> configs;
    private Consumer<String, GenericRecord> consumer;

    public Subscriber() {

    }

    public void init(final Map<String, Object> defaults, final String groupId, final String topic) {
        this.configs = new HashMap<>();
        if (defaults != null) {
            configs.putAll(defaults);
        }
        if (this.getProps() != null) {
            configs.putAll(this.getProps());
        }
        if (!isBlank(groupId)) {
            this.groupId = groupId;
        }
        if (!isBlank(topic)) {
            this.topic = topic;
        }
        initACIConsumer();
    }

    private void initACIConsumer() {
        configs.put("pie.queue.kaffe.connect", this.getUri());
        configs.put("pie.queue.kaffe.config.injection", true);
        configs.put("pie.queue.kaffe.client.id", this.clientId());
        configs.put("pie.queue.kaffe.namespace.id", this.namespace());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId());
        ACIPrivateKeyProvider.setPrivateKeyProvider(configs, this.getPrivateKey());
        configs.put("key.deserializer", StringDeserializer.class);
        PipelineConstants.SCHEMA_REFERENCE_TYPE schemaReferenceType = schemaReferenceType(this.getSchemaReferenceType());
        if (schemaReferenceType == null || schemaReferenceType == AppleSchemaStore) {
            appendSchemaStoreEndpointDetails(configs, isBlank(this.getSchemaReference()) ? AppConfig.environment().getConfig().getSchemaReference() : this.getSchemaReference());
            configs.put(SCHEMA_STORE_CLIENT_SERVICE_NAME_CONFIG, this.clientId() + "." + this.getAppId());
        }
        configs.put("config.interceptors.classes", Stream.of(ConsumerGroupIdConfigurationInterceptor.class, KaffeConsumerConfigurationInterceptor.class, PieDeserializerConfigurationInterceptor.class).map(Class::getCanonicalName).collect(Collectors.joining(",")));
        configs.put("value.deserializer", KafkaAvroDeserializer.class);
        configs.put("pie.queue.ext.avro.read.schema.resolver.factory.class", UseWriteSchema.Factory.class);
        configs.put("enable.auto.commit", true);
        this.consumer = KafkaClientUtils.createConsumer(configs);
    }

    public String clientId() {
        String client = (this.clientId == null || this.clientId.isBlank()) ? subscriberId : clientId;
        return this.group == null ? client : (this.group + "." + client);
    }

    protected String namespace() {
        return this.group == null ? this.getNamespace() : (this.group + "." + this.getNamespace());
    }

    public String groupId() {
        String groupId = (this.groupId == null || this.clientId.isBlank()) ? "consumerGroupId" : this.groupId;
        return this.clientId() + "." + groupId;
    }

    public List<GenericRecord> consumeMessagesBySchemaId(final String schemaId, final int partitionId, final long offset, final long noOfMessages, final long runtimeDurationMillis) throws Exception {
        final long startTime = System.nanoTime();
        final String fullTopicName = this.fullTopicName();
        long recordTotalCount = 0L;
        List<GenericRecord> messages = new ArrayList<>();
        LOGGER.debug("Starting consumer loop for topic", Map.of(SCHEMA_ID, String.valueOf(schemaId), "partition", partitionId, "offset", offset, "noOfMessages", noOfMessages, "runtimeDurationMillis", runtimeDurationMillis, "fullTopicName", this.fullTopicName()));
        try {
            long pollTimeoutMs = 301; //TBD
            TopicPartition topicPartition = null;
            if (partitionId < 0) {
                this.consumer().subscribe(singletonList(fullTopicName));
            } else {
                topicPartition = new TopicPartition(fullTopicName, partitionId);
                this.consumer().assign(singletonList(topicPartition));
            }
            while (startTime + runtimeDurationMillis > System.nanoTime()) {
                ConsumerRecords<String, GenericRecord> records;
                try {
                    if (offset <= 0) {
                        this.consumer().seekToBeginning(this.consumer().assignment());
                    } else if (topicPartition != null) {
                        this.consumer().seek(topicPartition, offset);
                    } else {
                        throw new InvalidInputException("offset can be supplied only when specific partitionId is supplied");
                    }
                    records = this.consumer().poll(Duration.ofMillis(pollTimeoutMs));
                } catch (Exception e) {
                    LOGGER.error("Failed to poll records from topic : " + this.getTopic() + ". Reason : " + e.getMessage());
                    continue;
                }
                Iterable<ConsumerRecord<String, GenericRecord>> consumerRecords = records.records(fullTopicName);
                for (ConsumerRecord<String, GenericRecord> record : consumerRecords) {
                    recordTotalCount++;
                    LOGGER.debug("Record {} : Offset {}, Partition {}, Topic {}", recordTotalCount, record.offset(), record.partition(), record.topic());
                    if (isBlank(schemaId)) {
                        LOGGER.debug("Find a message with schemaId {}", record.value().getSchema().getFullName());
                        messages.add(record.value());
                    } else if (record.value().getSchema().getFullName().equals(schemaId)) {
                        LOGGER.debug("Find a matched message with schemaId {}", schemaId);
                        messages.add(record.value());
                    }
                    if (messages.size() == noOfMessages) {
                        LOGGER.debug("Successfully found all {} matched messages", noOfMessages);
                        return messages;
                    }
                }
                if (recordTotalCount != 0 && messages.size() != 0) {
                    LOGGER.debug("Only found {} matched messages", messages.size());
                    return messages;
                }
            }
        } catch (Exception e) {
            throw new Exception("Failed while consuming messages. Reason : " + e.getMessage(), e);
        } finally {
            LOGGER.debug("Consumer Finished - Runtime [{}millisec], Consumed [{}] Total Records", (System.nanoTime() - startTime) / 1000000.0, recordTotalCount);
            this.consumer().unsubscribe();
        }
        return messages;
    }

    public String fullTopicName() {
        return this.namespace() + "." + this.topic;
    }

    public Consumer<String, GenericRecord> consumer() {
        return consumer;
    }

    @Override
    public void close() {
        if (consumer == null) {
            return;
        }
        consumer.close();
    }
}
