package com.apple.aml.stargate.connector.aci.kafka;

import com.google.common.io.Closeables;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.gauge;

public class KafkaCommitter extends DoFn<KV<TopicPartition, Long>, Void> implements Serializable {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final ConcurrentHashMap<String, ConcurrentHashMap<String, AtomicLong>> COMMIT_MAP = new ConcurrentHashMap<>();
    private final String nodeName;
    private final String nodeType;
    private final Map<String, Object> config;
    private AtomicReference<KafkaConsumer> reference;

    public KafkaCommitter(final String nodeName, final String nodeType, final Map<String, Object> config) {
        this.config = config;
        this.nodeName = nodeName;
        this.nodeType = nodeType;
    }

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() throws Exception {
        reference = new AtomicReference<>(new KafkaConsumer(config));
    }

    @StartBundle
    @SuppressWarnings("unchecked")
    public void startBundle() {
        reference.compareAndSet(null, new KafkaConsumer(config));
    }

    @Teardown
    public void teardown() throws Exception {
        Closeables.close(reference.getAndSet(null), true);
    }

    @RequiresStableInput
    @ProcessElement
    @SuppressWarnings("unchecked")
    public void processElement(@Element final KV<TopicPartition, Long> kv) throws Exception {
        KafkaConsumer consumer = null;
        TopicPartition record = kv.getKey();
        long commitOffset = kv.getValue();
        long existingCommit = -1;
        try {
            consumer = reference.get();
            if (consumer == null) {
                reference.compareAndSet(null, new KafkaConsumer(config));
                consumer = reference.get();
            }
            String topicPartitionLabel = String.format("%s:%4d", record.topic(), record.partition());
            AtomicLong eLong = COMMIT_MAP.computeIfAbsent(nodeName, x -> new ConcurrentHashMap<>()).computeIfAbsent(topicPartitionLabel, x -> new AtomicLong(Long.MIN_VALUE));
            existingCommit = eLong.get();
            if (existingCommit > commitOffset) {
                LOGGER.warn("Commit with older offset received; Will ignore this commit as we have already moved further", Map.of("requestedOffset", commitOffset, "existingOffset", existingCommit, NODE_NAME, nodeName, "topic", record.topic(), "partition", record.partition()));
                return;
            }
            synchronized (eLong) {
                consumer.commitSync(Map.of(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(commitOffset)));
                if (eLong.get() < commitOffset) eLong.set(commitOffset);
            }
            gauge(nodeName, nodeType, UNKNOWN, "offset_commit", "topic:partition", topicPartitionLabel).set(eLong.get());
        } catch (Exception e) {
            LOGGER.warn("Could not commit kafka offset", Map.of("commitOffset", commitOffset, "recordOffset", kv.getValue(), ERROR_MESSAGE, String.valueOf(e.getMessage()), NODE_NAME, nodeName, NODE_TYPE, nodeType, "topic", record.topic(), "partition", record.partition()), e);
            reference.set(null);
            Closeables.close(consumer, true);
            throw e;
        }
    }
}
