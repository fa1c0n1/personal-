package com.apple.aml.stargate.connector.aci.kafka;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.gauge;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramBytes;

@DefaultCoder(AvroCoder.class)
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OffsetState {
    private String nodeName;
    private String nodeType;
    private String topic;
    private int partition;
    private String topicPartitionLabel;
    private long emittedOffset;
    private long nextOffset;
    private long committedOffset;
    private long waterMark;
    private long commitTime;

    public static OffsetState newState(final String nodeName, final String nodeType, final KafkaCheckMark mark) {
        OffsetState state = new OffsetState();
        state.nodeName = nodeName;
        state.nodeType = nodeType;
        state.topic = mark.getTopic();
        state.partition = mark.getPartition();
        state.topicPartitionLabel = String.format("%s:%4d", state.topic, state.partition);
        state.emittedOffset = mark.getOffset();
        state.nextOffset = mark.getOffset() + 1;
        state.committedOffset = mark.getOffset() + 1;
        state.waterMark = mark.getWaterMark();
        state.commitTime = System.currentTimeMillis();
        return state;
    }

    public void recordEmitted(final ConsumerRecord<String, byte[]> emitted) {
        long error_delta = emitted.offset() - emittedOffset - 1;
        if (error_delta >= 0) {
            emittedOffset = emitted.offset();
            gauge(nodeName, nodeType, UNKNOWN, "emitted_offset", "topic:partition", topicPartitionLabel).set(emittedOffset);
        }
        gauge(nodeName, nodeType, UNKNOWN, "emitted_delta_offset", "topic:partition", topicPartitionLabel).set(error_delta);
        if (emitted.timestamp() > waterMark) waterMark = emitted.timestamp();
    }

    public void recordConsumed(final ConsumerRecord<String, byte[]> record) {
        long position = record.offset();
        nextOffset = position + 1;
        gauge(nodeName, nodeType, UNKNOWN, "consumed_offset", "topic:partition", topicPartitionLabel).set(position);
        counter(nodeName, nodeType, UNKNOWN, "poll_consumed", "topic:partition", topicPartitionLabel).inc();
        histogramBytes(nodeName, nodeType, UNKNOWN, "raw_msg_size", "topic", record.topic()).observe(record.value().length);
    }

    public void recordCommitted(final long position) {
        committedOffset = position;
        commitTime = System.currentTimeMillis();
        gauge(nodeName, nodeType, UNKNOWN, "committed_offset", "topic:partition", topicPartitionLabel).set(position);
    }

    public Map<String, Object> logMap() {
        return Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, "topic", topic, "partition", partition, "nextOffset", nextOffset, "emittedOffset", emittedOffset, "committedOffset", committedOffset, "commitTime", commitTime, "waterMark", waterMark);
    }
}
