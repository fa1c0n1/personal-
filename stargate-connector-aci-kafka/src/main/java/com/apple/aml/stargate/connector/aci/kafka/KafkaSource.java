package com.apple.aml.stargate.connector.aci.kafka;

import com.apple.aml.stargate.beam.sdk.io.kafka.KafkaMessage;
import com.apple.aml.stargate.common.options.ACIKafkaOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.common.TopicPartition;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class KafkaSource extends UnboundedSource<KV<String, KafkaMessage>, KafkaCheckMark> implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String nodeName;
    private final String nodeType;
    private final Map<String, Object> config;
    private final List<KV<TopicPartition, Triple<Long, Long, Long>>> partitionGroup;
    private final ACIKafkaOptions options;
    private final Coder<KafkaMessage> coder;
    private final KV<TopicPartition, Triple<Long, Long, Long>> currentPartition;

    public KafkaSource(final String nodeName, final String nodeType, final ACIKafkaOptions options, final Map<String, Object> config, final List<KV<TopicPartition, Triple<Long, Long, Long>>> partitionGroup, final Coder<KafkaMessage> coder) {
        this(nodeName, nodeType, config, partitionGroup, null, coder, options);
    }

    public KafkaSource(final String nodeName, final String nodeType, final Map<String, Object> config, final List<KV<TopicPartition, Triple<Long, Long, Long>>> partitionGroup, final KV<TopicPartition, Triple<Long, Long, Long>> currentPartition, final Coder<KafkaMessage> coder, final ACIKafkaOptions options) {
        this.config = config;
        this.partitionGroup = partitionGroup;
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.currentPartition = currentPartition;
        this.options = options;
        this.coder = coder;
    }

    @Override
    public List<? extends UnboundedSource<KV<String, KafkaMessage>, KafkaCheckMark>> split(final int desiredNumSplits, final PipelineOptions pipelineOptions) throws Exception {
        if (currentPartition != null) return Collections.singletonList(this);
        return partitionGroup.stream().map(x -> new KafkaSource(nodeName, nodeType, config, partitionGroup, x, coder, this.options)).collect(Collectors.toList());
    }

    @Override
    public UnboundedReader<KV<String, KafkaMessage>> createReader(final PipelineOptions pipelineOptions, @Nullable final KafkaCheckMark checkpointMark) throws IOException {
        if (currentPartition == null) return null;
        TopicPartition topicPartition = currentPartition.getKey();
        KafkaCheckMark checkMark = checkpointMark;
        if (checkMark == null) {
            checkMark = new KafkaCheckMark(topicPartition.topic(), topicPartition.partition(), currentPartition.getValue().getLeft(), BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis(), Optional.empty());
        } else {
            if (!topicPartition.topic().equals(checkMark.getTopic()) || topicPartition.partition() != checkMark.getPartition()) throw new IOException("Checkpoint doesn't match with requested topicPartition");
            if (checkMark.getOffset() <= 0) checkMark.setOffset(currentPartition.getValue().getLeft());
        }
        return new KafkaReader(this, checkMark, nodeName, nodeType, config, options);
    }

    @Override
    public Coder<KafkaCheckMark> getCheckpointMarkCoder() {
        return AvroCoder.of(KafkaCheckMark.class);
    }

    @Override
    public Coder<KV<String, KafkaMessage>> getOutputCoder() {
        return KvCoder.of(StringUtf8Coder.of(), coder);
    }
}
