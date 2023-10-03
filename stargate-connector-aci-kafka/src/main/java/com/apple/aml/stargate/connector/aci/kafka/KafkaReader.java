package com.apple.aml.stargate.connector.aci.kafka;

import com.apple.aml.stargate.beam.sdk.io.kafka.KafkaMessage;
import com.apple.aml.stargate.common.options.ACIKafkaOptions;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.Instant;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.apple.aml.stargate.beam.sdk.io.kafka.KafkaMessage.kafkaMessage;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.gauge;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramSize;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;

public class KafkaReader extends UnboundedSource.UnboundedReader<KV<String, KafkaMessage>> {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final KafkaSource source;
    private final String nodeName;
    private final String nodeType;
    private final Map<String, Object> config;
    private final ACIKafkaOptions options;
    private final int maxRecords;
    private final TopicPartition topicPartition;
    private final String topicPartitionLabel;
    private final AtomicReference<KafkaCheckMark> checkpointReference = new AtomicReference<>();
    private final AtomicReference<KafkaConsumer<String, byte[]>> consumerReference = new AtomicReference<>();
    private final ArrayBlockingQueue<ConsumerRecord<String, byte[]>> records;
    private final long maxPollRecords;
    private final AtomicLong latestOffset = new AtomicLong();
    private final AtomicLong offsetTracker = new AtomicLong();
    private final AtomicLong offsetLocker = new AtomicLong();
    private final boolean pollOnAdvance;
    private final OffsetState state;
    private ExecutorService pollService;
    private ExecutorService latestOffsetPollService;
    private ScheduledExecutorService commitService;
    private RateLimiter rateLimiter;
    private ConsumerRecord<String, byte[]> current = null;

    public KafkaReader(final KafkaSource source, final KafkaCheckMark mark, final String nodeName, final String nodeType, final Map<String, Object> config, final ACIKafkaOptions options) {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.source = source;
        this.config = config;
        this.options = options;
        mark.setReader(Optional.of(this));
        this.checkpointReference.set(mark);
        this.pollOnAdvance = options.getPollers() <= 0 && this.options.getMinInMemoryRecords() > 0;
        this.topicPartition = new TopicPartition(mark.getTopic(), mark.getPartition());
        this.state = OffsetState.newState(nodeName, nodeType, mark);
        this.latestOffset.set(this.state.getNextOffset());
        this.offsetTracker.set(this.state.getNextOffset());
        this.offsetLocker.set(this.state.getNextOffset());
        this.topicPartitionLabel = String.format("%s:%4d", topicPartition.topic(), topicPartition.partition());
        this.maxPollRecords = config.containsKey(MAX_POLL_RECORDS_CONFIG) ? Long.parseLong(config.get(MAX_POLL_RECORDS_CONFIG).toString()) : (options.getMaxPollRecords() <= 0 ? 100L : options.getMaxPollRecords());
        this.rateLimiter = options.getMaxTps() <= 0 ? null : RateLimiter.create(options.getMaxTps());
        this.maxRecords = options.getMaxInMemoryRecords() > 0 ? options.getMaxInMemoryRecords() : Math.min((int) (options.getMaxTps() <= 0 ? (10 * this.maxPollRecords) : (2 * options.getMaxTps())), 1000);
        this.records = new ArrayBlockingQueue<>(this.maxRecords);
    }

    @Override
    @SuppressWarnings("deprecation")
    public boolean start() throws IOException {
        LOGGER.debug("Starting Kafka reader now.", Map.of("noOfPollers", options.getPollers(), "pollOnAdvance", pollOnAdvance, "minRecords", options.getMinInMemoryRecords(), "maxRecords", maxRecords, "rateLimit", rateLimiter == null ? -1 : rateLimiter.getRate(), "commitInterval", options.getCommitInterval().toSeconds()), state.logMap());
        if (options.getPollers() >= 2 || options.isEnableLatestOffsetPoller()) {
            this.latestOffsetPollService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setDaemon(true).setNameFormat(String.format("%s-offset-poller-%s-%s", nodeName.toLowerCase(), topicPartition.topic(), topicPartition.partition())).build());
            latestOffsetPollService.submit(this::offsetPoll);
        }
        KafkaConsumer<String, byte[]> consumer = consumer();
        ConsumerRecords<String, byte[]> consumerRecords = consumer.poll(0);
        if (!records.isEmpty()) {
            try {
                consumeRecords(consumerRecords);
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(String.format("%s-poller-%s-%s-%d", nodeName.toLowerCase(), topicPartition.topic(), topicPartition.partition())).build();
        if (pollOnAdvance) {
            LOGGER.debug("Starting reader in poll when required/on-advance mode", state.logMap());
            this.pollService = Executors.newSingleThreadExecutor(threadFactory);
            this.pollService.submit(this::pollToMemory);
        } else if (options.getPollers() <= 1) {
            LOGGER.debug("Starting reader in daemon poll mode", state.logMap());
            this.pollService = Executors.newSingleThreadExecutor(threadFactory);
            this.pollService.submit(this::daemonPoll);
        } else {
            LOGGER.debug("Starting concurrent daemon pollers", state.logMap());
            this.pollService = Executors.newFixedThreadPool(options.getPollers(), threadFactory);
            for (int i = 0; i < options.getPollers(); i++) {
                int poller = i;
                this.pollService.submit(() -> concurrentPoll(poller));
            }
        }
        commitService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).setNameFormat(String.format("%s-committer-%s-%s", nodeName.toLowerCase(), topicPartition.topic(), topicPartition.partition())).build());
        commitService.scheduleWithFixedDelay(this::commitOffset, options.getCommitInterval().toSeconds(), options.getCommitInterval().toSeconds(), TimeUnit.SECONDS);
        return false;
    }

    @Override
    public boolean advance() throws IOException {
        current = records.poll();
        if (current == null) {
            if (pollOnAdvance) pollService.submit(this::pollToMemory);
            return false;
        }
        state.recordEmitted(current);
        long size = records.size();
        gauge(nodeName, nodeType, UNKNOWN, "in_memory_records", "topic:partition", topicPartitionLabel).set(size);
        if (pollOnAdvance && size < options.getMinInMemoryRecords()) pollService.submit(this::pollToMemory);
        if (rateLimiter != null) rateLimiter.acquire();
        return true;
    }

    @Override
    public Instant getWatermark() {
        return Instant.ofEpochMilli(state.getWaterMark());
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
        return new KafkaCheckMark(topicPartition.topic(), topicPartition.partition(), state.getEmittedOffset(), state.getWaterMark(), Optional.of(this));
    }

    @Override
    public UnboundedSource<KV<String, KafkaMessage>, ?> getCurrentSource() {
        return source;
    }

    void pollToMemory() {
        _pollToMemory();
    }

    synchronized void _pollToMemory() {
        KafkaConsumer<String, byte[]> consumer = null;
        try {
            consumer = consumer();
            while (records.size() < options.getMinInMemoryRecords()) {
                _poll(consumer);
            }
        } catch (Exception e) {
            LOGGER.warn("Error in consuming from kafka. Will retry in next advance", state.logMap(), Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            consumerReference.set(null);
            try {
                Closeables.close(consumer, true);
            } catch (Exception ignored) {

            }
        }
    }

    KafkaConsumer<String, byte[]> consumer() {
        KafkaConsumer<String, byte[]> consumer = consumerReference.get();
        if (consumer != null) return consumer;
        consumer = new KafkaConsumer<>(config);
        consumer.assign(ImmutableList.of(topicPartition));
        consumer.seek(topicPartition, state.getNextOffset());
        consumerReference.compareAndSet(null, consumer);
        return consumer;
    }

    private boolean _poll(final KafkaConsumer<String, byte[]> consumer) throws Exception {
        consumer.seek(topicPartition, state.getNextOffset());
        long pollStartTime = System.nanoTime();
        ConsumerRecords<String, byte[]> consumerRecords = consumer.poll(options.getPollTimeout());
        gauge(nodeName, nodeType, UNKNOWN, "poller_poll_wait_time_micro", "topic:partition", topicPartitionLabel).set((System.nanoTime() - pollStartTime) / 1000.0);
        int pollRecordCount = consumerRecords.isEmpty() ? 0 : consumerRecords.count();
        histogramSize(nodeName, nodeType, UNKNOWN, "poll_consumed", "topic:partition", topicPartitionLabel).observe(pollRecordCount);
        if (pollRecordCount == 0) return true;
        consumeRecords(consumerRecords);
        _commitOffset(consumer);
        return false;
    }

    private void consumeRecords(final ConsumerRecords<String, byte[]> consumerRecords) throws InterruptedException {
        int count = 0;
        long queueStartTime = System.nanoTime();
        for (ConsumerRecord<String, byte[]> record : consumerRecords) {
            records.put(record);
            state.recordConsumed(record);
            count++;
        }
        gauge(nodeName, nodeType, UNKNOWN, "poller_queue_segment_time_micro", "topic:partition", topicPartitionLabel).set((System.nanoTime() - queueStartTime) / 1000.0);
        histogramSize(nodeName, nodeType, UNKNOWN, "poll_consumed", "topic:partition", topicPartitionLabel).observe(count);
    }

    void _commitOffset(final KafkaConsumer<String, byte[]> consumer) {
        long startTime = System.nanoTime();
        KafkaCheckMark mark = null;
        try {
            long offsetToCommit = 0;
            if (options.isCommitWithoutCheckpoint()) {
                offsetToCommit = state.getEmittedOffset();
            } else {
                mark = checkpointReference.getAndSet(null);
                if (mark == null) {
                    counter(nodeName, nodeType, UNKNOWN, "empty_checkpoint_mark", "topic:partition", topicPartitionLabel).inc();
                    return;
                }
                offsetToCommit = mark.getOffset();
            }
            offsetToCommit += 1;
            if (state.getCommittedOffset() == offsetToCommit) {
                return;
            }
            if (state.getCommittedOffset() > offsetToCommit) {
                counter(nodeName, nodeType, UNKNOWN, "unaligned_commit_offset", "topic:partition", topicPartitionLabel).inc();
                return;
            }
            consumer.commitSync(Map.of(topicPartition, new OffsetAndMetadata(offsetToCommit)));
            state.recordCommitted(offsetToCommit);
            counter(nodeName, nodeType, UNKNOWN, "commit_success", "topic:partition", topicPartitionLabel).inc();
        } catch (Exception e) {
            if (mark != null) checkpointReference.compareAndSet(null, mark);
            LOGGER.warn("Could not commit kafka offset. Will retry later", state.logMap(), Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            counter(nodeName, nodeType, UNKNOWN, "commit_error", "topic:partition", topicPartitionLabel).inc();
        } finally {
            histogramDuration(nodeName, nodeType, UNKNOWN, "commit_offset", "topic:partition", topicPartitionLabel).observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

    void offsetPoll() {
        while (true) {
            KafkaConsumer<String, byte[]> consumer = null;
            List<TopicPartition> partitions = ImmutableList.of(topicPartition);
            try {
                Map<String, Object> updated = new HashMap<>(config);
                updated.put(GROUP_ID_CONFIG, String.format("offset-%s", updated.get(GROUP_ID_CONFIG)));
                consumer = new KafkaConsumer<>(config);
                consumer.assign(partitions);
                while (true) {
                    latestOffset.set(consumer.endOffsets(partitions).get(topicPartition));
                    gauge(nodeName, nodeType, UNKNOWN, "latest_offset", "topic:partition", topicPartitionLabel).set(latestOffset.get());
                    Thread.sleep(100);
                }
            } catch (Exception e) {
                LOGGER.warn("Error in fetching offset from kafka. Will close this consumer and retry again", state.logMap(), Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                try {
                    Closeables.close(consumer, true);
                } catch (Exception ignored) {

                }
            }
        }
    }

    void commitOffset() {
        KafkaConsumer<String, byte[]> consumer = null;
        try {
            consumer = new KafkaConsumer<>(config);
            consumer.assign(ImmutableList.of(topicPartition));
            if (consumer == null) return;
            _commitOffset(consumer);
        } catch (Exception e) {
            LOGGER.error("Error in performing  kafka commit. Will try again in next scheduled commit..", state.logMap(), Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            try {
                Closeables.close(consumer, true);
            } catch (Exception ignored) {

            }
        }
    }

    void daemonPoll() {
        while (true) {
            KafkaConsumer<String, byte[]> consumer = null;
            try {
                consumer = consumer();
                while (true) {
                    _poll(consumer);
                }
            } catch (Exception e) {
                LOGGER.warn("Error in consuming from kafka. Will close this consumer and retry again", state.logMap(), Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                consumerReference.set(null);
                try {
                    Closeables.close(consumer, true);
                } catch (Exception ignored) {

                }
            }
        }
    }

    int concurrentPoll(final int poller) {
        String pollerLabel = String.format("%s:%d", topicPartitionLabel, poller);
        while (true) {
            KafkaConsumer<String, byte[]> consumer = null;
            try {
                consumer = new KafkaConsumer<>(config);
                consumer.assign(ImmutableList.of(topicPartition));
                if (consumer == null) continue;
                while (true) {
                    _concurrentPoll(consumer, poller, pollerLabel);
                }
            } catch (Exception e) {
                LOGGER.error("Error in performing  concurrent poll", state.logMap(), Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                try {
                    Closeables.close(consumer, true);
                } catch (Exception ignored) {

                }
                throw new IllegalStateException(e); // TODO : Need graceful exit and/or error handling ( like reset tracker to last success emit )
            }
        }
    }

    void _concurrentPoll(final KafkaConsumer<String, byte[]> consumer, final int poller, final String pollerLabel) throws Exception {
        if (latestOffset.get() - offsetLocker.get() < maxPollRecords) return;
        long startTime = System.nanoTime();
        long offsetLockTime;
        long startOffset;
        synchronized (offsetLocker) {
            offsetLockTime = System.nanoTime() - startTime;
            if (latestOffset.get() - offsetLocker.get() < maxPollRecords) return;
            startOffset = offsetLocker.getAndAdd(maxPollRecords);
        }
        gauge(nodeName, nodeType, UNKNOWN, "poller_offset_locker_time_micro", "topic:partition:poller", pollerLabel).set(offsetLockTime / 1000.0);
        startTime = System.nanoTime();
        List<ConsumerRecord<String, byte[]>> recordList = new ArrayList<>();
        while (recordList.size() < maxPollRecords) {
            consumer.seek(topicPartition, startOffset + recordList.size());
            long pollStartTime = System.nanoTime();
            ConsumerRecords<String, byte[]> consumerRecords = consumer.poll(options.getPollTimeout());
            gauge(nodeName, nodeType, UNKNOWN, "poller_poll_wait_time_micro", "topic:partition:poller", pollerLabel).set((System.nanoTime() - pollStartTime) / 1000.0);
            int pollRecordCount = consumerRecords.isEmpty() ? 0 : consumerRecords.count();
            histogramSize(nodeName, nodeType, UNKNOWN, "poll_consumed", "topic:partition", topicPartitionLabel).observe(pollRecordCount);
            if (pollRecordCount == 0) continue;
            for (ConsumerRecord<String, byte[]> record : consumerRecords) {
                recordList.add(record);
                if (recordList.size() == maxPollRecords) break;
            }
        }
        gauge(nodeName, nodeType, UNKNOWN, "poller_poll_segment_time_micro", "topic:partition:poller", pollerLabel).set((System.nanoTime() - startTime) / 1000.0);
        counter(nodeName, nodeType, UNKNOWN, "poller_consumed", "topic:partition:poller", pollerLabel).inc(recordList.size());
        startTime = System.nanoTime();
        while (offsetTracker.get() < startOffset) {
            Thread.sleep(5); // TODO : We should have max timelimit to spend in this waiting loop
        }
        gauge(nodeName, nodeType, UNKNOWN, "poller_segment_resume_time_micro", "topic:partition:poller", pollerLabel).set((System.nanoTime() - startTime) / 1000.0);
        if (offsetTracker.get() > startOffset) {
            LOGGER.error("Error in synchronizing kafka pollers!! ");
            throw new IllegalStateException("Error in synchronizing kafka pollers!!");
        }
        startTime = System.nanoTime();
        long timeTakenToQueue;
        synchronized (offsetTracker) {
            offsetLockTime = System.nanoTime() - startTime;
            if (offsetTracker.get() != startOffset) throw new IllegalStateException("Error in synchronizing kafka pollers!!");
            long queueStartTime = System.nanoTime();
            for (ConsumerRecord<String, byte[]> record : recordList) {
                records.put(record);
                state.recordConsumed(record);
            }
            timeTakenToQueue = System.nanoTime() - queueStartTime;
            offsetTracker.getAndAdd(recordList.size());
        }
        gauge(nodeName, nodeType, UNKNOWN, "poller_offset_tracker_time_micro", "topic:partition:poller", pollerLabel).set(offsetLockTime / 1000.0);
        gauge(nodeName, nodeType, UNKNOWN, "poller_queue_segment_time_micro", "topic:partition:poller", pollerLabel).set(timeTakenToQueue / 1000.0);
    }

    @Override
    public KV<String, KafkaMessage> getCurrent() throws NoSuchElementException {
        if (current == null) throw new NoSuchElementException();
        return KV.of(options.isAppendOffsetToKey() ? String.format("%s:%d:%d", current.key(), current.partition(), current.offset()) : current.key(), kafkaMessage(current.topic(), current.headers(), current.value(), current.partition(), current.offset()));
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
        if (current == null) throw new NoSuchElementException();
        return Instant.ofEpochMilli(current.timestamp());
    }

    @Override
    public void close() throws IOException {
        commitService.shutdown();
        if (pollService != null) pollService.shutdown();
        if (latestOffsetPollService != null) latestOffsetPollService.shutdown();
        if (consumerReference.get() != null) Closeables.close(consumerReference.get(), true);
    }

    void finalizeCheckpointMarkAsync(final KafkaCheckMark mark) {
        KafkaCheckMark old = checkpointReference.getAndSet(mark);
        gauge(nodeName, nodeType, UNKNOWN, "checkpoint_queued", "topic:partition", topicPartitionLabel).set(mark.getOffset());
        if (old != null) {
            gauge(nodeName, nodeType, UNKNOWN, "checkpoint_skipped", "topic:partition", topicPartitionLabel).set(old.getOffset());
        }
    }
}
