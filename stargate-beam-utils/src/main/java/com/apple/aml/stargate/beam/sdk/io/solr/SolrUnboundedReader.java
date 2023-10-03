package com.apple.aml.stargate.beam.sdk.io.solr;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.options.SolrOptions;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.UNBOUNDED_ERROR;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;

public class SolrUnboundedReader extends UnboundedSource.UnboundedReader<Long> {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    protected final SolrOperations solrOps;
    private final SolrSource source;
    private final String nodeName;
    private final String nodeType;
    private final SolrOptions options;
    private final AtomicReference<SolrCheckMark> checkpointReference = new AtomicReference<>();
    private final org.joda.time.Duration pollDuration;
    private final org.joda.time.Duration retryDuration;
    private long currentBatchTs;
    private final AtomicLong nextCheckTime = new AtomicLong(System.currentTimeMillis());


    public SolrUnboundedReader(final SolrSource source, final SolrCheckMark mark, final String nodeName, final String nodeType, final PipelineConstants.ENVIRONMENT environment, final java.time.Instant pipelineInvokeTime, final SolrOptions options) throws Exception {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.source = source;
        this.options = options;
        mark.setReader(Optional.of(this));
        this.checkpointReference.set(mark);
        this.solrOps = new SolrOperations(nodeName, nodeType, environment, pipelineInvokeTime, options);
        pollDuration = org.joda.time.Duration.standardSeconds(this.options.getPollDuration().getSeconds());
        retryDuration = org.joda.time.Duration.standardSeconds(this.options.getPollRetryDuration().getSeconds());
    }

    @Override
    public boolean start() throws IOException {
        return false;
    }

    @Override
    public boolean advance() throws IOException {
        long currentTime = System.currentTimeMillis();
        if (currentTime < nextCheckTime.get()) return false;
        synchronized (nextCheckTime) {
            long available = -1;
            long start = checkpointReference.get().getStart();
            long end = checkpointReference.get().getEnd();
            try {
                long startTime = System.nanoTime();
                available = solrOps.getNextAvailableTimestamp(start, end);
                histogramDuration(nodeName, nodeType, UNKNOWN, "next_available").observe((System.nanoTime() - startTime) / 1000000.0);
                if (available <= -1) {
                    LOGGER.debug("No new batch available", Map.of(NODE_NAME, nodeName, "collection", solrOps.collection));
                    nextCheckTime.set(System.currentTimeMillis() + pollDuration.getMillis());
                    return false;
                }

                if (solrOps.isBatchComplete(available)) {
                    LOGGER.debug("Claimed a new available timestamp. Will process using this timestamp value", Map.of("available", available, NODE_NAME, nodeName, "collection", solrOps.collection));
                    currentBatchTs = available;
                    checkpointReference.set(new SolrCheckMark(available + 1, Long.MAX_VALUE, BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis(), Optional.empty()));
                    return true;
                } else {
                    nextCheckTime.set(System.currentTimeMillis() + pollDuration.getMillis());
                    return false;
                }
            } catch (Exception e) {
                LOGGER.error("Error in processing unbounded range. Will retry after configured retryDuration", Map.of("start", start, "end", end, "available", available, "errorMessage", String.valueOf(e.getMessage()), NODE_NAME, nodeName, "collection", solrOps.collection), e);
                counter(nodeName, nodeType, UNKNOWN, UNBOUNDED_ERROR).inc();
                nextCheckTime.set(System.currentTimeMillis() + retryDuration.getMillis());
                return false;
            }
        }
    }

    @Override
    public Instant getWatermark() {
        return new Instant(checkpointReference.get().getWaterMark());
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
        return new SolrCheckMark(checkpointReference.get().getStart(), checkpointReference.get().getEnd(), checkpointReference.get().getWaterMark(), Optional.of(this));
    }

    @Override
    public UnboundedSource<Long, ?> getCurrentSource() {
        return source;
    }

    @Override
    public Long getCurrent() throws NoSuchElementException {
        return currentBatchTs;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
        return new Instant(currentBatchTs);
    }

    @Override
    public void close() throws IOException {
    }

}
