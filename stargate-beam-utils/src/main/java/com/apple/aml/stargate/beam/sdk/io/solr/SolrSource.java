package com.apple.aml.stargate.beam.sdk.io.solr;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.options.SolrOptions;
import lombok.SneakyThrows;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.utils.FormatUtils.parseDateTime;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class SolrSource extends UnboundedSource<Long, SolrCheckMark> implements Serializable {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final long serialVersionUID = 1L;
    protected final TimeUnit timestampUnit;
    private final String nodeName;
    private final String nodeType;
    private final SolrOptions options;
    private final Coder<Long> coder;
    private final java.time.Instant pipelineInvokeTime;
    private final PipelineConstants.ENVIRONMENT environment;

    public SolrSource(final String nodeName, final String nodeType, final PipelineConstants.ENVIRONMENT environment, final SolrOptions options, final Coder<Long> coder, final Instant pipelineInvokeTime) {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.options = options;
        this.coder = coder;
        this.pipelineInvokeTime = pipelineInvokeTime;
        this.environment = environment;
        this.timestampUnit = options.getTimestampUnit() == null || options.getTimestampUnit().trim().isBlank() ? TimeUnit.MILLISECONDS : TimeUnit.valueOf(options.getTimestampUnit().trim().toUpperCase());
    }

    @Override
    public List<? extends UnboundedSource<Long, SolrCheckMark>> split(int desiredNumSplits, PipelineOptions options) throws Exception {
        return Collections.singletonList(this);
    }

    @SneakyThrows
    @Override
    public UnboundedReader<Long> createReader(PipelineOptions options, @Nullable SolrCheckMark checkpointMark) throws IOException {
        SolrCheckMark checkMark = checkpointMark;
        if (checkMark == null) {
            long start = 0;
            long end = Long.MAX_VALUE;
            Instant readFrom = null;
            if (this.options.getReadFrom() != null && !this.options.getReadFrom().trim().isBlank()) {
                readFrom = parseDateTime(this.options.getReadFrom().trim());
                LOGGER.debug("Custom readFrom supplied! Will use that as starting timestamp", Map.of("readFrom", this.options.getReadFrom().trim(), "resolved", readFrom == null ? "null" : readFrom.toString(), "resolvedEpoc", readFrom == null ? -1 : readFrom.toEpochMilli()));
            }
            start = readFrom == null ? (this.options.isUseLatestTimestamp() ? timestampUnit.convert(pipelineInvokeTime.toEpochMilli(), TimeUnit.MILLISECONDS) : 0) : timestampUnit.convert(readFrom.toEpochMilli(), TimeUnit.MILLISECONDS);
            checkMark = new SolrCheckMark(start, end, BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis(), Optional.empty());
            LOGGER.debug("Initial Restriction is set to", Map.of("start", start, "end", end, NODE_NAME, nodeName, "collection", this.options.getCollection()));
        }
        return new SolrUnboundedReader(this, checkMark, nodeName, nodeType, environment, pipelineInvokeTime, this.options);
    }

    @Override
    public Coder<SolrCheckMark> getCheckpointMarkCoder() {
        return AvroCoder.of(SolrCheckMark.class);
    }

    @Override
    public Coder<Long> getOutputCoder() {
        return coder;
    }

}
