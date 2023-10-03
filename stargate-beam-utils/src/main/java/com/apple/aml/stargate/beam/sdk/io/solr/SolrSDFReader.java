package com.apple.aml.stargate.beam.sdk.io.solr;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.options.SolrOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.utils.FormatUtils.parseDateTime;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;


public class SolrSDFReader extends SolrOperations {
    protected static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final long serialVersionUID = 1L;

    public SolrSDFReader(final String nodeName, final String nodeType, final PipelineConstants.ENVIRONMENT env, final Instant pipelineInvokeTime, final SolrOptions options) throws Exception {
        super(nodeName, nodeType, env, pipelineInvokeTime, options);
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@Element final Long epoc) throws Exception {
        long start = 0;
        long end = Long.MAX_VALUE;
        if (timestampFieldName != null) {
            Instant readFrom = null;
            if (option.getReadFrom() != null && !option.getReadFrom().trim().isBlank()) {
                readFrom = parseDateTime(option.getReadFrom().trim());
                LOGGER.debug("Custom readFrom supplied! Will use that as starting timestamp", Map.of("readFrom", option.getReadFrom().trim(), "resolved", readFrom == null ? "null" : readFrom.toString(), "resolvedEpoc", readFrom == null ? -1 : readFrom.toEpochMilli()));
            }
            if (option.isBounded()) {
                start = readFrom == null ? (option.isUseLatestTimestamp() && "full".equalsIgnoreCase(option.getCollectionType()) ? fetchLatestTimestampValue() : 0) : timestampUnit.convert(readFrom.toEpochMilli(), TimeUnit.MILLISECONDS);
                end = timestampUnit.convert(epoc, TimeUnit.MILLISECONDS);
            } else {
                start = readFrom == null ? (option.isUseLatestTimestamp() ? timestampUnit.convert(epoc, TimeUnit.MILLISECONDS) : 0) : timestampUnit.convert(readFrom.toEpochMilli(), TimeUnit.MILLISECONDS);
                end = Long.MAX_VALUE;
            }
        }
        LOGGER.debug("Initial Restriction is set to", Map.of("start", start, "end", end, NODE_NAME, nodeName, "collection", collection));
        return new OffsetRange(start, end);
    }

    @GetRestrictionCoder
    public Coder<OffsetRange> restrictionCoder() {
        return new OffsetRange.Coder();
    }

    @NewTracker
    public OffsetRangeTracker restrictionTracker(@Element final Long epoc, @Restriction final OffsetRange restriction) {
        return new OffsetRangeTracker(restriction);
    }

}
