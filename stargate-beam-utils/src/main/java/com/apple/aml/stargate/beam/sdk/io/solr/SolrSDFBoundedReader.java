package com.apple.aml.stargate.beam.sdk.io.solr;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.options.SolrOptions;
import freemarker.template.Configuration;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;

@BoundedPerElement
public class SolrSDFBoundedReader extends SolrSDFReader {
    public SolrSDFBoundedReader(final String nodeName, final String nodeType, final PipelineConstants.ENVIRONMENT env, final Instant pipelineInvokeTime, final SolrOptions options) throws Exception {
        super(nodeName, nodeType, env, pipelineInvokeTime, options);
    }

    @SplitRestriction
    public void splitRestriction(final @Restriction OffsetRange restriction, final OutputReceiver<OffsetRange> splitReceiver) {
        List<OffsetRange> offsetRangeList = new ArrayList<>();
        Instant end = Instant.ofEpochMilli(TimeUnit.MILLISECONDS.convert(restriction.getTo(), timestampUnit));
        Instant start;
        if (restriction.getFrom() == 0) {
            start = end.minus(option.getBoundedSplitSize(), ChronoUnit.DAYS);
            offsetRangeList.add(new OffsetRange(0, timestampUnit.convert(start.toEpochMilli(), TimeUnit.MILLISECONDS)));
        } else {
            start = Instant.ofEpochMilli(TimeUnit.MILLISECONDS.convert(restriction.getFrom(), timestampUnit));
        }
        Instant endMinusOne = end.minus(1, ChronoUnit.DAYS);
        while (start.isBefore(endMinusOne)) {
            Instant newStart = start.plus(1, ChronoUnit.DAYS);
            offsetRangeList.add(new OffsetRange(timestampUnit.convert(start.toEpochMilli(), TimeUnit.MILLISECONDS), timestampUnit.convert(newStart.toEpochMilli(), TimeUnit.MILLISECONDS)));
            start = newStart;
        }
        if (start.toEpochMilli() < end.toEpochMilli()) {
            offsetRangeList.add(new OffsetRange(timestampUnit.convert(start.toEpochMilli(), TimeUnit.MILLISECONDS), timestampUnit.convert(end.toEpochMilli(), TimeUnit.MILLISECONDS)));
        }
        if (!option.isUseTimestampStats()) {
            offsetRangeList.forEach(range -> {
                LOGGER.debug("Adding new split with range " + range, Map.of("start", range.getFrom(), "end", range.getTo(), NODE_NAME, nodeName, "collection", collection));
                splitReceiver.output(range);
            });
            return;
        }
        offsetRangeList.forEach(range -> {
            LOGGER.debug("Querying solr to split current range further" + range, Map.of("start", range.getFrom(), "end", range.getTo(), NODE_NAME, nodeName, "collection", collection));
            try {
                fetchDistinctTimestampRanges(range).forEach(splitRange -> {
                    LOGGER.debug("Adding new split with range " + splitRange, Map.of("start", splitRange.getFrom(), "end", splitRange.getTo(), NODE_NAME, nodeName, "collection", collection));
                    splitReceiver.output(splitRange);
                });
            } catch (Exception e) {
                LOGGER.debug("Could not split current range further" + range, Map.of("start", range.getFrom(), "end", range.getTo(), NODE_NAME, nodeName, "collection", collection, ERROR_MESSAGE, String.valueOf(e.getMessage())));
                splitReceiver.output(range);
            }
        });
    }

    @ProcessElement
    public void processElement(@Element final Long epoc, final RestrictionTracker<OffsetRange, Long> tracker, final ProcessContext ctx) {
        Configuration configuration = null;
        if (keyTemplateName != null) {
            configuration = freeMarkerConfiguration();
            loadFreemarkerTemplate(keyTemplateName, option.getKeyExpression());
        }
        Configuration finalConfiguration = configuration;
        OffsetRange offsetRange = tracker.currentRestriction();
        long start = offsetRange.getFrom();
        long end = offsetRange.getTo();
        if (tracker.tryClaim(end - 1)) {
            processSolrClaim(ctx, finalConfiguration, start, end);
        }
    }
}
