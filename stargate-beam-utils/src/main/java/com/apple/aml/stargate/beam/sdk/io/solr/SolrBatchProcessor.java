package com.apple.aml.stargate.beam.sdk.io.solr;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.options.SolrOptions;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.time.Instant;

import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class SolrBatchProcessor extends SolrOperations {
    protected static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final long serialVersionUID = 1L;

    public SolrBatchProcessor(final String nodeName, final String nodeType, final PipelineConstants.ENVIRONMENT env, final Instant pipelineInvokeTime, final SolrOptions options) throws Exception {
        super(nodeName, nodeType, env, pipelineInvokeTime, options);
    }

    @ProcessElement
    public void processElement(@Element final Long batchId, final ProcessContext ctx) throws Exception {
        long available = batchId;
        processSolrClaim(ctx, configuration(), available, available);
        if ("full".equalsIgnoreCase(option.getCollectionType())) processSolrClaim(ctx, configuration(), 0L, available, true);
    }

}
