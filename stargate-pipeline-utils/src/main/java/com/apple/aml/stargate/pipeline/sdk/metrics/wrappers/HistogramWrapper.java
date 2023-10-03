package com.apple.aml.stargate.pipeline.sdk.metrics.wrappers;

import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.exprHistogram;

public class HistogramWrapper {
    private final String nodeName;
    private final String nodeType;
    private final String schemaId;

    public HistogramWrapper(final String nodeName, final String nodeType, final String schemaId) {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.schemaId = schemaId;
    }

    public boolean observe(final String key, final Number value) {
        exprHistogram(nodeName, nodeType, schemaId, key).observe(value.doubleValue());
        return true;
    }
}
