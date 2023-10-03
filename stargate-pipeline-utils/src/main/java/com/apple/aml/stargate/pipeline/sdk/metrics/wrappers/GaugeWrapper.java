package com.apple.aml.stargate.pipeline.sdk.metrics.wrappers;

import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.exprGauge;

public class GaugeWrapper {
    private final String nodeName;
    private final String nodeType;
    private final String schemaId;

    public GaugeWrapper(final String nodeName, final String nodeType, final String schemaId) {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.schemaId = schemaId;
    }

    public boolean inc(final String key) {
        return incBy(key, 1);
    }

    public boolean incBy(final String key, final Number value) {
        exprGauge(nodeName, nodeType, schemaId, key).inc(value.doubleValue());
        return true;
    }

    public boolean dec(final String key) {
        return incBy(key, -1);
    }

    public boolean set(final String key, final Number value) {
        exprGauge(nodeName, nodeType, schemaId, key).set(value.doubleValue());
        return true;
    }
}
