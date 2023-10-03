package com.apple.aml.stargate.flink.inject;

import com.apple.aml.stargate.common.services.NodeService;
import com.apple.aml.stargate.common.utils.ContextHandler;

import java.io.Serializable;

public class NoOpFlinkNodeServiceHandler implements NodeService, Serializable {

    @Override
    public String nodeName() {
        return ContextHandler.ctx().getNodeName();
    }

    @Override
    public String nodeType() {
        return ContextHandler.ctx().getNodeType();
    }

    @Override
    public String inputSchemaId() {
        return ((FlinkContext) ContextHandler.ctx()).getInputSchemaId();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void emit(final String schemaId, final String _key, final Object object) throws Exception {
    }
}
