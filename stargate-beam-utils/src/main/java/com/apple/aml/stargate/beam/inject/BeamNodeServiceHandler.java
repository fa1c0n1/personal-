package com.apple.aml.stargate.beam.inject;

import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.services.NodeService;
import com.apple.aml.stargate.common.utils.ContextHandler;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.beam.sdk.ts.JavaFunction.emitOutput;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class BeamNodeServiceHandler implements NodeService, Serializable {
    private static final ConcurrentHashMap<String, ConcurrentHashMap<String, ObjectToGenericRecordConverter>> converterMap = new ConcurrentHashMap<>();

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
        return ((BeamContext) ContextHandler.ctx()).getInputSchemaId();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void emit(final String schemaId, final String _key, final Object object) throws Exception {
        String key = isBlank(_key) ? UUID.randomUUID().toString() : _key;
        BeamContext ctx = (BeamContext) ContextHandler.ctx();
        String targetSchemaId = isBlank(schemaId) ? (ctx.getOutputSchemaId() == null ? ctx.getInputSchemaId() : ctx.getOutputSchemaId()) : schemaId;
        emitOutput(KV.of(key, null), key, object, ctx.getWindowedContext(), ctx.getSchema(), ctx.getInputSchemaId(), targetSchemaId, ctx.getConverter(), converterMap.computeIfAbsent(ctx.getNodeName(), n -> new ConcurrentHashMap<>()), ctx.getNodeName(), ctx.getNodeType());
    }
}
