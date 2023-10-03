package com.apple.aml.stargate.beam.inject;

import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.services.ErrorService;
import com.apple.aml.stargate.common.utils.ContextHandler;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;

public class BeamErrorHandler implements ErrorService, Serializable {
    private static final ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap = new ConcurrentHashMap<>();

    @Override
    public void publishError(final Exception exception, final Map<String, Object> map) throws Exception {
        publishError(exception, map, (String) map.get("stargateKey"));
    }

    @Override
    public void publishError(final Exception exception, final Map<String, Object> map, final String key) throws Exception {
        Schema schema = ContextHandler.ctx().getSchema();
        GenericRecord record = converterMap.computeIfAbsent(schema.getFullName(), s -> converter(schema)).convert(map);
        publishError(exception, record, key);
    }

    @Override
    public void publishError(final Exception exception, final GenericRecord record) {
        publishError(exception, record, null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void publishError(final Exception exception, final GenericRecord record, final String key) {
        ContextHandler.Context ctx = ContextHandler.ctx();
        ((BeamContext) ctx).getWindowedContext().output(ERROR_TAG, eRecord(ctx.getNodeName(), ctx.getNodeType(), "error_publish", KV.of(key == null ? UUID.randomUUID().toString() : key, record), exception));
    }

    @Override
    public void publishError(final Exception exception, final Object pojo) throws Exception {
        publishError(exception, pojo, null);
    }

    @Override
    public void publishError(final Exception exception, final Object pojo, final String key) throws Exception {
        Schema schema = ContextHandler.ctx().getSchema();
        GenericRecord record = converterMap.computeIfAbsent(schema.getFullName(), s -> converter(schema)).convert(pojo);
        publishError(exception, record, key);
    }
}
