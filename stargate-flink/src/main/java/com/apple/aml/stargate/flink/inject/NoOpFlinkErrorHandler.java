package com.apple.aml.stargate.flink.inject;

import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import com.apple.aml.stargate.common.pojo.ErrorRecord;
import com.apple.aml.stargate.common.services.ErrorService;
import com.apple.aml.stargate.common.utils.ContextHandler;
import com.apple.aml.stargate.flink.utils.FlinkUtils;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class NoOpFlinkErrorHandler implements ErrorService, Serializable {
    @Override
    public void publishError(final Exception exception, final Map<String, Object> map) throws Exception {
    }

    @Override
    public void publishError(final Exception exception, final Map<String, Object> map, final String key) throws Exception {
    }

    @Override
    public void publishError(final Exception exception, final GenericRecord record) {
    }

    @Override
    @SuppressWarnings("unchecked")
    public void publishError(final Exception exception, final GenericRecord record, final String key) {
        //TODO need to figure out how to emit the output as it reqires us to register a seperate source for the same.
    }

    @Override
    public void publishError(final Exception exception, final Object pojo) throws Exception {
    }

    @Override
    public void publishError(final Exception exception, final Object pojo, final String key) throws Exception {
    }
}
