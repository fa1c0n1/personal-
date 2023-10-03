package com.apple.aml.stargate.beam.sdk.io.http;

import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.HttpInvokerOptions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;

import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class HttpIO {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    @SuppressWarnings("unchecked")
    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return collection.apply(node.getName(), new HttpInvoker(node, (HttpInvokerOptions) node.getConfig(), false));
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return collection.apply(node.getName(), new HttpInvoker(node, (HttpInvokerOptions) node.getConfig(), true));
    }
}
