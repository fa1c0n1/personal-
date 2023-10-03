package com.apple.aml.stargate.connector.stratos;

import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.StratosOptions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;

import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class StratosQueueIO {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    public SCollection<KV<String, GenericRecord>> read(final Pipeline pipeline, final StargateNode node) throws Exception {
        StratosOptions stratosOptions = (StratosOptions) node.getConfig();
        Coder<GenericRecord> avroCoder = pipeline.getCoderRegistry().getCoder(GenericRecord.class);
        Coder<KV<String, GenericRecord>> kvCoder = KvCoder.of(StringUtf8Coder.of(), avroCoder);
        return SCollection.of(pipeline, pipeline.apply(node.getName(), org.apache.beam.sdk.io.Read.from(new StratosSource(node.getName(), node.getType(), stratosOptions, kvCoder))));
    }
}
