package com.apple.aml.stargate.beam.sdk.io.splunk;

import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.SplunkOptions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.splunk.SplunkIO;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Map;

import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.applyWindow;
import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.batchPartitionKey;
import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.batchWriter;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.GENERIC_RECORD_GROUP_TAG;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public final class GenericSplunkIO {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        SplunkOptions options = applyOptionsOverrides((SplunkOptions) node.getConfig(), node);
        if (options.getCommType() != null && options.getCommType().toLowerCase().contains("forward")) {
            return collection.apply(node.getName(), new LogbackWriter<>(node.getName(), node.getType(), options, true));
        }
        if (options.getBatchSize() <= 0 && options.getBatchDuration() == null) options.setBatchDuration(Duration.ofSeconds(1));
        return batchCollection(node, collection, options, options.isEmitSuccessRecords(), true);
    }

    private SplunkOptions applyOptionsOverrides(SplunkOptions options, final StargateNode node) {
        if (options == null) options = new SplunkOptions();
        if ("RawLog".equalsIgnoreCase(node.getType()) || "RawSplunk".equalsIgnoreCase(node.getType())) {
            options.setCommType("SplunkForwarder");
        }
        return options;
    }

    private SCollection<KV<String, GenericRecord>> batchCollection(final StargateNode node, final SCollection<KV<String, GenericRecord>> collection, final SplunkOptions options, final boolean emitSuccessRecords, final boolean emitHecErrors) throws Exception {
        if (options.getConnectionOptions() != null) LOGGER.debug("Http connection options specified", readJsonMap(jsonString(options.getConnectionOptions())));
        LOGGER.debug("Optimized batching enabled. Will apply batching", Map.of("batchSize", options.getBatchSize(), "partitions", options.getPartitions()));
        SCollection<KV<String, GenericRecord>> window = applyWindow(collection, options, node.getName());
        PipelineConstants.ENVIRONMENT environment = node.environment();
        return window.group(node.name("partition-key"), batchPartitionKey(options.getPartitions(), options.getPartitionStrategy()), GENERIC_RECORD_GROUP_TAG).apply(node.name("batch"), batchWriter(node.getName(), node.getType(), environment, options, new SplunkWriter<>(node.getName(), node.getType(), environment, options, emitSuccessRecords, emitHecErrors)));
    }

    private SplunkIO.Write writer(final SplunkOptions options) {
        SplunkIO.Write writer = SplunkIO.write(options.getUrl(), options.getToken());
        if (options.batchCount() > 0) {
            writer = writer.withBatchCount(options.batchCount());
        }
        if (options.parallelism() > 0) {
            writer = writer.withParallelism(options.parallelism());
        }
        return writer;
    }

    @SuppressWarnings("unchecked")
    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        SplunkOptions options = applyOptionsOverrides((SplunkOptions) node.getConfig(), node);
        if (options.getCommType() != null && options.getCommType().toLowerCase().contains("forward")) {
            return collection.apply(node.getName(), new LogbackWriter(node.getName(), node.getType(), options, false));
        }
        if (options.getBatchSize() <= 0 && options.getBatchDuration() == null) options.setBatchDuration(Duration.ofSeconds(1));
        return batchCollection(node, collection, options, false, false);
    }
}
