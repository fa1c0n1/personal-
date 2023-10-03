package com.apple.aml.stargate.beam.sdk.ts;

import com.apple.aml.stargate.beam.sdk.predicates.KVFilterPredicate;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.KVFilterOptions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_SKIPPED;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;

public class PredicateFilter extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> implements Serializable {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final long serialVersionUID = 1L;
    private String nodeName;
    private String nodeType;
    private SerializableFunction<KV<String, GenericRecord>, Boolean> predicate;

    public static PredicateFilter filterBy(final SerializableFunction<KV<String, GenericRecord>, Boolean> predicate, final String nodeName, final String nodeType) {
        PredicateFilter filter = new PredicateFilter();
        filter.initialize(predicate, nodeName, nodeType);
        return filter;
    }

    private void initialize(final SerializableFunction<KV<String, GenericRecord>, Boolean> predicate, final String nodeName, final String nodeType) {
        this.predicate = predicate;
        this.nodeName = nodeName;
        this.nodeType = nodeType;
    }

    @SuppressWarnings("unchecked")
    public void initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        final KVFilterOptions options = (KVFilterOptions) node.getConfig();
        this.initialize(new KVFilterPredicate(options, node.getName(), node.getType()), node.getName(), node.getType());
    }

    @ProcessElement
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
        String schemaId = UNKNOWN;
        boolean emit;
        try {
            try {
                schemaId = kv.getValue() == null ? UNKNOWN : kv.getValue().getSchema().getFullName();
            } catch (Exception ignored) {
            }
            counter(nodeName, nodeType, schemaId, ELEMENTS_IN).inc();
            emit = predicate.apply(kv);
            if (!emit) {
                counter(nodeName, nodeType, schemaId, ELEMENTS_SKIPPED).inc();
                return;
            }
        } catch (Exception e) {
            counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
            LOGGER.warn("Error evaluating the predicate", Map.of("key", String.valueOf(kv.getKey()), SCHEMA_ID, schemaId, NODE_NAME, nodeName, NODE_TYPE, nodeType, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            ctx.output(ERROR_TAG, eRecord(nodeName, nodeType, "eval_predicate", kv, e));
            return;
        }
        ctx.output(kv);
        incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, SOURCE_SCHEMA_ID, schemaId);
    }

}
