package com.apple.aml.stargate.beam.sdk.predicates;

import com.apple.aml.stargate.common.options.KVFilterOptions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;

import static com.apple.aml.stargate.common.nodes.StargateNode.nodeName;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class KVFilterPredicate implements SerializableFunction<KV<String, GenericRecord>, Boolean> {
    private final FreemarkerPredicate freemarkerPredicate;
    private final KVFilterOptions options;

    public KVFilterPredicate(final KVFilterOptions options, final String nodeName, final String nodeType) {
        this.options = options;
        this.freemarkerPredicate = isBlank(options.getExpression()) ? null : new FreemarkerPredicate(options.getExpression().trim(), nodeName(nodeName, "expression-filter"));
    }

    @Override
    public Boolean apply(final KV<String, GenericRecord> kv) {
        boolean allow = options.shouldOutput(kv.getKey(), kv.getValue().getSchema().getFullName());
        if (options.isNegate()) {
            return allow && (freemarkerPredicate == null || !freemarkerPredicate.apply(kv));
        }
        return allow && (freemarkerPredicate == null || freemarkerPredicate.apply(kv));
    }
}
