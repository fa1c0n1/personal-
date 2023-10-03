package com.apple.aml.stargate.beam.sdk.ts;

import com.apple.aml.stargate.beam.inject.BeamContext;
import com.apple.aml.stargate.beam.sdk.utils.BeamUtils;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.utils.ContextHandler;
import com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;

import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;

public class FreemarkerEvaluator extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> implements Serializable {
    private BaseFreemarkerEvaluator evaluator = new BaseFreemarkerEvaluator(BeamUtils.nodeService(), BeamUtils.errorService());

    public void initWrite(final Pipeline pipeline, final StargateNode node) throws Exception {
        evaluator.initFreemarkerNode(node, false);
    }

    public void initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        evaluator.initFreemarkerNode(node, true);
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return collection.apply(node.getName(), this);
    }

    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return collection.apply(node.getName(), this);
    }

    public void initFreemarkerNode(final Pipeline pipeline, final StargateNode node, final boolean emit) throws Exception {
        evaluator.initFreemarkerNode(node, emit);
    }

    public Pair<String, Object> bean() {
        return null; // Will be used by classes extending this transformer ( Unfortunately with beam+annotations, you cannot override `processElement` hence this approach )
    }

    @Setup
    public void setup() throws Exception {
        evaluator.setup();
    }

    @SuppressWarnings({"unchecked"})
    @DoFn.ProcessElement
    public void processElement(@DoFn.Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
        ContextHandler.setContext(BeamContext.builder().windowedContext(ctx).build());
        evaluator.processElement(kv, bean(), skv -> ctx.output(skv), ekv -> ctx.output(ERROR_TAG, ekv));
    }

}