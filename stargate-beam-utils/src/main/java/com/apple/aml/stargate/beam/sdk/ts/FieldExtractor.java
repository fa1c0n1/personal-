package com.apple.aml.stargate.beam.sdk.ts;

import com.apple.aml.stargate.beam.inject.BeamContext;
import com.apple.aml.stargate.beam.sdk.utils.BeamUtils;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.utils.ContextHandler;
import com.apple.aml.stargate.pipeline.sdk.ts.BaseFieldExtractor;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;

import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;

public class FieldExtractor extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> implements Serializable {
    private static final long serialVersionUID = 1L;
    private BaseFieldExtractor evaluator = new BaseFieldExtractor(BeamUtils.nodeService(), BeamUtils.errorService());

    public void initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        evaluator.initTransform(node);
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return collection.apply(node.getName(), this);
    }

    @Setup
    public void setup() throws Exception {
        evaluator.setup();
    }

    @ProcessElement
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
        ContextHandler.setContext(BeamContext.builder().windowedContext(ctx).build());
        evaluator.processElement(kv, skv -> ctx.output(skv), ekv -> ctx.output(ERROR_TAG, ekv));
    }
}
