package com.apple.aml.stargate.pipeline.pojo;

import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.pojo.Holder;
import org.apache.beam.sdk.options.PipelineOptions;

public abstract class Vertx extends Holder<StargateNode> {
    public abstract void initRuntimeClass(final String type, final String dagName, final PipelineOptions options);
    public abstract Class runtimeClass();

}
