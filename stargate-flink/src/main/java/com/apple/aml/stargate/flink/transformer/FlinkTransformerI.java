package com.apple.aml.stargate.flink.transformer;

import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.flink.streaming.api.EnrichedDataStream;

import java.io.Serializable;

public interface FlinkTransformerI extends  Serializable {

    public EnrichedDataStream transform(StargateNode node, EnrichedDataStream enrichedDataStream) throws Exception;
}
