package com.apple.aml.stargate.flink.source;

import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.flink.streaming.api.EnrichedDataStream;

import java.io.Serializable;


public interface FlinkSourceI extends Serializable {
    public EnrichedDataStream read(StargateNode node,EnrichedDataStream enrichedDataStream) throws Exception;

}
