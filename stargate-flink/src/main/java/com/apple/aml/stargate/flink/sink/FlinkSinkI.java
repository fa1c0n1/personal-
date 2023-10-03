package com.apple.aml.stargate.flink.sink;

import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.flink.streaming.api.EnrichedDataStream;

import java.io.Serializable;

public interface FlinkSinkI extends Serializable {
    public void write(final StargateNode node, EnrichedDataStream enrichedDataStream) throws Exception;
}
