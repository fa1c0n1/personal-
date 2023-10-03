package com.apple.aml.stargate.flink.source;

import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.flink.streaming.api.EnrichedDataStream;

public class IcebergSource implements FlinkSourceI{
    @Override
    public EnrichedDataStream read(StargateNode node, EnrichedDataStream enrichedDataStream) throws Exception {
        return null;
    }
}
