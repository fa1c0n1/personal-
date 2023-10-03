package com.apple.aml.stargate.flink.sink;

import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.flink.streaming.api.EnrichedDataStream;
import com.apple.aml.stargate.flink.sink.functions.LoggerSinkFunction;

public class LogSink implements FlinkSinkI {

    @Override
    public void write(StargateNode node, EnrichedDataStream enrichedDataStream) throws Exception {
       enrichedDataStream.getDataStream().addSink(new LoggerSinkFunction(node));
    }
}
