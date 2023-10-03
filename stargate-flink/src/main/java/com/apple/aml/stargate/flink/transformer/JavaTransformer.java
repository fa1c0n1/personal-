package com.apple.aml.stargate.flink.transformer;

import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.JavaFunctionOptions;
import com.apple.aml.stargate.flink.functions.JavaMapFunction;
import com.apple.aml.stargate.flink.streaming.api.EnrichedDataStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class JavaTransformer implements FlinkTransformerI{

    @Override
    public EnrichedDataStream transform(StargateNode node, EnrichedDataStream enrichedDataStream) throws Exception {
        SingleOutputStreamOperator<Tuple2<String, GenericRecord>> transformedStream = enrichedDataStream.getDataStream().process(new JavaMapFunction(node,(JavaFunctionOptions) node.getConfig()));
        return new EnrichedDataStream(enrichedDataStream.getExecutionEnvironment(), transformedStream,null);
    }
}
