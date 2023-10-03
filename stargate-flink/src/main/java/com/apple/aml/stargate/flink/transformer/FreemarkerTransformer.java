package com.apple.aml.stargate.flink.transformer;

import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.FlinkSqlOptions;
import com.apple.aml.stargate.common.options.FreemarkerOptions;
import com.apple.aml.stargate.flink.streaming.api.EnrichedDataStream;
import com.apple.aml.stargate.flink.functions.FreemarkerMapFunction;
import com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;

import static com.apple.aml.stargate.common.utils.LogUtils.logger;


public class FreemarkerTransformer implements FlinkTransformerI {

    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());


    @Override
    public EnrichedDataStream transform(StargateNode node, EnrichedDataStream enrichedDataStream) throws Exception {
        SingleOutputStreamOperator<Tuple2<String,GenericRecord>> transformedStream = enrichedDataStream.getDataStream().process(new FreemarkerMapFunction(node));
        FreemarkerOptions options = (FreemarkerOptions) node.getConfig();
        Tuple2<String,String> schema = Tuple2.of(options.getSchemaId(),PipelineUtils.fetchSchemaWithLocalFallback(node.getSchemaReference(),options.getSchemaId()).toString());
        return new EnrichedDataStream(enrichedDataStream.getExecutionEnvironment(), transformedStream,schema );
    }



}
