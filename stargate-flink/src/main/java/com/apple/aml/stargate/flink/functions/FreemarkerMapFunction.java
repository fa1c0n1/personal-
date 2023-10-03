package com.apple.aml.stargate.flink.functions;

import com.apple.aml.stargate.beam.sdk.utils.BeamUtils;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.flink.utils.FlinkErrorUtils;
import com.apple.aml.stargate.flink.utils.FlinkUtils;
import com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class FreemarkerMapFunction extends ProcessFunction<Tuple2<String, GenericRecord>, Tuple2<String, GenericRecord>> {

    private BaseFreemarkerEvaluator baseFreemarkerEvaluator;

    private StargateNode node;

    public FreemarkerMapFunction(StargateNode node){
        this.node=node;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
        baseFreemarkerEvaluator = new BaseFreemarkerEvaluator(FlinkUtils.nodeService(), FlinkUtils.errorService());
        baseFreemarkerEvaluator.initFreemarkerNode(node, true);
    }

    @Override
    public void processElement(Tuple2<String, GenericRecord> value, ProcessFunction<Tuple2<String, GenericRecord>, Tuple2<String, GenericRecord>>.Context ctx, Collector<Tuple2<String, GenericRecord>> out) throws Exception {
        baseFreemarkerEvaluator.processElement(FlinkUtils.convertToKV(value), null, skv -> {
            out.collect(FlinkUtils.convertGenericRecordToTuple2(skv));
        }, ekv -> ctx.output(FlinkErrorUtils.errorTag, FlinkUtils.convertErrorRecordToTuple2(ekv)));
    }
}
