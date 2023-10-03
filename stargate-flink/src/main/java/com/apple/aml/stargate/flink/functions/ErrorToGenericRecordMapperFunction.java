package com.apple.aml.stargate.flink.functions;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.pojo.ErrorRecord;
import com.apple.aml.stargate.flink.utils.FlinkUtils;
import com.apple.aml.stargate.pipeline.sdk.ts.BaseErrorPayloadConverter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class ErrorToGenericRecordMapperFunction extends RichFlatMapFunction<KV<String, ErrorRecord>, Tuple2<String, GenericRecord>> {

    private final String nodeName;
    private final PipelineConstants.ENVIRONMENT environment;

    private BaseErrorPayloadConverter baseErrorPayloadConverter;

    public ErrorToGenericRecordMapperFunction(final String errorNodeName, final PipelineConstants.ENVIRONMENT environment) {
        this.nodeName = errorNodeName;
        this.environment = environment;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        baseErrorPayloadConverter= new BaseErrorPayloadConverter(nodeName,environment);
    }

    @Override
    public void flatMap(KV<String, ErrorRecord> value, Collector<Tuple2<String, GenericRecord>> out) throws Exception {
        out.collect(FlinkUtils.convertGenericRecordToTuple2(baseErrorPayloadConverter.convert(value)));
    }
}
