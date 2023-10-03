package com.apple.aml.stargate.flink.sink.functions;

import com.apple.aml.stargate.common.nodes.StargateNode;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.*;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class LoggerSinkFunction extends RichSinkFunction<Tuple2<String,GenericRecord>> {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private String nodeName;

    private String nodeType;

    public LoggerSinkFunction(StargateNode node) {
        nodeName = node.getName();
        nodeType = node.getType();
    }


    @Override
    public void invoke(Tuple2<String,GenericRecord> row, Context context) throws Exception {
        super.invoke(row, context);
        Tuple2<String, GenericRecord> input = (Tuple2<String, GenericRecord>) row;
        GenericRecord record = input.f1;
        String schemaId = record == null ? UNKNOWN : record.getSchema().getFullName();
        LOGGER.info(EMPTY_STRING, Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, schemaId, "key", String.valueOf(input.f0), "record", record == null ? EMPTY_STRING : record));
        System.out.println(record.toString());
    }
}
