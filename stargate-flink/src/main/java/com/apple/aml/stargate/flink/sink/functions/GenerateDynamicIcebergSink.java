package com.apple.aml.stargate.flink.sink.functions;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

import java.util.Map;

public class GenerateDynamicIcebergSink extends ProcessFunction<GenericRecord, GenericRecord> {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private Map<String, OutputTag<GenericRecord>> sourceTags;
    private String fieldName;

    public GenerateDynamicIcebergSink(Map<String, OutputTag<GenericRecord>> tags, String fieldName) {
        this.sourceTags = tags;
        this.fieldName = fieldName;
    }

    @Override
    public void processElement(GenericRecord value, Context ctx, Collector<GenericRecord> out) throws Exception {
        String source = value.get(fieldName).toString();
        logger.info(String.format("Generating side output for source %s", source));

        if(sourceTags.containsKey(source)) {
            logger.info(String.format("Record with source %s sent to %s", source, sourceTags.get(source).getId()));
            ctx.output(sourceTags.get(source),value);
        }else{
            logger.info(String.format("Record with source %s sent to default", source));
            out.collect(value);
        }
    }

}