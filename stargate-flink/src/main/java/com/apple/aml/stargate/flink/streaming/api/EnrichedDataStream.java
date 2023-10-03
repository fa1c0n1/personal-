package com.apple.aml.stargate.flink.streaming.api;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.Serializable;


public class EnrichedDataStream implements Serializable {

    private StreamExecutionEnvironment executionEnvironment;
    private StreamTableEnvironment tableEnvironment;
    private DataStream<Tuple2<String, GenericRecord>> dataStream;


    private Tuple2<String,String> schema;


    public EnrichedDataStream(StreamExecutionEnvironment executionEnvironment, DataStream<Tuple2<String, GenericRecord>> dataStream) {
        this.dataStream = dataStream;
        this.executionEnvironment=executionEnvironment;
        this.tableEnvironment = StreamTableEnvironment.create(this.executionEnvironment);
    }

    public EnrichedDataStream(StreamExecutionEnvironment executionEnvironment, DataStream<Tuple2<String, GenericRecord>> dataStream,Tuple2<String,String> schema) {
        this.dataStream = dataStream;
        this.executionEnvironment=executionEnvironment;
        this.tableEnvironment = StreamTableEnvironment.create(this.executionEnvironment);
        this.schema=schema;
    }


    public StreamExecutionEnvironment getExecutionEnvironment() {
        return executionEnvironment;
    }

    public StreamTableEnvironment getTableEnvironment() {
        return tableEnvironment;
    }

    public DataStream<Tuple2<String, GenericRecord>> getDataStream() {
        return dataStream;
    }
    
    public Tuple2<String, String> getSchema() {
        return schema;
    }


}