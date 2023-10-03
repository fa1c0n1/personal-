package com.apple.aml.stargate.flink;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Random;

public class MainClass {
    public static void main(String[] args) throws Exception {
        // Initialize Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define Avro Schema
//        String userSchema = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";

        // Create a DataStream with random generator source
        DataStream<Tuple2<String, GenericRecord>> randomStream = env
                .fromElements(1, 2, 3, 4, 5)
                .map(new MyTuple2Mapper());

        // Print the DataStream
        randomStream.print();

        // Execute the Flink job
//        env.execute("Random Avro Source");
    }
}



