package com.apple.aml.stargate.flink;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Random;


public  class MyTuple2Mapper implements MapFunction<Integer, Tuple2<String, GenericRecord>> {
    @Override
    public Tuple2<String, GenericRecord> map(Integer i) {
        Random rand = new Random();
        String userSchema = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
        Schema schema = new Schema.Parser().parse(userSchema);
        GenericRecord user = new GenericData.Record(schema);
        user.put("name", "user" + rand.nextInt(5));
        user.put("age", rand.nextInt(100));
        return Tuple2.of(userSchema, user);
    }
}
