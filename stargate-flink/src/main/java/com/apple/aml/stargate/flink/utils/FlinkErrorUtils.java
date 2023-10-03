package com.apple.aml.stargate.flink.utils;

import com.apple.aml.stargate.common.pojo.ErrorRecord;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

public class FlinkErrorUtils {

    @SuppressWarnings("unchecked")
    public static final OutputTag<Tuple2<String,ErrorRecord>> errorTag = new OutputTag<Tuple2<String,ErrorRecord>>("error-side-output", getSGErrorTypeInfo()) {
    };
    public static TupleTypeInfo getSGErrorTypeInfo() {
        TupleTypeInfo<Tuple2<String, ErrorRecord>> tupleType = new TupleTypeInfo<>(
                Types.STRING,
                TypeInformation.of(ErrorRecord.class)
        );
        return tupleType;
    }

    public static DataStream<Tuple2<String,ErrorRecord>> getErrorStream(DataStream<Tuple2<String,GenericRecord>> dataStream) {
        SingleOutputStreamOperator<Tuple2<String,GenericRecord>> operator = null;
        if (dataStream instanceof SingleOutputStreamOperator) {
            operator = (SingleOutputStreamOperator<Tuple2<String,GenericRecord>>) dataStream;

            if(operator.getSideOutput(errorTag)!=null){
                return (DataStream<Tuple2<String,ErrorRecord>>)operator.getSideOutput(errorTag);
            }
        }
        return null;
    }
}
