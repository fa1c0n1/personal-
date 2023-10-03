package com.apple.aml.stargate.flink.functions;

import com.apple.aml.stargate.common.options.FlinkSqlOptions;
import com.apple.aml.stargate.flink.utils.FlinkUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * Maps SQL source table {@link Row} object into Transformed {@link Row} with
 */
public class RowToAvroMapperFunction extends RichMapFunction<Row, Tuple2<String,GenericRecord>> implements Serializable {

    public String schemaString;
    public Schema schema = null;
    public DataType dataType = null;
    public FlinkSqlOptions options= null;

    public RowToAvroMapperFunction(String schemaString, FlinkSqlOptions options) {
        this.schemaString = schemaString;
        this.dataType = AvroSchemaConverter.convertToDataType(schemaString);
        this.options=options;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        schema = new Schema.Parser().parse(schemaString);
        super.open(parameters);
    }

    @Override
    public Tuple2<String,GenericRecord> map(Row row) throws Exception {
        GenericRecord avroRecord = new GenericData.Record(schema);
        for (int i = 0; i < schema.getFields().size(); i++) {
            Schema.Field field = schema.getFields().get(i);

            Object fieldValue =  safeGetField(row,field.name());
            if(fieldValue!=null){
                Object avroFieldValue = FlinkUtils.convertToAvro(fieldValue, field.schema());
                avroRecord.put(field.name(), avroFieldValue);
            }
        }
        Tuple2<String, GenericRecord> output = new Tuple2<>();
        output.f0=FlinkUtils.getUniqueKey(row,options.getId()!=null?options.getId().split(","):new String[0]);
        output.f1=avroRecord;
        return output;
    }

    public static Object safeGetField(Row row, String fieldName) {
        try {
            return row.getField(fieldName);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

}
