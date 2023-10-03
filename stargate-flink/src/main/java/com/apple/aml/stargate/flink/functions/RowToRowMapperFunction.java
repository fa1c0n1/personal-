package com.apple.aml.stargate.flink.functions;

import com.apple.aml.stargate.flink.utils.FlinkUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * Converts Generic SQL Row from SQL Source into Encriched Row with Schema
 */
public class RowToRowMapperFunction extends RichMapFunction<Row, Row> implements Serializable {

    public String schemaString;
    public Schema schema = null;
    public DataType dataType = null;
    public RowType rowType;


    public RowToRowMapperFunction(String schemaString){
        this.schemaString = schemaString;
        this.dataType = AvroSchemaConverter.convertToDataType(schemaString);
        this.rowType= (RowType) this.dataType.getLogicalType();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        schema = new Schema.Parser().parse(schemaString);
        super.open(parameters);
    }

    @Override
    public Row map(Row row) throws Exception {
        GenericRecord avroRecord = new GenericData.Record(schema);
        for (int i = 0; i < schema.getFields().size(); i++) {
            Schema.Field field = schema.getFields().get(i);
            Object fieldValue = row.getField(FlinkUtils.getFieldIndex(row, field.name()));
            Object avroFieldValue = FlinkUtils.convertToAvro(fieldValue, field.schema());
            avroRecord.put(field.name(), avroFieldValue);
        }

        AvroToRowDataConverters.AvroToRowDataConverter rowDataConverter = AvroToRowDataConverters.createRowConverter(rowType);
        RowData outRowData = (RowData) rowDataConverter.convert(avroRecord);
        RowRowConverter converter = RowRowConverter.create(dataType);
        Row transformedRow = converter.toExternal(outRowData);

        return transformedRow;
    }
}
