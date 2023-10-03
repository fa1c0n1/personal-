package com.apple.aml.stargate.flink.functions;

import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * Maps SQL source table {@link Row} object into Transformed {@link Row} with
 */
public class AvroToRowMapperFunction extends RichMapFunction<Tuple2<String,GenericRecord>, Row> implements Serializable {

    public String schemaString;
    public DataType dataType = null;
    public RowType rowType;
    public AvroToRowMapperFunction(String schemaString) {
        this.schemaString = schemaString;
        this.dataType = AvroSchemaConverter.convertToDataType(schemaString);
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        LogicalType flinkType = AvroSchemaConverter.convertToDataType(schemaString).getLogicalType();
        rowType = (RowType) flinkType;
        super.open(parameters);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Row map(Tuple2<String,GenericRecord> data) throws Exception {
        AvroToRowDataConverters.AvroToRowDataConverter rowDataConverter = AvroToRowDataConverters.createRowConverter(rowType);
        RowData outRowData = (RowData) rowDataConverter.convert(data.f1);
        RowRowConverter converter = RowRowConverter.create(dataType);
        Row transformedRow = converter.toExternal(outRowData);
        return transformedRow;
    }

}
