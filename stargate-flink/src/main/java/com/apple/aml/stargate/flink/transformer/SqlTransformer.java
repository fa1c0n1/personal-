package com.apple.aml.stargate.flink.transformer;

import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.FlinkSqlOptions;
import com.apple.aml.stargate.common.utils.SchemaUtils;
import com.apple.aml.stargate.flink.functions.AvroToRowMapperFunction;
import com.apple.aml.stargate.flink.functions.RowToAvroMapperFunction;
import com.apple.aml.stargate.flink.functions.RowToRowMapperFunction;
import com.apple.aml.stargate.flink.streaming.api.EnrichedDataStream;
import com.apple.aml.stargate.flink.utils.FlinkUtils;
import com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import org.apache.avro.Schema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;

import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class SqlTransformer implements FlinkTransformerI {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());


    public static void initTransform(StargateNode node) {

    }

    @Override
    public EnrichedDataStream transform(StargateNode node,EnrichedDataStream enrichedDataStream) throws Exception {
        StreamTableEnvironment streamTableEnvironment = enrichedDataStream.getTableEnvironment();
        String finalSchema = null;
        String finalSchemaId=null;
        if(enrichedDataStream.getSchema()!=null){
            finalSchema = enrichedDataStream.getSchema().f1;
            finalSchemaId=enrichedDataStream.getSchema().f0;
        }
        FlinkSqlOptions options = (FlinkSqlOptions) node.getConfig();
        String transformationSQL = options.getSql();

        if(finalSchema==null){
            Schema schema = fetchSchemaWithLocalFallback(node.getSchemaReference(), options.getSchemaId());
            finalSchema=schema.toString();
            finalSchemaId=options.getSchemaId();
        }
        //convert stream from GenericRecord to Row to perform SQL queries
        SingleOutputStreamOperator<Row> sqlStream = enrichedDataStream.getDataStream().map(new AvroToRowMapperFunction(finalSchema));
        TypeInformation<Row> rowTypeInfo = AvroSchemaConverter.convertToTypeInfo(finalSchema);
        sqlStream.returns(rowTypeInfo);


        String tableName =   options.getTableName() != null ? options.getTableName() : finalSchemaId!=null?"default_catalog.default_database."+FlinkUtils.getTableName(finalSchemaId):"default_catalog.default_database."+node.getName();

        streamTableEnvironment.createTemporaryView(tableName, sqlStream);
        //execute the sql.
        Table tempTable = streamTableEnvironment.sqlQuery(transformationSQL);
        DataStream<Row> outputStream = null;

        if (options.getSchemaId() != null) {
            finalSchema = PipelineUtils.fetchSchemaWithLocalFallback(node.getSchemaReference(), options.getSchemaId()).toString();
            finalSchemaId= options.getSchemaId();
        }
        outputStream = streamTableEnvironment.toDataStream(tempTable);
        enrichedDataStream = new EnrichedDataStream(enrichedDataStream.getExecutionEnvironment(), outputStream.map(new RowToAvroMapperFunction(finalSchema, options)), Tuple2.of(finalSchemaId,finalSchema));
        return enrichedDataStream;
    }
}