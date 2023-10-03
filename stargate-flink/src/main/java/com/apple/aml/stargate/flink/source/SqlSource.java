package com.apple.aml.stargate.flink.source;

import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.FlinkSqlOptions;
import com.apple.aml.stargate.flink.functions.RowToAvroMapperFunction;
import com.apple.aml.stargate.flink.streaming.api.EnrichedDataStream;
import com.apple.aml.stargate.flink.functions.RowToRowMapperFunction;
import com.apple.aml.stargate.flink.utils.FlinkUtils;
import com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SqlSource implements FlinkSourceI {
    public static Map<String, String> configs = new HashMap<>();
    public static String sql = null;


    @SuppressWarnings("unchecked")
    public static void initRead(StargateNode node) {
        FlinkSqlOptions options = (FlinkSqlOptions) node.getConfig();
        Set<Map.Entry<String, byte[]>> entrySet = node.configFiles().entrySet();
        for (Map.Entry<String, byte[]> entry : entrySet) {
            String fileName = entry.getKey();
            byte[] content = entry.getValue();
            if (fileName.endsWith(".properties")) {
                FlinkUtils.convertPropertiesToMap(content, configs);
            } else if (fileName.endsWith(".jks") || fileName.endsWith(".pem")) {
                FlinkUtils.writeToFileSystem(options.getFileBasePath(), fileName, content);
            }
        }
        sql = options.getSql();
        sql = FlinkUtils.processSQLWithPlaceholders(sql, configs);
    }


    @Override
    public EnrichedDataStream read(StargateNode node, EnrichedDataStream enrichedDataStream) throws Exception {
        FlinkSqlOptions options = (FlinkSqlOptions) node.getConfig();
        StreamTableEnvironment streamTableEnvironment = enrichedDataStream.getTableEnvironment();
        streamTableEnvironment.executeSql(sql).await();

        Table table = streamTableEnvironment.from(options.getTableName() != null ? options.getTableName() : "default_catalog.default_database."+FlinkUtils.getTableName(options.getSchemaId()));
        String schemaString = PipelineUtils.fetchSchemaWithLocalFallback(node.getSchemaReference(), options.getSchemaId()).toString();
        return new EnrichedDataStream(enrichedDataStream.getExecutionEnvironment(), streamTableEnvironment.toDataStream(table).map(new RowToAvroMapperFunction(schemaString,options)), Tuple2.of(options.getSchemaId(),schemaString));
    }
}