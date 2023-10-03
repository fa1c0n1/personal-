package com.apple.aml.stargate.flink.sink;

import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.FlinkSqlOptions;
import com.apple.aml.stargate.flink.functions.AvroToRowMapperFunction;
import com.apple.aml.stargate.flink.streaming.api.EnrichedDataStream;
import com.apple.aml.stargate.flink.utils.FlinkUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SqlSink implements FlinkSinkI {

    public static Map<String, String> configs = new HashMap<>();
    public static String sql = null;

    @SuppressWarnings("unchecked")
    public static void initWrite(StargateNode node) {
        {
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
    }

    @Override
    public void write(StargateNode node, EnrichedDataStream enrichedDataStream) throws Exception {
        FlinkSqlOptions options = (FlinkSqlOptions) node.getConfig();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(enrichedDataStream.getExecutionEnvironment());
        streamTableEnvironment.executeSql(sql).await();

        TypeInformation<Row> rowTypeInfo = AvroSchemaConverter.convertToTypeInfo(enrichedDataStream.getSchema().f1);
        String tempTableName="default_catalog.default_database."+node.getAlias()+"_table";
        streamTableEnvironment.createTemporaryView(tempTableName,  enrichedDataStream.getDataStream().map(new AvroToRowMapperFunction(enrichedDataStream.getSchema().f1)).returns(rowTypeInfo));
        String tableName =   options.getTableName() != null ? "default_catalog.default_database."+options.getTableName() : "default_catalog.default_database."+FlinkUtils.getTableName(options.getSchemaId());
        streamTableEnvironment.executeSql("INSERT INTO " + tableName + " SELECT * FROM "+tempTableName);
        enrichedDataStream.getDataStream().print();
    }
}
