package com.apple.aml.stargate.flink.sink;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.IcebergOptions;
import com.apple.aml.stargate.flink.sink.functions.GenerateDynamicIcebergSink;
import com.apple.aml.stargate.flink.streaming.api.EnrichedDataStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.AvroGenericRecordToRowDataMapper;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class IcebergSink implements FlinkSinkI {

    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    transient Configuration hadoopConfiguration;

    @Override
    public void write(StargateNode node, EnrichedDataStream enrichedDataStream) throws Exception {
        IcebergOptions options = (IcebergOptions) node.getConfig();
        writeToStaticOrDynamicSink(enrichedDataStream, options);
    }


    private void writeToStaticOrDynamicSink(EnrichedDataStream stream, IcebergOptions options) {
        if (options.isDynamic()) {
            createDynamicSink(stream, options);
        } else {
            createStaticSink(stream, options);
        }
    }

    /**
     * The createStaticSink function is used to create a static sink for the Iceberg table.
     * The function takes in an EnrichedDataStream and IcebergOptions as parameters.
     * It then creates a DataStream of GenericRecords from the EnrichedDataStream, which is then passed into the buildTableLoader function along with the IcebergOptions object.
     * The TableLoader object returned by buildTableLoader is used to load a Table object, which has its schema converted into RowType using FlinkSchemaUtil's convert method.

     *
     * @param EnrichedDataStream stream Get the schema of the stream
     * @param IcebergOptions options Get the table name, flink parallelism and iceberg configuration
     **
     */
    private void createStaticSink(EnrichedDataStream stream, IcebergOptions options) {
        DataStream<GenericRecord> sinkStream = getGenericRecordStream(stream);
        String schemaString = stream.getSchema().f0;
        org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schemaString);
        TableLoader tableLoader = buildTableLoader(avroSchema, options);
        int parallelism = options.getFlinkParallelism() > 0 ? options.getFlinkParallelism() :stream.getExecutionEnvironment().getParallelism();
        Table table = tableLoader.loadTable();
        RowType rowType = FlinkSchemaUtil.convert(table.schema());

        FlinkSink.builderFor(
                        sinkStream,
                        // The assumption is that the Avro GenericRecord has the same schema
                        // (field types and order) as the Iceberg table schema.
                        AvroGenericRecordToRowDataMapper.forAvroSchema(avroSchema),
                        FlinkCompatibilityUtil.toTypeInfo(rowType))
                .table(tableLoader.loadTable())
                .tableLoader(tableLoader)
                .writeParallelism(parallelism)
                .distributionMode(DistributionMode.valueOf(options.getDistributionMode().toUpperCase()))
                .uidPrefix(options.getTableName())
                .append();
    }

    /**
     * The createDynamicSink function is used to create a dynamic Iceberg sink.
     * The function takes in an EnrichedDataStream and IcebergOptions as parameters.
     * It then creates a DataStream of GenericRecords from the EnrichedDataStream, and parses the schema string into an Avro Schema object.
     * Next, it splits the splitKeyValues option by commas to get a list of sources that will be written to different tables in Iceberg.
     * Then, it iterates through each source in this list and adds them as OutputTags for use with Flink's SideOutputs feature later on.  This allows
     *
     * @param EnrichedDataStream stream Get the schema of the stream
     * @param IcebergOptions options Set the table name, split key and split key values
     */
    public void createDynamicSink(EnrichedDataStream stream, IcebergOptions options) {
        LOGGER.info("Creating Dynamic Iceberg Sink");
        DataStream<GenericRecord> sinkStream = getGenericRecordStream(stream);
        String schemaString = stream.getSchema().f0;
        org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schemaString);

        List<String> sourceList = Arrays.asList(options.getSplitKeyValues().split(","));
        Map<String, OutputTag<GenericRecord>> tagsMap = new HashMap<>();
        for (String source : sourceList) {
            tagsMap.put(source, new OutputTag<GenericRecord>(source) {});
        }

        SingleOutputStreamOperator<GenericRecord> splitStream = sinkStream.process(new GenerateDynamicIcebergSink(tagsMap, options.getSplitKey()));

        tagsMap.forEach((source, tag) -> {
            DataStream<GenericRecord> outputStream = splitStream.getSideOutput(tag);
            LOGGER.info(String.format("Writing split stream %s", tag.getId()));
            options.setTableName(source);
            TableLoader tableLoader = buildTableLoader(avroSchema, options);
            int parallelism = options.getFlinkParallelism() > 0 ? options.getFlinkParallelism() :stream.getExecutionEnvironment().getParallelism();
            Table table = tableLoader.loadTable();
            RowType rowType = FlinkSchemaUtil.convert(table.schema());

            FlinkSink.builderFor(
                            outputStream,
                            // The assumption is that the Avro GenericRecord has the same schema
                            // (field types and order) as the Iceberg table schema.
                            AvroGenericRecordToRowDataMapper.forAvroSchema(avroSchema),
                            FlinkCompatibilityUtil.toTypeInfo(rowType))
                    .table(tableLoader.loadTable())
                    .tableLoader(tableLoader)
                    .writeParallelism(parallelism)
                    .distributionMode(DistributionMode.valueOf(options.getDistributionMode().toUpperCase()))
                    .uidPrefix(options.getTableName())
                    .append();
        });
    }


    private DataStream<GenericRecord> getGenericRecordStream(EnrichedDataStream stream) {
        return stream.getDataStream().map((MapFunction<Tuple2<String, GenericRecord>, GenericRecord>) value -> value.f1);
    }

    public Catalog icebergCatalog(final PipelineConstants.CATALOG_TYPE catalogType, final Map<String, Object> options, final Configuration conf) {
        return CatalogUtil.buildIcebergCatalog(catalogType.name(), (Map<String, String>) (Map) options, conf);
    }

    /**
     * The buildTableLoader function is used to create a TableLoader object that can be used to load data into an Iceberg table.
     * The function takes in the Avro schema of the data being loaded and options for how to configure Iceberg.
     * It then creates or updates an Iceberg table based on these parameters, and returns a TableLoader object that can be used
     * by Spark jobs to load data into this table.  This function should only need to be called once per job, as it will create/update
     * the underlying iceberg tables if they do not exist or have changed since last time this was run (e.g.,
     *
     * @param org.apache.avro.Schema schema Build the iceberg table schema
     * @param IcebergOptions options Get the catalog type, catalog properties and hadoop configurations
     *
     * @return A tableloader object
     **/
    public TableLoader buildTableLoader(org.apache.avro.Schema schema, IcebergOptions options) {
        Catalog icebergCatalog = icebergCatalog(options.catalogType(), options.getCatalogProperties(), getHadoopConfigurations(options));
        Schema tableSchema = buildIcebergTableSchema(schema);
        PartitionSpec icebergPartitionSpec = buildIcebergPartitionSpec(options, tableSchema);
        createOrUpdateIcebergTable(icebergCatalog, options, tableSchema, icebergPartitionSpec);
        return loadTable(options);
    }

    /**
     * The getHadoopConfigurations function is used to set the Hadoop configuration properties.
     *
     *
     * @param IcebergOptions options Get the catalog properties
     *
     * @return A configuration object with the catalog properties
     **/
    public Configuration getHadoopConfigurations(IcebergOptions options){
        hadoopConfiguration = new Configuration();
        options
                .getCatalogProperties()
                .forEach((key, value) -> hadoopConfiguration.set(key, (String) value));
        return hadoopConfiguration;

    }

    /**
     * The buildIcebergPartitionSpec function builds a PartitionSpec object from the partition column names
     * provided in the IcebergOptions. The PartitionSpec is used to define how data is partitioned when it's written
     * to an Iceberg table. In this case, we're using identity partitions, which means that each value of a given
     * column will be stored in its own directory on disk. For example, if you have two columns: `year` and `month`,
     * then all rows with year=2020 and month=01 will be stored together in one directory on disk (e.g., /year=2020/month
     *
     * @param IcebergOptions options Get the partition column from the command line
     * @param Schema tableSchema Create the partitionspec
     *
     * @return A partitionspec object
     *
     */
    private PartitionSpec buildIcebergPartitionSpec(IcebergOptions options, Schema tableSchema) {
        PartitionSpec.Builder builder = PartitionSpec.builderFor(tableSchema);
        List<String> partitionColumns = Arrays.asList(options.getPartitionColumn().split(","));

        for (String partitionColumn : partitionColumns) {
            builder.identity(partitionColumn);
        }
        return builder.build();
    }

    private Schema buildIcebergTableSchema(org.apache.avro.Schema schema) {
        return AvroSchemaUtil.toIceberg(schema);
    }

    /**
     * The createOrUpdateIcebergTable function creates a new Iceberg table if it does not exist, or updates an existing
     * Iceberg table with the specified schema and partition spec.
     *
     *
     * @param Catalog icebergCatalog Create a namespace and table
     * @param IcebergOptions options Get the database and table name
     * @param Schema tableSchema Define the table schema
     * @param PartitionSpec icebergPartitionSpec Create the partitioning scheme for the table
     *
     * @return A table
     **/
    private void createOrUpdateIcebergTable(Catalog icebergCatalog, IcebergOptions options, Schema tableSchema, PartitionSpec icebergPartitionSpec) {
        if(!((SupportsNamespaces) icebergCatalog).namespaceExists(Namespace.of(options.getDbName()))){
            ((SupportsNamespaces) icebergCatalog).createNamespace(Namespace.of(options.getDbName()));
            LOGGER.info("Creating Iceberg Namespace ", options.getDbName());
        }

        if(!icebergCatalog.tableExists(TableIdentifier.of(options.getDbName(), options.getTableName()))){
            icebergCatalog.createTable(TableIdentifier.of(options.getDbName(), options.getTableName()), tableSchema, icebergPartitionSpec);
            LOGGER.info("Creating Iceberg Table ", options.getTableName());
        }
        }


    /**
     * The loadTable function is used to load the table from the catalog.
     *
     *
     * @param IcebergOptions options Get the catalog type, database name, table name and properties
     *
     * @return A tableloader object
     *
     */
    private TableLoader loadTable(IcebergOptions options) {

        CatalogLoader catalogLoader = getCatalogLoader(options.getCatalogType(), options.getCatalogType(), hadoopConfiguration, options.getCatalogProperties());
        TableLoader tl = TableLoader.fromCatalog(catalogLoader, TableIdentifier.of(options.getDbName(), options.getTableName()));
        // Open the catalog table loader.
        if (tl instanceof TableLoader.CatalogTableLoader) {
            LOGGER.info("Table Loader open");
            tl.open();
        }
        return tl;
    }


    /**
     * The getCatalogLoader function is used to create a CatalogLoader object.
     * The CatalogLoader class is an abstract class that provides the interface for loading catalogs.
     * It has several subclasses, each of which implements a different way of loading catalogs:
     * - HadoopCatalogLoader loads from HDFS or S3 using the Hadoop FileSystem API;
     * - HiveCatalogLoader loads from Hive metastore; and,
     * - RestCatalogLoader loads from REST APIs (e.g., AWS Glue).
     *
     * @param String catalogType Determine which catalogloader to return
     * @param String catalogName Specify the name of the catalog
     * @param Configuration hadoopConf Pass in the hadoop configuration
     * @param Map&lt;String Pass in the properties of the catalog
     * @param Object&gt; props Pass in the properties of the catalog
     *
     * @return A catalogloader object
     *
     */
    public CatalogLoader getCatalogLoader(String catalogType, String catalogName, Configuration hadoopConf, Map<String, Object> props) {
        CatalogLoader catalogLoader = null;

        Map<String, String> catalogProps = props.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> String.valueOf(e.getValue())));

        switch (catalogType.toLowerCase()) {
            case "hadoop":
                catalogLoader = CatalogLoader.hadoop(catalogName, hadoopConf, catalogProps);
                break;
            case "hive":
                catalogLoader = CatalogLoader.hive(catalogName, hadoopConf, catalogProps);
                break;
            case "rest":
                catalogLoader = CatalogLoader.rest(catalogName, hadoopConf, catalogProps);
                break;
            case "custom":
                String customImpl = catalogProps.getOrDefault("customImpl", "");
                catalogLoader = CatalogLoader.custom(catalogName, catalogProps, hadoopConf, customImpl);
                break;
            default:
                throw new IllegalArgumentException("Invalid catalog type: " + catalogType);
        }

        return catalogLoader;
    }

}
