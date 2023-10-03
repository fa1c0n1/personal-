package com.apple.aml.stargate.connector.iceberg;

import com.apple.aml.stargate.beam.sdk.io.hadoop.HdfsIO;
import com.apple.aml.stargate.beam.sdk.io.s3.S3IO;
import com.apple.aml.stargate.beam.sdk.transforms.CollectionFns;
import com.apple.aml.stargate.beam.sdk.utils.WindowFns;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.constants.PipelineConstants.NODE_TYPE;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.IcebergOptions;
import com.apple.aml.stargate.common.options.S3Options;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static com.apple.aml.stargate.beam.sdk.io.s3.S3IO.setupS3Options;
import static com.apple.aml.stargate.beam.sdk.utils.FileWriterFns.compressionCodec;
import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.applyWindow;
import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.batchPartitionKey;
import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.batchWriter;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.GENERIC_RECORD_GROUP_TAG;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.ClassUtils.duplicate;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.getInternalSchema;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static org.apache.iceberg.TableProperties.OBJECT_STORE_ENABLED;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.WRITE_DATA_LOCATION;

public class IcebergIO extends HdfsIO {
    public static final TupleTag<KV<String, DataFile>> KV_STRING_VS_DATAFILE = new TupleTag<>() {
    };
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    @SuppressWarnings("unchecked")
    public static void init(final PipelineOptions pipelineOptions, final StargateNode node) throws Exception {
        IcebergOptions options = (IcebergOptions) node.getConfig();
        if (options == null) throw new GenericException("Missing Iceberg configuration", Map.of("nodeName", node.getName(), "nodeType", node.getType()));
        if (isBlank(options.getDbName())) {
            throw new GenericException("Iceberg DB Name (dbName) attribute is missing", Map.of("nodeName", node.getName(), "nodeType", node.getType()));
        }
        if (isBlank(options.getStorageType())) {
            options.setStorageType(node.getType().toLowerCase().contains("s3") ? "S3" : "Hdfs");
        }
        options.setAutoUpdateTable(true);
        if (options.isPassThrough()) options.setCommitInPlace(true);
        Map<String, Object> tableProperties = options.getTableProperties();
        if (tableProperties == null) {
            tableProperties = new HashMap<>();
            options.setTableProperties(tableProperties);
        }

        if (options.getSparkProperties() == null) {
            options.setSparkProperties(new HashMap<>());
        }
        final Map<String, Object> sparkProperties = options.getSparkProperties();
        PipelineConstants.CATALOG_TYPE catalogType = options.catalogType();
        final Map<String, Object> catalogProperties = new HashMap<>();
        if (catalogType.catalogImpl() != null) {
            catalogProperties.put(CatalogProperties.CATALOG_IMPL, catalogType.catalogImpl());
        }
        Map<String, Object> hiveProperties = options.getHiveProperties();
        if (catalogType == PipelineConstants.CATALOG_TYPE.hive) {
            if (hiveProperties == null) {
                hiveProperties = new HashMap<>();
                options.setHiveProperties(hiveProperties);
            }
            catalogProperties.remove(CatalogProperties.CATALOG_IMPL);
            sparkProperties.put("sql.catalog.spark_catalog.type", "hive");
        } else {
            if (isBlank(options.getDbPath())) {
                options.setDbPath(options.basePath() + "/db");
            }
            catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, options.getDbPath());
            if (catalogType == PipelineConstants.CATALOG_TYPE.dynamodb) {
                catalogProperties.put(CatalogProperties.LOCK_IMPL, "org.apache.iceberg.aws.glue.DynamoLockManager");
            }
        }
        if (node.getType().toLowerCase().contains("adt") || "adt".equalsIgnoreCase(options.getImplementation())) {
            tableProperties.put("spark.sql.sources.provider", "adt");
        } else {
            sparkProperties.put("sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
            if (catalogType == PipelineConstants.CATALOG_TYPE.hive) {
                tableProperties.put(TableProperties.ENGINE_HIVE_ENABLED, "true");
                hiveProperties.put(ConfigProperties.ENGINE_HIVE_ENABLED, "true");
            }
        }
        sparkProperties.put("sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
        sparkProperties.put("sql.sources.partitionOverwriteMode", "dynamic");
        NODE_TYPE storageType = options.storageType();
        if (storageType == NODE_TYPE.S3) {
            sparkProperties.put("sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog");
            catalogProperties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
            if (!isBlank(options.getEndpoint())) catalogProperties.put(S3FileIOProperties.ENDPOINT, options.endpoint());
            if (!isBlank(options.getAccessKey())) catalogProperties.put(S3FileIOProperties.ACCESS_KEY_ID, options.getAccessKey());
            if (!isBlank(options.getSecretKey())) catalogProperties.put(S3FileIOProperties.SECRET_ACCESS_KEY, options.getSecretKey());
            if (!isBlank(options.getSessionToken())) catalogProperties.put(S3FileIOProperties.SESSION_TOKEN, options.getSessionToken());
            if (options.isEnableObjectStorage()) {
                if (!tableProperties.containsKey(OBJECT_STORE_ENABLED)) tableProperties.put(OBJECT_STORE_ENABLED, "true");
                if (!tableProperties.containsKey(WRITE_DATA_LOCATION)) tableProperties.put(WRITE_DATA_LOCATION, options.basePath());
            }
        }
        if (!tableProperties.containsKey(PARQUET_COMPRESSION)) tableProperties.put(PARQUET_COMPRESSION, compressionCodec(options));
        if (options.getCatalogProperties() != null) {
            options.getCatalogProperties().forEach((k, v) -> catalogProperties.put(k, String.valueOf(v)));
        }
        options.setCatalogProperties(catalogProperties);
        catalogProperties.forEach((k, v) -> sparkProperties.put(String.format("sql.catalog.spark_catalog.%s", k), v));
        if (storageType == NODE_TYPE.S3) {
            S3IO.init(pipelineOptions, node);
        } else {
            HdfsIO.init(pipelineOptions, node);
        }
    }

    public static String defaultCommitPath(final String basePath) {
        return basePath + "/iceberg-commit";
    }

    @SuppressWarnings("unchecked")
    public static Catalog icebergCatalog(final PipelineConstants.CATALOG_TYPE catalogType, final Map<String, Object> options, final Configuration conf) {
        return CatalogUtil.buildIcebergCatalog(catalogType.name(), (Map<String, String>) (Map) options, conf);
    }

    public static Schema commitFileSchema(final PipelineConstants.ENVIRONMENT environment) {
        Schema.Parser parser = new Schema.Parser();
        String schemaString = commitFileSchemaString().replaceAll("\\#\\{ENV\\}", environment.name().toLowerCase());
        return parser.parse(schemaString);
    }

    private static String commitFileSchemaString() {
        try {
            String schemaString = IOUtils.resourceToString("/icebergcommit.avsc", Charset.defaultCharset());
            if (schemaString == null || schemaString.isBlank()) {
                throw new Exception("Could not load schemaString");
            }
            return schemaString;
        } catch (Exception e) {
            return "{\"type\":\"record\",\"name\":\"IcebergCommit\",\"namespace\":\"com.apple.aml.stargate.#{ENV}.internal\",\"fields\":[{\"name\":\"dbName\",\"type\":\"string\"},{\"name\":\"tableName\",\"type\":\"string\"},{\"name\":\"dataFile\",\"type\":\"string\"}]}";
        }
    }

    public void initRead(final Pipeline pipeline, final StargateNode node) throws Exception {
        super.initRead(pipeline, node);
    }

    @SuppressWarnings("unchecked")
    public void initCommon(final Pipeline pipeline, final StargateNode node) throws Exception {
        IcebergOptions options = (IcebergOptions) node.getConfig();
        NODE_TYPE storageType = options.storageType();
        if (storageType == NODE_TYPE.S3) {
            setupS3Options(pipeline, (S3Options) node.getConfig());
            super.initCommon(pipeline, node);
        }
    }

    @Override
    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        applyCoderRegistrySettings(pipeline);
        IcebergOptions options = (IcebergOptions) node.getConfig();
        options.setCommitToDisk(false);
        return _transform(pipeline, node, collection, false);
    }

    public void initWrite(final Pipeline pipeline, final StargateNode node) throws Exception {
        super.initWrite(pipeline, node);
    }

    public void initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        super.initTransform(pipeline, node);
    }

    @Override
    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return _transform(pipeline, node, collection, true);
    }

    private void applyCoderRegistrySettings(final Pipeline pipeline) {
        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderProvider(CoderProviders.forCoder(new TypeDescriptor<DataFile>() {
        }, SerializableCoder.of(DataFile.class)));
    }

    @SuppressWarnings("unchecked")
    public SCollection<KV<String, GenericRecord>> _transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> inputCollection, final boolean emit) throws Exception {
        applyCoderRegistrySettings(pipeline);
        IcebergOptions options = (IcebergOptions) node.getConfig();
        PipelineConstants.ENVIRONMENT environment = node.environment();
        SCollection<KV<String, GenericRecord>> window = applyWindow(inputCollection, options, node.getName());
        if (options.isUseDataFileWriter()) {
            if (options.isCommitInPlace()) {
                return window.apply(node.name("write-commit"), new IcebergDataFileWriter(node.getName(), node.getType(), options), KV_STRING_VS_DATAFILE).apply(node.name("file-details"), new DataFileDetails(environment));
            } else {
                return window.apply(node.name("write"), new IcebergDataFileWriter(node.getName(), node.getType(), options), KV_STRING_VS_DATAFILE).list(node.name("commit"), Combine.globally(new IcebergDataFileCommitter(environment, options)).withoutDefaults()).apply(node.name("result"), new CollectionFns.IndividualRecords<>());
            }
        }
        if (options.getBatchSize() > 1 || !isBlank(options.getBatchBytes()) || (options.getBatchDuration() != null && options.getBatchDuration().getSeconds() > 0)) {
            LOGGER.debug("Optimized batching enabled. Will apply batching", Map.of("batchSize", options.getBatchSize(), "batchBytes", String.valueOf(options.getBatchBytes()), "batchDuration", options.batchDuration(), "partitions", options.getPartitions()));
            window = window.group(node.name("partition-key"), batchPartitionKey(options.getPartitions(), options.getPartitionStrategy()), GENERIC_RECORD_GROUP_TAG).apply(node.name("batch"), batchWriter(node.getName(), node.getType(), environment, options, new IcebergBaseWriter<>(node.getName(), node.getType(), environment, options, emit)));
        } else if (options.isShuffle()) {
            LOGGER.debug("Shuffle enabled. Will apply shuffle");
            window = window.list(node.name("gather-window"), new WindowFns.GatherResults<>()).apply(node.name("write"), new IcebergWriter<>(node.getName(), node.getType(), environment, options));
        } else {
            window = window.apply(node.name("write"), new IcebergWriter<>(node.getName(), node.getType(), environment, options));
        }
        if (options.isCommitInPlace()) return window;
        IcebergOptions commitOptions = duplicate(options, IcebergOptions.class);
        commitOptions.setBatchSize(options.getCommitBatchSize());
        commitOptions.setBatchDuration(options.getCommitBatchDuration() == null ? Duration.ofSeconds(30) : options.getCommitBatchDuration());
        commitOptions.setPartitions(options.getCommitPartitions());
        SCollection<KV<String, GenericRecord>> commitWindow = applyWindow(window, commitOptions, node.name("commit"));
        LOGGER.debug("Applying batch commit partitions", Map.of("partitions", commitOptions.getPartitions()));
        window = commitWindow.group(node.name("write-commit-key"), batchPartitionKey(commitOptions.getPartitions(), commitOptions.getPartitionStrategy()), GENERIC_RECORD_GROUP_TAG).apply(node.name("write-commit-batch"), batchWriter(node.name("commit"), node.getType(), environment, commitOptions, new IcebergCommitter<>(node.getName(), node.getType(), environment, commitOptions)));
        return window;
    }

    public SCollection<KV<String, GenericRecord>> read(final Pipeline pipeline, final StargateNode node) throws Exception {
        IcebergOptions options = (IcebergOptions) node.getConfig();
        return null;
    }

    public static class DataFileDetails extends DoFn<KV<String, DataFile>, KV<String, GenericRecord>> {
        private final PipelineConstants.ENVIRONMENT environment;
        private final Schema schema;
        private final ObjectToGenericRecordConverter converter;

        public DataFileDetails(final PipelineConstants.ENVIRONMENT environment) {
            this.environment = environment;
            this.schema = getInternalSchema(environment, "filewriter.avsc", "{\"type\": \"record\",\"name\": \"FileDetails\",\"namespace\": \"com.apple.aml.stargate.#{ENV}.internal\",\"fields\": [{\"name\": \"fileName\",\"type\": \"string\"}]}");
            this.converter = converter(this.schema);
        }

        @ProcessElement
        public void processElement(@Element final KV<String, DataFile> kv, final ProcessContext ctx) throws Exception {
            DataFile datafile = kv.getValue();
            if (datafile == null) {
                return;
            }
            ctx.output(KV.of(kv.getKey(), this.converter.convert(Map.of("fileName", datafile.path(), "recordCount", datafile.recordCount()))));
        }
    }
}
