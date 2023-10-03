package com.apple.aml.stargate.connector.iceberg;

import com.apple.aml.stargate.beam.sdk.formatters.GenericRecordToPartitionText;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.options.IcebergOpsOptions;
import com.apple.aml.stargate.common.options.SchemaLevelOptions;
import com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.beam.sdk.formatters.GenericRecordToPartitionText.partitioner;
import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.DB_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.TABLE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.COMMIT_SUCCESS;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.PipelineConstants.DEFAULT_MAPPING;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.connector.iceberg.IcebergIO.defaultCommitPath;
import static com.apple.aml.stargate.connector.iceberg.IcebergIO.icebergCatalog;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.hiveConfiguration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.isDynamicKerberosLoginEnabled;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.performKerberosLogin;

public class IcebergTableCommitter extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> implements Serializable {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final String nodeName;
    private final String nodeType;
    private final Level logPayloadLevel;
    private final Map<String, SchemaLevelOptions> mappings;
    private final SchemaLevelOptions defaultOptions;
    private final boolean deleteCommitPathOnSuccess;
    private final PipelineConstants.CATALOG_TYPE catalogType;
    private final Map<String, Object> catalogProperties;
    private final boolean kerberosLoginEnabled;
    private final ConcurrentHashMap<String, GenericRecordToPartitionText> partitionerMap;
    private Catalog catalog;

    public IcebergTableCommitter(final String nodeName, final String nodeType, final IcebergOpsOptions options) {
        LOGGER.debug("Creating Iceberg table committer with options={}", options);
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.logPayloadLevel = options.logPayloadLevel();
        this.mappings = options.mappings();
        this.defaultOptions = this.mappings.get(DEFAULT_MAPPING);
        this.catalogType = options.catalogType();
        this.catalogProperties = options.getCatalogProperties();
        this.deleteCommitPathOnSuccess = options.isDeleteCommitPathOnSuccess();
        this.kerberosLoginEnabled = isDynamicKerberosLoginEnabled();
        this.partitionerMap = new ConcurrentHashMap<>();
        LOGGER.debug("Initialized Iceberg table committer successfully");
    }

    @Setup
    public void setup() throws Exception {
        if (kerberosLoginEnabled) performKerberosLogin();
        catalog = icebergCatalog(catalogType, catalogProperties, hiveConfiguration());
    }

    @SuppressWarnings("unchecked")
    @ProcessElement
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx, final BoundedWindow window, final PaneInfo paneInfo) throws Exception {
        log(logPayloadLevel, nodeName, nodeType, kv);
        GenericRecord record = kv.getValue();
        Schema schema = record.getSchema();
        String schemaId = schema.getFullName();
        counter(nodeName, nodeType, schemaId, ELEMENTS_IN).inc();
        SchemaLevelOptions options = mappings.getOrDefault(schemaId, defaultOptions);
        GenericRecordToPartitionText partitioner = partitionerMap.computeIfAbsent(schemaId, s -> partitioner(options, this.nodeName, this.nodeType, null));
        String tableId = partitioner.tableName(kv, paneInfo, window, Instant.now());
        String dbName = options.getDbName();
        LOGGER.debug("Table derived as", Map.of(SCHEMA_ID, schemaId, "dbName", dbName, "tableName", tableId));
        Table table = catalog.loadTable(TableIdentifier.of(dbName, tableId));
        if (table == null) {
            LOGGER.error("Could not find table entry", Map.of("dbName", dbName, "tableName", tableId));
            throw new GenericException("Could not find table entry", Map.of("dbName", dbName, "tableName", tableId));
        }
        String fullTableId = dbName + "." + tableId;
        String commitDir = partitioner.commitPath(kv, paneInfo, window, Instant.now(), fullTableId, () -> defaultCommitPath(PipelineUtils.getSharedDir()));
        Path commitDirectory = Paths.get(commitDir);
        File commitDirectoryFile = commitDirectory.toFile();
        if (!commitDirectoryFile.exists() || !commitDirectoryFile.isDirectory()) {
            throw new GenericException("Commit Directory doesn't exist or is not a valid directory", Map.of("dbName", dbName, "tableName", tableId, "commitDir", commitDir));
        }
        LOGGER.debug("Committing Iceberg datafiles now..", Map.of("dbName", dbName, "tableName", tableId, "commitDir", commitDir));
        final AppendFiles append = table.newAppend();
        Map<String, Map<String, Object>> logInfo = new HashMap<>();
        long recordCount = 0;
        for (File file : commitDirectoryFile.listFiles()) {
            final String commitFilePath = file.getAbsolutePath();
            try (FileInputStream fis = new FileInputStream(file)) {
                try (ObjectInputStream reader = new ObjectInputStream(fis)) {
                    Object object = reader.readObject();
                    if (object == null || !(object instanceof List)) {
                        LOGGER.warn("Commit directory contains invalid serialized datafile..Will ignore and continue..", Map.of("fileName", commitFilePath, "dbName", dbName, "tableName", tableId, "commitDir", commitDir));
                        continue;
                    }
                    List<DataFile> dataFiles = (List<DataFile>) object;
                    for (DataFile dataFile : dataFiles) {
                        String path = dataFile.path().toString();
                        long count = dataFile.recordCount();
                        LOGGER.debug("Appending dataFile to iceberg commit", Map.of("dbName", dbName, "tableName", tableId, "commitFilePath", commitFilePath, "dataFilePath", path));
                        append.appendFile(dataFile);
                        recordCount += count;
                        logInfo.put(path, Map.of("count", count));
                    }
                }
            }
        }
        append.commit();
        counter(nodeName, nodeType, UNKNOWN, COMMIT_SUCCESS, "tableName", fullTableId).inc(recordCount);
        if (deleteCommitPathOnSuccess) {
            LOGGER.debug("Delete commit files on success is enabled. Will trigger delete folder for path", Map.of("dbName", dbName, "tableName", tableId, "commitDir", commitDir));
            File[] files = commitDirectoryFile.listFiles();
            if (files != null) {
                for (File file : files) {
                    file.delete();
                }
            }
            commitDirectoryFile.delete();
        }
        LOGGER.debug("Iceberg commit successful", Map.of("dbName", dbName, "tableName", tableId, "commitDir", commitDir, "fileCount", logInfo.size(), "recordCount", recordCount), logInfo);
        incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, DB_NAME + "_" + TABLE_NAME, dbName + "_" + tableId, SOURCE_SCHEMA_ID, schemaId);
        ctx.output(kv);
    }
}
