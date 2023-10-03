package com.apple.aml.stargate.connector.iceberg;

import com.apple.aml.stargate.beam.sdk.formatters.GenericRecordToPartitionText;
import com.apple.aml.stargate.beam.sdk.io.file.BaseFileWriter;
import com.apple.aml.stargate.beam.sdk.utils.FileWriterFns.WriterKey;
import com.apple.aml.stargate.beam.sdk.utils.FileWriterFns.WriterValue;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.constants.PipelineConstants.CATALOG_TYPE;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.options.IcebergOptions;
import com.apple.aml.stargate.common.options.SchemaLevelOptions;
import com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.beam.BeamAppenderFactory;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.io.WriteResult;
import org.joda.time.Instant;
import org.slf4j.Logger;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.FORMATTER_DATE_TIME_1;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.BUCKET_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.FILE_PATH;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.COMMIT_SUCCESS;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.connector.iceberg.GenericRecordStructLikeWrapper.wrapper;
import static com.apple.aml.stargate.connector.iceberg.IcebergIO.commitFileSchema;
import static com.apple.aml.stargate.connector.iceberg.IcebergIO.defaultCommitPath;
import static com.apple.aml.stargate.connector.iceberg.IcebergIO.icebergCatalog;
import static com.apple.aml.stargate.connector.iceberg.IcebergTableCreator.getOrCreateTable;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.hiveConfiguration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;


public class IcebergBaseWriter<Input> extends BaseFileWriter<Input> {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    protected final boolean enableLocation;
    protected final String bucket;
    private final String dbPath;
    private final Map<String, Object> tableProperties;
    private final int commitRetries;
    private final long commitRetryWait;
    private final boolean commitToDisk;
    private final boolean commitInPlace;
    private final Schema commitFileSchema;
    private final CATALOG_TYPE catalogType;
    private final Map<String, Object> catalogProperties;
    protected transient Catalog catalog;
    protected transient ConcurrentHashMap<String, Table> tableMap;
    protected transient ConcurrentHashMap<String, String> commitPathMap;
    protected transient ConcurrentHashMap<String, Map<String, String>> namespaceMap;

    public IcebergBaseWriter(final String nodeName, final String nodeType, final PipelineConstants.ENVIRONMENT environment, final IcebergOptions options, final boolean emit) {
        super(nodeName, nodeType, environment, options, emit);
        this.bucket = options.getBucket();
        this.dbPath = options.getDbPath();
        this.tableProperties = options.getTableProperties();
        this.commitRetries = options.getCommitRetries();
        this.commitRetryWait = options.getCommitRetryWait();
        this.commitInPlace = options.isCommitInPlace();
        this.commitFileSchema = this.commitInPlace ? null : commitFileSchema(environment);
        this.commitToDisk = options.isCommitToDisk();
        this.catalogType = options.catalogType();
        this.catalogProperties = options.getCatalogProperties();
        this.enableLocation = options.enableLocation();
        this.namespaceMap = new ConcurrentHashMap<>();
    }

    public void setup() throws Exception {
        super.setup();
        if (namespaceMap == null) namespaceMap = new ConcurrentHashMap<>();
        if (tableMap == null) tableMap = new ConcurrentHashMap<>();
        if (catalog == null) catalog = icebergCatalog(catalogType, catalogProperties, hiveConfiguration());
        commitPathMap = new ConcurrentHashMap<>();
    }

    public void closeBatch(final ConcurrentHashMap<WriterKey, WriterValue> writerMap, final WindowedContext context, final PipelineConstants.BATCH_WRITER_CLOSE_TYPE batchType) throws Exception {
        if (writerMap.isEmpty()) {
            LOGGER.debug("No writers created in this batch. Will skip this batch");
            return;
        }
        Map<String, List<DataFile>> commitMap = new HashMap<>();
        Exception latestException = null;
        int writerCount = writerMap.size();
        long totalRecordCount = 0;
        LOGGER.debug("Processing iceberg closeBatch", Map.of("writerCount", writerCount, NODE_NAME, nodeName, NODE_TYPE, nodeType));

        for (WriterKey writerKey : new ArrayList<>(writerMap.keySet())) {
            IcebergWriterValue value = (IcebergWriterValue) writerMap.remove(writerKey);
            if (value == null) continue;
            Map writerLogInfo = writerKey.logMap();
            TaskWriter writer = value.getTaskWriter();
            String fullTableId = writerKey.getFilePath();
            int fileCount = 0;
            long recordCount = 0;
            List<DataFile> files = commitMap.get(fullTableId);
            try {
                WriteResult result = writer.complete();
                if (files == null) {
                    files = new ArrayList<>();
                    commitMap.put(fullTableId, files);
                }
                for (DataFile file : result.dataFiles()) {
                    long rcount = file.recordCount();
                    if (rcount <= 0) continue;
                    files.add(file);
                    recordCount += rcount;
                    totalRecordCount += rcount;
                    fileCount++;
                }
            } catch (Exception e) {
                String fileNames = files == null ? EMPTY_STRING : files.stream().map(f -> f.path()).collect(Collectors.joining(","));
                Map<String, Object> logInfo = Map.of("fullTableId", fullTableId, "writer", String.valueOf(writer), "files", fileNames, "recordCount", recordCount, "totalRecordCount", totalRecordCount);
                if (files == null || files.isEmpty() || recordCount == 0) {
                    LOGGER.debug("Iceberg task writer created but with 0 records. Will try to abort this writer and ignore any errors", logInfo, writerLogInfo, Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                    try {
                        writer.abort();
                    } catch (Exception ae) {
                    }
                } else {
                    latestException = e;
                    LOGGER.error("Error while closing iceberg writer", logInfo, writerLogInfo, Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                }
            }
            LOGGER.debug("Iceberg bundle details", Map.of("fileCount", fileCount, "recordCount", recordCount), writerLogInfo);
        }
        if (latestException != null) {
            throw latestException;
        }
        if (commitInPlace) {
            if (commitToDisk) saveCommitDetailsToDisk(commitMap);
            else commitDataFiles(commitMap, catalog, commitRetries, commitRetryWait, nodeName, nodeType);
        } else {
            emitCommitFiles(commitMap, context);
        }
        LOGGER.debug("Iceberg io writers closed successfully", Map.of("writerCount", writerCount, "totalRecordCount", totalRecordCount, NODE_NAME, nodeName, NODE_TYPE, nodeType));
    }

    private void saveCommitDetailsToDisk(final Map<String, List<DataFile>> commitMap) throws Exception {
        LOGGER.debug("Saving Iceberg datafile details to disk now", Map.of("tableCount", commitMap.size(), NODE_NAME, nodeName, NODE_TYPE, nodeType));
        for (Map.Entry<String, List<DataFile>> entry : commitMap.entrySet()) {
            String fullTableId = entry.getKey();
            String[] split = fullTableId.split("\\.");
            String commitDir = commitPathMap.get(fullTableId);
            Path commitDirectory = Paths.get(commitDir);
            commitDirectory.toFile().mkdirs();
            commitDir = commitDirectory.toFile().getAbsolutePath();
            java.time.Instant instant = java.time.Instant.now();
            String fileName = FORMATTER_DATE_TIME_1.format(instant) + "-" + UUID.randomUUID();
            Path commitFile = Paths.get(commitDir, fileName);
            try (FileOutputStream file = new FileOutputStream(commitFile.toFile())) {
                try (ObjectOutputStream writer = new ObjectOutputStream(file)) {
                    writer.writeObject(entry.getValue());
                }
            }
        }
    }

    public static void commitDataFiles(final Map<String, List<DataFile>> commitMap, final Catalog catalog, final int commitRetries, final long commitRetryWait, final String nodeName, final String nodeType) throws InterruptedException {
        LOGGER.debug("Committing Iceberg bundle now", Map.of("tableCount", commitMap.size(), NODE_NAME, nodeName, NODE_TYPE, nodeType));
        for (String fullTableId : new ArrayList<>(commitMap.keySet())) {
            String[] split = fullTableId.split("\\.");
            String tableName = fullTableId;
            List<DataFile> files = commitMap.remove(fullTableId);
            if (files == null || files.isEmpty()) continue;
            int fileCount = files.size();
            String location = "unknown";
            for (int i = 0; i < commitRetries; i++) {
                Map<String, Map<String, Object>> logInfo = new HashMap<>();
                try {
                    Table table = catalog.loadTable(TableIdentifier.of(split[0], split[1]));
                    if (table == null) {
                        LOGGER.error("Could not find local table entry", Map.of("fullTableId", fullTableId, NODE_NAME, nodeName, NODE_TYPE, nodeType));
                        throw new GenericException("Could not find local table entry", Map.of("fullTableId", fullTableId, NODE_NAME, nodeName, NODE_TYPE, nodeType));
                    }
                    tableName = table.name();
                    location = table.location();
                    final AppendFiles append = table.newAppend();
                    long totalRecordCount = 0;
                    for (DataFile file : files) {
                        append.appendFile(file);
                        long count = file.recordCount();
                        totalRecordCount += count;
                        logInfo.put(file.path().toString(), Map.of("count", count));
                    }
                    append.commit();
                    counter(nodeName, nodeType, UNKNOWN, COMMIT_SUCCESS, "tableName", fullTableId).inc(totalRecordCount);
                    LOGGER.debug("Iceberg commit successful", Map.of("retryIndex", i, "fileCount", fileCount, "tableName", tableName, "location", location, NODE_NAME, nodeName, NODE_TYPE, nodeType, "totalCommitRecordCount", totalRecordCount), logInfo);
                    break;
                } catch (Exception e) {
                    if ((i + 1) == commitRetries) {
                        LOGGER.error("Iceberg commit failed. Exhausted retries too. Have to error out !!", Map.of("retryIndex", i, "fileCount", fileCount, "tableName", tableName, "location", location, NODE_NAME, nodeName, NODE_TYPE, nodeType), logInfo);
                        throw new GenericException("Iceberg commit failed for table : " + fullTableId, Map.of("tableName", tableName, "location", location, NODE_NAME, nodeName, NODE_TYPE, nodeType), e).wrap();
                    } else {
                        LOGGER.warn("Iceberg commit failed. Will retry again", Map.of("retryIndex", i, "fileCount", fileCount, "tableName", tableName, "location", location, NODE_NAME, nodeName, NODE_TYPE, nodeType), logInfo);
                        if (commitRetryWait > 0) {
                            LOGGER.info("Sleeping for " + commitRetryWait + "ms before retry.", Map.of("retryIndex", i, "fileCount", fileCount, "tableName", tableName, "location", location, NODE_NAME, nodeName, NODE_TYPE, nodeType), logInfo);
                            Thread.sleep(commitRetryWait);
                        }
                    }
                }
            }
        }
    }

    private void emitCommitFiles(final Map<String, List<DataFile>> commitMap, final WindowedContext context) throws InterruptedException {
        if (context == null && !commitMap.isEmpty()) {
            LOGGER.warn("Commit data files invoked without a context. cannot proceed. will commit in the next window..", Map.of("tableCount", commitMap.size(), NODE_NAME, nodeName, NODE_TYPE, nodeType));
            return;
        }
        LOGGER.debug("Emitting commit data files to next node", Map.of("tableCount", commitMap.size(), NODE_NAME, nodeName, NODE_TYPE, nodeType));
        int commitFileCount = 0;
        for (Map.Entry<String, List<DataFile>> entry : commitMap.entrySet()) {
            String fullTableId = entry.getKey();
            String[] split = fullTableId.split("\\.");
            for (DataFile dataFile : entry.getValue()) {
                emitCommitFile(context, split[0], split[1], dataFile);
                commitFileCount++;
            }
        }
        LOGGER.debug("Commit files sent to next node successfully", Map.of("tableCount", commitMap.size(), "commitFileCount", commitFileCount, NODE_NAME, nodeName, NODE_TYPE, nodeType));
    }

    private void emitCommitFile(final WindowedContext context, final String dbName, final String tableName, final DataFile dataFile) {
        GenericRecord record = new GenericData.Record(commitFileSchema);
        record.put("dbName", dbName);
        record.put("tableName", tableName);
        record.put("dataFile", new String(Base64.getEncoder().encode(SerializationUtils.serialize(dataFile)), StandardCharsets.UTF_8));
        context.outputWithTimestamp(KV.of(String.format("%s.%s", dbName, tableName), record), Instant.now());
    }

    @Override
    public void emitMetrics(final String schemaId, final String sourceSchemaId, final String filePath, final SchemaLevelOptions options) {
        incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, BUCKET_NAME + "_" + FILE_PATH, String.format("%s_%s", bucket, filePath), SOURCE_SCHEMA_ID, sourceSchemaId);
    }

    public WriterValue getWriterValue(final KV<String, GenericRecord> kv, final ConcurrentHashMap<WriterKey, WriterValue> writerMap, final SchemaLevelOptions options, final PaneInfo paneInfo, final BoundedWindow window, final org.joda.time.Instant timestamp, final Schema schema, final String schemaId, final int version, final GenericRecordToPartitionText partitioner) throws Exception {
        String tableId = partitioner.tableName(kv, paneInfo, window, timestamp);
        final String fullTableId = options.getDbName() + "." + tableId;
        final WriterKey writerKey = new WriterKey(nodeName, nodeType, schemaId, version, window, paneInfo, fullTableId, EMPTY_STRING, timestamp);
        WriterValue writer = writerMap.computeIfAbsent(writerKey, wk -> {
            try {
                LOGGER.debug("Creating a new iceberg writer for current bundle", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName, NODE_TYPE, nodeType));
                if (commitToDisk) commitPathMap.computeIfAbsent(fullTableId, x -> partitioner.commitPath(kv, paneInfo, window, timestamp, x, () -> defaultCommitPath(PipelineUtils.getSharedDir())));
                Table table = tableMap.computeIfAbsent(fullTableId, t -> getOrCreateTable(nodeName, nodeType, options.getDbName(), tableId, schema, options, namespaceMap, catalog, dbPath, basePath, tableProperties, enableLocation));
                TaskWriter taskWriter = getTaskWriter(table, wk.getPaneIndex(), schemaId, FileFormat.PARQUET, nodeName, nodeType);
                wk.setCreatedOn(org.joda.time.Instant.now());
                WriterValue value = new IcebergWriterValue(wk, taskWriter);
                LOGGER.debug("Iceberg writer created successfully for", Map.of(SCHEMA_ID, schemaId, "paneIndex", wk.getPaneIndex(), "windowStart", wk.getStart(), "windowEnd", wk.getEnd(), "workerId", wk.getWorkerId(), "tableId", wk.getFilePath()));
                return value;
            } catch (Exception e) {
                throw new GenericException("Could not create iceberg writer for writerKey", Map.of(SCHEMA_ID, schemaId, "paneIndex", wk.getPaneIndex(), "windowStart", wk.getStart(), "windowEnd", wk.getEnd(), "workerId", wk.getWorkerId(), "tableId", wk.getFilePath()), e).wrap();
            }
        });
        return writer;
    }

    @SuppressWarnings("unchecked")
    static TaskWriter getTaskWriter(final Table table, final long paneIndex, final String schemaId, final FileFormat fileFormat, final String nodeName, final String nodeType) {
        PartitionSpec spec = table.spec();
        FileIO io = table.io();
        TaskWriter writer;
        BeamAppenderFactory appenderFactory = new BeamAppenderFactory(table);
        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, (int) paneIndex, paneIndex).format(fileFormat).build();
        if (spec.isUnpartitioned()) {
            writer = new UnpartitionedWriter<>(spec, fileFormat, appenderFactory, fileFactory, io, Long.MAX_VALUE);
        } else {
            writer = new PartitionedFanoutWriter<GenericRecord>(spec, fileFormat, appenderFactory, fileFactory, io, Long.MAX_VALUE) {
                @Override
                protected PartitionKey partition(final GenericRecord row) {
                    PartitionKey partitionKey = new PartitionKey(spec, spec.schema());
                    partitionKey.partition(wrapper(row));
                    return partitionKey;
                }
            };
        }
        LOGGER.debug("Iceberg writer created successfully for", Map.of(SCHEMA_ID, schemaId, "tableName", table.name(), "location", table.location(), "tableInfo", table.toString(), NODE_NAME, nodeName, NODE_TYPE, nodeType));
        return writer;
    }
}

