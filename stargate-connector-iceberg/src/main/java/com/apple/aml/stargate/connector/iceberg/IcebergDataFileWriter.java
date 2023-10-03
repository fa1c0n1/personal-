package com.apple.aml.stargate.connector.iceberg;

import com.apple.aml.stargate.beam.sdk.formatters.GenericRecordToPartitionText;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.options.IcebergOptions;
import com.apple.aml.stargate.common.options.SchemaLevelOptions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.joda.time.Instant;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.beam.sdk.formatters.GenericRecordToPartitionText.partitioner;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_WRITTEN;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.SPECIAL_DELIMITER_CAP;
import static com.apple.aml.stargate.common.constants.CommonConstants.SPECIAL_DELIMITER_CAP_REGEX;
import static com.apple.aml.stargate.common.constants.PipelineConstants.DEFAULT_MAPPING;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.connector.iceberg.IcebergBaseWriter.getTaskWriter;
import static com.apple.aml.stargate.connector.iceberg.IcebergIO.icebergCatalog;
import static com.apple.aml.stargate.connector.iceberg.IcebergTableCreator.getOrCreateTable;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.hiveConfiguration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.isDynamicKerberosLoginEnabled;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.performKerberosLogin;

public class IcebergDataFileWriter extends DoFn<KV<String, GenericRecord>, KV<String, DataFile>> {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    protected final boolean enableLocation;
    private final String nodeName;
    private final String nodeType;
    private final Map<String, SchemaLevelOptions> mappings;
    private final String basePath;
    private final String dbPath;
    private final Map<String, Object> tableProperties;
    private final SchemaLevelOptions defaultOptions;
    private final FileFormat fileFormat;
    private final boolean commitPerBundle;
    private final PipelineConstants.CATALOG_TYPE catalogType;
    private final Map<String, Object> catalogProperties;
    private final ConcurrentHashMap<String, GenericRecordToPartitionText> partitionerMap;
    private final boolean kerberosLoginEnabled;
    private Catalog catalog;
    private transient ConcurrentHashMap<String, Table> tableMap;
    private transient ConcurrentHashMap<String, Map<String, String>> namespaceMap;
    private transient ConcurrentHashMap<String, TaskWriter<GenericRecord>> writerMap;
    private transient BoundedWindow lastSeenWindow;

    public IcebergDataFileWriter(final String nodeName, final String nodeType, final IcebergOptions options) {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.mappings = options.mappings();
        this.basePath = options.basePath();
        this.dbPath = options.getDbPath();
        this.tableProperties = options.getTableProperties();
        this.defaultOptions = this.mappings.get(DEFAULT_MAPPING);
        this.fileFormat = FileFormat.valueOf((options.getFileFormat() == null ? "parquet" : options.getFileFormat()).toUpperCase());
        this.catalogType = options.catalogType();
        this.catalogProperties = options.getCatalogProperties();
        this.commitPerBundle = options.isCommitInPlace();
        this.enableLocation = options.enableLocation();
        this.partitionerMap = new ConcurrentHashMap<>();
        this.kerberosLoginEnabled = isDynamicKerberosLoginEnabled();
    }

    @Setup
    public void setup() throws Exception {
        if (kerberosLoginEnabled) {
            performKerberosLogin();
        }
        namespaceMap = new ConcurrentHashMap<>();
        tableMap = new ConcurrentHashMap<>();
        catalog = icebergCatalog(catalogType, catalogProperties, hiveConfiguration());
    }

    @StartBundle
    public void startBundle(final StartBundleContext context) {
        writerMap = new ConcurrentHashMap<>();
        if (namespaceMap == null) {
            namespaceMap = new ConcurrentHashMap<>();
        }
        if (tableMap == null) {
            tableMap = new ConcurrentHashMap<>();
        }
    }

    @FinishBundle
    public void finishBundle(final FinishBundleContext context) {
        if (writerMap.isEmpty()) {
            LOGGER.debug("No writers created in this bundle. Will skip this bundle", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType));
            return;
        }
        LOGGER.debug("Processing iceberg finishBundle", Map.of("writerCount", writerMap.size(), NODE_NAME, nodeName, NODE_TYPE, nodeType));
        List<KV<String, DataFile>> dataFiles = new ArrayList<>();
        Instant now = Instant.now();
        Map<String, List<DataFile>> commitMap = new HashMap<>();
        for (Map.Entry<String, TaskWriter<GenericRecord>> entry : writerMap.entrySet()) {
            TaskWriter writer = entry.getValue();
            String[] tokens = entry.getKey().split(SPECIAL_DELIMITER_CAP_REGEX);
            String dbName = tokens[1];
            String tableId = tokens[2];
            String fullTableId = dbName + "." + tableId;
            int count = 0;
            try {
                WriteResult result = writer.complete();
                List<DataFile> files = commitMap.get(fullTableId);
                if (files == null) {
                    files = new ArrayList<>();
                    commitMap.put(fullTableId, files);
                }
                for (DataFile file : result.dataFiles()) {
                    dataFiles.add(KV.of(fullTableId, file));
                    files.add(file);
                    count++;
                }
            } catch (IOException e) {
                throw new GenericException("Error while closing iceberg writers", commitMap, e).wrap();
            }
            LOGGER.debug("Iceberg bundle details", Map.of("dbName", dbName, "tableId", tableId, "fileCount", count, NODE_NAME, nodeName, NODE_TYPE, nodeType));
        }
        writerMap.clear();
        if (commitPerBundle) {
            LOGGER.debug("Iceberg commit per bundle enabled. Committing now", Map.of("tableCount", commitMap.size(), NODE_NAME, nodeName, NODE_TYPE, nodeType));
            for (Map.Entry<String, List<DataFile>> entry : commitMap.entrySet()) {
                String[] split = entry.getKey().split("\\.");
                TableIdentifier tableIdentifier = TableIdentifier.of(split[0], split[1]);
                Table table = catalog.loadTable(tableIdentifier);
                final AppendFiles append = table.newAppend();
                List<DataFile> files = entry.getValue();
                Map<String, Map<String, Object>> logInfo = new HashMap<>();
                for (DataFile file : files) {
                    append.appendFile(file);
                    logInfo.put(file.path().toString(), Map.of("count", file.recordCount()));
                }
                append.commit();
                LOGGER.debug("Iceberg commit successful", Map.of("fileCount", files.size(), "tableName", table.name(), "location", table.location(), NODE_NAME, nodeName, NODE_TYPE, nodeType), logInfo);
            }
        }
        for (KV<String, DataFile> dataFile : dataFiles) {
            context.output(dataFile, now, lastSeenWindow);
        }
    }

    @SuppressWarnings("unchecked")
    @ProcessElement
    public void processElement(final ProcessContext context, final BoundedWindow window) throws Exception {
        KV<String, GenericRecord> kv = context.element();
        if (kv == null) {
            LOGGER.warn("Null kv record found. Will skip this record", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType));
            return;
        }
        GenericRecord record = kv.getValue();
        if (record == null) {
            LOGGER.warn("Null avro record found. Will skip this record", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType));
            return;
        }
        Schema schema = record.getSchema();
        String schemaId = schema.getFullName();
        PaneInfo paneInfo = context.pane();
        long taskId = paneInfo.getIndex();
        TaskWriter writer = getWriter(kv, schemaId, schema, taskId, paneInfo, window);
        writer.write(record);
        counter(nodeName, nodeType, schemaId, ELEMENTS_WRITTEN).inc();
        lastSeenWindow = window;
    }

    @SuppressWarnings("unchecked")
    private TaskWriter getWriter(final KV<String, GenericRecord> kv, final String schemaId, final Schema schema, final long paneIndex, final PaneInfo paneInfo, final BoundedWindow window) throws Exception {
        SchemaLevelOptions options = mappings.getOrDefault(schemaId, defaultOptions);
        GenericRecordToPartitionText partitioner = partitionerMap.computeIfAbsent(schemaId, s -> partitioner(options, this.nodeName, this.nodeType, null));
        String tableId = partitioner.tableName(kv, paneInfo, window, Instant.now());
        final String fullTableId = options.getDbName() + "." + tableId;
        final String key = schemaId + SPECIAL_DELIMITER_CAP + options.getDbName() + SPECIAL_DELIMITER_CAP + tableId + SPECIAL_DELIMITER_CAP + paneIndex;
        return writerMap.computeIfAbsent(key, s -> {
            LOGGER.debug("Creating a new writer for current bundle", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName, NODE_TYPE, nodeType));
            Table table = tableMap.computeIfAbsent(fullTableId, t -> getOrCreateTable(nodeName, nodeType, options.getDbName(), tableId, schema, mappings.getOrDefault(schema.getFullName(), defaultOptions), namespaceMap, catalog, dbPath, basePath, tableProperties, enableLocation));
            return getTaskWriter(table, paneIndex, schemaId, fileFormat, nodeName, nodeType);
        });
    }
}
