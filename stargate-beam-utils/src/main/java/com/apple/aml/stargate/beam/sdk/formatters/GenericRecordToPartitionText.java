package com.apple.aml.stargate.beam.sdk.formatters;

import com.apple.aml.stargate.common.constants.PipelineConstants.DATA_FORMAT;
import com.apple.aml.stargate.common.options.SchemaLevelOptions;
import com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils;
import freemarker.template.Configuration;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.function.Supplier;

import static com.apple.aml.stargate.common.constants.CommonConstants.DEFAULT_DELIMITER;
import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.Expressions.ALIAS_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.Expressions.COLLECTION_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.Expressions.FILE_PATH;
import static com.apple.aml.stargate.common.constants.CommonConstants.Expressions.PARTITION_PATH;
import static com.apple.aml.stargate.common.constants.CommonConstants.Expressions.TABLE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_PARTITIONED;
import static com.apple.aml.stargate.common.constants.CommonConstants.SPECIAL_DELIMITER_CAP;
import static com.apple.aml.stargate.common.utils.AvroUtils.getFieldValue;
import static com.apple.aml.stargate.common.utils.FormatUtils.parseDateTime;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.jvm.commons.util.Strings.isBlank;

public final class GenericRecordToPartitionText implements SerializableFunction<KV<String, GenericRecord>, String> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final DateTimeFormatter DEFAULT_FORMATTER = DateTimeFormatter.ofPattern(PARTITION_PATH).withZone(ZoneId.from(ZoneOffset.UTC));
    private final String columnName;
    private final String format;
    private final String filePath;
    private final String prefix;
    private final DATA_FORMAT fileFormat;
    private final String outputKey;
    private final String tableName;
    private final String commitPath;
    private final String collectionName;
    private final String aliasName;
    private final String nodeName;
    private final String nodeType;
    private final String prefixTemplateName;
    private final String filePathTemplateName;
    private final String tableNameTemplateName;
    private final String outputKeyTemplateName;
    private final String commitPathTemplateName;
    private final String collectionNameTemplateName;
    private final String aliasNameTemplateName;
    private final String xsvDelimiter;
    private final boolean includeXSVHeader;
    private transient DateTimeFormatter formatter;
    private transient Configuration configuration;

    public GenericRecordToPartitionText(final SchemaLevelOptions options, final String nodeName, final String nodeType, final DATA_FORMAT fileFormat) {
        this.columnName = options.getPartitionColumn();
        this.prefix = isBlank(options.prefix()) ? null : options.prefix().trim();
        this.filePath = isBlank(options.getFilePath()) ? (this.prefix == null ? FILE_PATH : ("/" + FILE_PATH)) : options.getFilePath();
        this.fileFormat = fileFormat;
        this.format = options.getPartitionFormat();
        this.tableName = isBlank(options.getTableName()) ? TABLE_NAME : options.getTableName().trim();
        this.outputKey = isBlank(options.getOutputKey()) ? null : options.getOutputKey().trim();
        this.commitPath = isBlank(options.getCommitPath()) ? null : options.getCommitPath().trim();
        this.collectionName = isBlank(options.getCollectionName()) ? COLLECTION_NAME : options.getCollectionName().trim();
        this.aliasName = isBlank(options.getAliasName()) ? ALIAS_NAME : options.getAliasName().trim();
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.prefixTemplateName = this.prefix == null ? null : (nodeName + "~prefix");
        this.filePathTemplateName = isBlank(this.filePath) ? null : (nodeName + "~filePath");
        this.outputKeyTemplateName = this.outputKey == null ? null : (nodeName + "~outputKey");
        this.tableNameTemplateName = nodeName + "~tableName";
        this.commitPathTemplateName = this.commitPath == null ? null : (nodeName + "~commitPath");
        this.collectionNameTemplateName = nodeName + "~collectionName";
        this.aliasNameTemplateName = nodeName + "~aliasName";
        this.xsvDelimiter = isBlank(options.getXsvDelimiter()) ? DEFAULT_DELIMITER : options.getXsvDelimiter();
        this.includeXSVHeader = options.isIncludeXSVHeader();

        LOGGER.debug("Created Partitioner successfully using", options.logMap());
    }

    public static GenericRecordToPartitionText partitioner(final SchemaLevelOptions options, final String nodeName, final String nodeType, final DATA_FORMAT fileFormat) {
        return new GenericRecordToPartitionText(options, nodeName, nodeType, fileFormat);
    }

    public String outputKey(final KV<String, GenericRecord> kv, final PaneInfo paneInfo, final BoundedWindow window, final org.joda.time.Instant timestamp) throws Exception {
        if (this.outputKey == null) {
            return null;
        }
        long startTime = System.nanoTime();
        String schemaId = UNKNOWN;
        try {
            GenericRecord record = kv.getValue();
            Schema schema = record.getSchema();
            schemaId = schema.getFullName();
            return evaluateFreemarker(configuration(), outputKeyTemplateName, kv.getKey(), record, schema, "paneInfo", paneInfo, "window", window, "timestamp", timestamp);
        } finally {
            histogramDuration(nodeName, nodeType, schemaId, "evaluate_output_key").observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

    @SuppressWarnings("unchecked")
    private Configuration configuration() {
        if (configuration != null) {
            return configuration;
        }
        if (this.prefixTemplateName != null) {
            loadFreemarkerTemplate(prefixTemplateName, this.prefix);
        }
        if (this.filePathTemplateName != null) {
            loadFreemarkerTemplate(filePathTemplateName, this.filePath);
        }
        if (this.tableNameTemplateName != null) {
            loadFreemarkerTemplate(tableNameTemplateName, this.tableName);
        }
        if (this.commitPathTemplateName != null) {
            loadFreemarkerTemplate(commitPathTemplateName, this.commitPath);
        }
        if (this.outputKeyTemplateName != null) {
            loadFreemarkerTemplate(outputKeyTemplateName, this.outputKey);
        }
        if (this.collectionNameTemplateName != null) {
            loadFreemarkerTemplate(collectionNameTemplateName, this.collectionName);
        }
        if (this.aliasNameTemplateName != null) {
            loadFreemarkerTemplate(aliasNameTemplateName, this.aliasName);
        }
        configuration = freeMarkerConfiguration();
        return configuration;
    }

    public String tableName(final KV<String, GenericRecord> kv, final PaneInfo paneInfo, final BoundedWindow window, final org.joda.time.Instant timestamp) throws Exception {
        long startTime = System.nanoTime();
        String schemaId = UNKNOWN;
        try {
            GenericRecord record = kv.getValue();
            Schema schema = record.getSchema();
            schemaId = schema.getFullName();
            return evaluateFreemarker(configuration(), tableNameTemplateName, kv.getKey(), record, schema, "paneInfo", paneInfo, "window", window, "timestamp", timestamp);
        } finally {
            histogramDuration(nodeName, nodeType, schemaId, "evaluate_table_name").observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

    @SneakyThrows
    public String collectionName(final KV<String, GenericRecord> kv, final PaneInfo paneInfo, final BoundedWindow window, final org.joda.time.Instant timestamp) {
        long startTime = System.nanoTime();
        String schemaId = UNKNOWN;
        try {
            GenericRecord record = kv.getValue();
            Schema schema = record.getSchema();
            schemaId = schema.getFullName();
            return evaluateFreemarker(configuration(), collectionNameTemplateName, kv.getKey(), record, schema, "paneInfo", paneInfo, "window", window, "timestamp", timestamp);
        } finally {
            histogramDuration(nodeName, nodeType, schemaId, "evaluate_collection_name").observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

    public String aliasName(final KV<String, GenericRecord> kv, final PaneInfo paneInfo, final BoundedWindow window, final org.joda.time.Instant timestamp) throws Exception {
        long startTime = System.nanoTime();
        String schemaId = UNKNOWN;
        try {
            GenericRecord record = kv.getValue();
            Schema schema = record.getSchema();
            schemaId = schema.getFullName();
            return evaluateFreemarker(configuration(), aliasNameTemplateName, kv.getKey(), record, schema, "paneInfo", paneInfo, "window", window, "timestamp", timestamp);
        } finally {
            histogramDuration(nodeName, nodeType, schemaId, "evaluate_alias_name").observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

    @SneakyThrows
    public String commitPath(final KV<String, GenericRecord> kv, final PaneInfo paneInfo, final BoundedWindow window, final org.joda.time.Instant timestamp, final String subPath, final Supplier<String> defaultSupplier) {
        if (this.commitPath == null) {
            return String.format("%s/%s", defaultSupplier.get(), subPath);
        }
        long startTime = System.nanoTime();
        String schemaId = UNKNOWN;
        try {
            GenericRecord record = kv.getValue();
            Schema schema = record.getSchema();
            schemaId = schema.getFullName();
            String path = evaluateFreemarker(configuration(), commitPathTemplateName, kv.getKey(), record, schema, "paneInfo", paneInfo, "window", window, "timestamp", timestamp).trim();
            return String.format("%s/%s", path.startsWith("/") ? path : String.format("%s/%s", PipelineUtils.getSharedDir(), path), subPath);
        } finally {
            histogramDuration(nodeName, nodeType, schemaId, "evaluate_commit_path").observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

    @SuppressWarnings("deprecation")
    public Triple<String, String, org.joda.time.Instant> tokens(final KV<String, GenericRecord> kv, final PaneInfo paneInfo, final BoundedWindow window, final org.joda.time.Instant timestamp) throws Exception {
        long startTime = System.nanoTime();
        String schemaId = UNKNOWN;
        try {
            GenericRecord record = kv.getValue();
            Schema schema = record.getSchema();
            schemaId = schema.getFullName();
            counter(nodeName, nodeType, schemaId, ELEMENTS_PARTITIONED).inc();
            Object partitionValue = columnName == null ? null : getFieldValue(record, columnName);
            Instant dateTime = partitionValue == null ? Instant.now() : parseDateTime(partitionValue);
            String partitionText = dateTime == null ? String.valueOf(partitionValue) : formatter().format(dateTime);
            Configuration config = configuration();
            String prefixPath = prefixTemplateName == null ? EMPTY_STRING : evaluateFreemarker(config, prefixTemplateName, kv.getKey(), record, schema, "paneInfo", paneInfo, "window", window, "timestamp", timestamp);
            String suffixPath = filePathTemplateName == null ? EMPTY_STRING : evaluateFreemarker(config, filePathTemplateName, kv.getKey(), record, schema, "paneInfo", paneInfo, "window", window, "timestamp", timestamp);
            histogramDuration(nodeName, nodeType, schemaId, "evaluate_partitioner").observe((System.nanoTime() - startTime) / 1000000.0);
            return Triple.of(prefixPath + suffixPath, partitionText, timestamp);
        } catch (Exception e) {
            histogramDuration(nodeName, nodeType, schemaId, "evaluate_partitioner_error").observe((System.nanoTime() - startTime) / 1000000.0);
            throw e;
        } finally {
            histogramDuration(nodeName, nodeType, schemaId, "partition").observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

    private DateTimeFormatter formatter() {
        if (formatter != null) {
            return formatter;
        }
        try {
            formatter = this.format == null ? DEFAULT_FORMATTER : DateTimeFormatter.ofPattern(this.format).withZone(ZoneId.from(ZoneOffset.UTC));
        } catch (Exception ignored) {
            LOGGER.warn("Could not create formatter using format : " + this.format, ignored);
            formatter = DEFAULT_FORMATTER;
        }
        return formatter;
    }

    @SneakyThrows
    @Override
    public String apply(final KV<String, GenericRecord> kv) {
        long startTime = System.nanoTime();
        String schemaId = UNKNOWN;
        try {
            GenericRecord record = kv.getValue();
            Schema schema = record.getSchema();
            schemaId = schema.getFullName();
            counter(nodeName, nodeType, schemaId, ELEMENTS_PARTITIONED).inc();
            Object partitionValue = columnName == null ? null : getFieldValue(record, columnName);
            Instant dateTime = partitionValue == null ? Instant.now() : parseDateTime(partitionValue);
            String partitionText = dateTime == null ? String.valueOf(partitionValue) : formatter().format(dateTime);
            Configuration config = configuration();
            org.joda.time.Instant timestamp = org.joda.time.Instant.now();
            String prefixPath = prefixTemplateName == null ? EMPTY_STRING : evaluateFreemarker(config, prefixTemplateName, kv.getKey(), record, schema, "timestamp", timestamp);
            String suffixPath = filePathTemplateName == null ? EMPTY_STRING : evaluateFreemarker(config, filePathTemplateName, kv.getKey(), record, schema, "timestamp", timestamp);
            return String.join(SPECIAL_DELIMITER_CAP, prefixPath + suffixPath, fileFormat.name(), schema.getFullName(), partitionText);
        } finally {
            histogramDuration(nodeName, nodeType, schemaId, "partition").observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

}
