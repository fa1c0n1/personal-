package com.apple.aml.stargate.beam.sdk.io.jdbc;

import com.apple.aml.stargate.beam.inject.BeamContext;
import com.apple.aml.stargate.beam.sdk.formatters.GenericRecordToPartitionText;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.options.JdbiOptions;
import com.apple.aml.stargate.common.options.SchemaLevelOptions;
import com.apple.aml.stargate.common.pojo.AvroRecord;
import com.apple.aml.stargate.common.utils.ContextHandler;
import com.apple.aml.stargate.pipeline.inject.JdbiServiceHandler;
import com.google.common.base.CaseFormat;
import freemarker.template.Configuration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.Instant;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.beam.sdk.formatters.GenericRecordToPartitionText.partitioner;
import static com.apple.aml.stargate.beam.sdk.io.jdbc.GenericJdbcIO.columnValues;
import static com.apple.aml.stargate.beam.sdk.io.jdbc.GenericJdbcIO.createTableStatement;
import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.TABLE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.PROCESS;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.PROCESS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.PROCESS_SUCCESS;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_LATEST_VERSION;
import static com.apple.aml.stargate.common.constants.PipelineConstants.DEFAULT_MAPPING;
import static com.apple.aml.stargate.common.utils.AvroUtils.getFieldValue;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.inject.JdbiServiceHandler.jdbiService;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class JdbcRecordExecutor extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final JdbiOptions options;
    private final boolean emit;
    private final String nodeName;
    private final String nodeType;
    final private String keyName;
    private final Map<String, SchemaLevelOptions> mappings;
    private final SchemaLevelOptions defaultOptions;
    private final ConcurrentHashMap<String, GenericRecordToPartitionText> partitionerMap;
    private final ConcurrentHashMap<Pair<String, String>, String> sqlMap = new ConcurrentHashMap<>();
    private transient Configuration configuration;
    private final String templateName;
    private final CaseFormat columnNameCaseFormat;
    private final ConcurrentHashMap<String, Boolean> tableMap = new ConcurrentHashMap<>();

    public JdbcRecordExecutor(final String nodeName, final String nodeType, final JdbiOptions options, final boolean emit) throws Exception {
        this.options = options;
        this.emit = emit;
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.keyName = isBlank(options.getKeyAttribute()) ? null : options.getKeyAttribute().trim();
        this.columnNameCaseFormat = isBlank(options.getColumnNameCaseFormat()) ? CaseFormat.LOWER_UNDERSCORE : CaseFormat.valueOf(options.getColumnNameCaseFormat().toUpperCase());
        this.mappings = options.mappings();
        this.defaultOptions = this.mappings.get(DEFAULT_MAPPING);
        this.partitionerMap = new ConcurrentHashMap<>();
        this.templateName = isBlank(options.getExpression()) ? null : (nodeName + "~template");
    }

    @StartBundle
    public void startBundle(final StartBundleContext context) {
        if (this.templateName != null) configuration();
    }

    @ProcessElement
    @SuppressWarnings("unchecked")
    public void processElement(@Element final KV<String, GenericRecord> kv, final BoundedWindow window, final PaneInfo paneInfo, final ProcessContext ctx) throws Exception {
        long startTime = System.nanoTime();
        log(options, nodeName, nodeType, kv);
        final GenericRecord record = kv.getValue();
        final Schema schema = record.getSchema();
        final String schemaId = schema.getFullName();
        counter(nodeName, nodeType, schemaId, ELEMENTS_IN).inc();
        int version = record instanceof AvroRecord ? ((AvroRecord) record).getSchemaVersion() : SCHEMA_LATEST_VERSION;
        String recordSchemaKey = String.format("%s:%d", schemaId, version);
        JdbiServiceHandler client = jdbiService(nodeName, options);
        String tableName = UNKNOWN;
        try {
            ContextHandler.setContext(BeamContext.builder().nodeName(nodeName).nodeType(nodeType).schema(schema).windowedContext(ctx).inputSchemaId(schemaId).outputSchemaId(options.getSchemaId()).build());
            int sqlStatus;
            if (templateName != null) {
                String sql = evaluateFreemarker(configuration(), templateName, kv.getKey(), kv.getValue(), schema);
                sqlStatus = client.execute(sql);
            } else {
                GenericRecordToPartitionText partitioner = partitionerMap.computeIfAbsent(schemaId, s -> partitioner(mappings.getOrDefault(schemaId, defaultOptions), this.nodeName, this.nodeType, null));
                tableName = ensureTableExists(schema, partitioner.tableName(kv, paneInfo, window, Instant.now()));
                if (isBlank(options.getOperation()) || "insert".equalsIgnoreCase(options.getOperation()) || "save".equalsIgnoreCase(options.getOperation()) || "write".equalsIgnoreCase(options.getOperation())) {
                    String sql = sqlMap.computeIfAbsent(Pair.of(recordSchemaKey, "insert"), p -> {
                        String columnNames = schema.getFields().stream().map(f -> CaseFormat.LOWER_CAMEL.to(columnNameCaseFormat, f.name())).collect(Collectors.joining(","));
                        return String.format("insert into %s(%s) values(%s)", "%s", columnNames, schema.getFields().stream().map(f -> "?").collect(Collectors.joining(",")));
                    });
                    sql = String.format(sql, tableName);
                    sqlStatus = client.execute(sql, columnValues(record));
                } else if (keyName != null && ("merge".equalsIgnoreCase(options.getOperation()) || "upsert".equalsIgnoreCase(options.getOperation()))) {
                    String sql = sqlMap.computeIfAbsent(Pair.of(recordSchemaKey, "upsert"), p -> {
                        String columnNames = schema.getFields().stream().map(f -> CaseFormat.LOWER_CAMEL.to(columnNameCaseFormat, f.name())).collect(Collectors.joining(","));
                        return String.format("insert into %s(%s) values(%s) on conflict(%s) do update set %s", "%s", columnNames, schema.getFields().stream().map(f -> "?").collect(Collectors.joining(",")), CaseFormat.LOWER_CAMEL.to(columnNameCaseFormat, keyName), schema.getFields().stream().map(f -> String.format("%s = ?", f.name())).collect(Collectors.joining(",")));
                    });
                    sql = String.format(sql, tableName);
                    Object[] values = columnValues(record);
                    Object[] args = new Object[values.length * 2];
                    System.arraycopy(values, 0, args, 0, values.length);
                    System.arraycopy(values, 0, args, values.length, values.length);
                    sqlStatus = client.execute(sql, args);
                } else if (keyName != null && ("update".equalsIgnoreCase(options.getOperation()) || "set".equalsIgnoreCase(options.getOperation()))) {
                    String primaryKey = (String) getFieldValue(record, keyName);
                    String sql = sqlMap.computeIfAbsent(Pair.of(recordSchemaKey, "update"), p -> String.format("update %s set %s where %s = ?", "%s", schema.getFields().stream().map(f -> String.format("%s = ?", f.name())).collect(Collectors.joining(",")), CaseFormat.LOWER_CAMEL.to(columnNameCaseFormat, keyName)));
                    sql = String.format(sql, tableName);
                    Object[] values = columnValues(record);
                    Object[] args = new Object[values.length + 1];
                    System.arraycopy(values, 0, args, 0, values.length);
                    args[values.length] = primaryKey;
                    sqlStatus = client.execute(sql, args);
                } else throw new UnsupportedOperationException();
            }
            if (sqlStatus < options.getSuccessStatus()) throw new GenericException("sql executed without errors; however the return status doesn't match success criteria; hence failing", Map.of(TABLE_NAME, tableName, "sqlStatus", sqlStatus));
            histogramDuration(nodeName, nodeType, schemaId, PROCESS_SUCCESS).observe((System.nanoTime() - startTime) / 1000000.0);
            incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, TABLE_NAME, tableName, SOURCE_SCHEMA_ID, schemaId);
            if (emit) ctx.output(kv);
        } catch (Exception e) {
            histogramDuration(nodeName, nodeType, schemaId, PROCESS_ERROR).observe((System.nanoTime() - startTime) / 1000000.0);
            counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
            LOGGER.warn("Could not execute jdbc query", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, "key", kv.getKey(), TABLE_NAME, tableName, SCHEMA_ID, schemaId, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            ctx.output(ERROR_TAG, eRecord(nodeName, nodeType, "jdbc_execute", kv, e));
        } finally {
            ContextHandler.clearContext();
            histogramDuration(nodeName, nodeType, schemaId, PROCESS).observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

    @SuppressWarnings("unchecked")
    private Configuration configuration() {
        if (configuration != null) {
            return configuration;
        }
        loadFreemarkerTemplate(templateName, this.options.getExpression());
        configuration = freeMarkerConfiguration();
        return configuration;
    }

    private String ensureTableExists(final Schema schema, final String tableName) {
        if (tableMap.computeIfAbsent(tableName, t -> {
            JdbiServiceHandler client = jdbiService(nodeName, options);
            try {
                int status = client.execute(createTableStatement(schema, tableName, columnNameCaseFormat, keyName));
                if (status < 0) throw new GenericException("sql execution returned invalid status", Map.of(TABLE_NAME, tableName, "sqlStatus", status));
            } catch (Exception e) {
                try {
                    client.execute(String.format("select 1 from %s where 1!=1", tableName));
                } catch (Exception ex) {
                    throw new RuntimeException(e);
                }
                return true;
            }
            return true;
        })) return tableName;
        throw new IllegalStateException("Could not find details about the requested table");
    }

}
