package com.apple.aml.stargate.beam.sdk.io.http;

import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.JdbcIndexOptions;
import com.apple.aml.stargate.common.options.LocalOfsOptions;
import com.apple.aml.stargate.common.utils.WebUtils;
import freemarker.template.Configuration;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.MEDIA_TYPE_APPLICATION_JSON;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CACHE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.TABLE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.URL;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.OfsConstants.PRIMARY_KEY_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.options.DMLOperation.dmlStatement;
import static com.apple.aml.stargate.common.utils.AvroUtils.getFieldValue;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.JsonUtils.dropSensitiveKeys;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.WebUtils.getHttpClient;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;

public class LocalOfsInvoker extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> {
    static final ConcurrentHashMap<String, Boolean> TABLE_CREATE_STATUS_MAP = new ConcurrentHashMap<>();
    static final ConcurrentHashMap<String, String> TABLE_NAME_MAP = new ConcurrentHashMap<>();
    static final ConcurrentHashMap<String, Boolean> TABLE_INDEX_MAP = new ConcurrentHashMap<>();
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final String tableNameTemplateName;
    private final String expressionTemplateName;
    private final String baseUri;
    private final LocalOfsOptions options;
    private final Map<String, Set<JdbcIndexOptions>> indexes;
    private final boolean emit;
    private final String type;
    private final String cacheName;
    private final String nodeName;
    private final String nodeType;
    private transient OkHttpClient client;
    private transient Configuration configuration;

    public LocalOfsInvoker(final StargateNode node, final LocalOfsOptions options, final Map<String, Set<JdbcIndexOptions>> indexes, final boolean emit) throws Exception {
        this.options = options;
        this.indexes = Optional.ofNullable(indexes).orElse(Map.of());
        this.baseUri = options.getBaseUri().trim();
        this.emit = emit;
        this.type = String.valueOf(options.getType());
        this.cacheName = options.getCacheName().trim();
        this.nodeName = node.getName();
        this.nodeType = node.getType();
        tableNameTemplateName = options.getTableName() == null ? null : (nodeName + "~tableName");
        expressionTemplateName = options.getExpression() == null ? null : (nodeName + "~expression");
        init();
    }

    private void init() throws Exception {
        client = options.isDisableCertificateValidation() ? getHttpClient() : new OkHttpClient();
    }

    static String sqlType(final Schema schema) {
        switch (schema.getType()) {
            case UNION:
                List<Schema> types = schema.getTypes();
                for (Schema type : types) {
                    if (type.getType() == Schema.Type.NULL) {
                        continue;
                    }
                    return sqlType(type);
                }
            case INT:
                return "INT";
            case LONG:
                return "BIGINT";
            case FLOAT:
            case DOUBLE:
                return "DECIMAL";
            case BOOLEAN:
                return "BOOLEAN";
            default:
                return "VARCHAR";
        }
    }


    static String createTableStatement(final String cacheName, final String tableName, final Schema schema) {
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append("( ").append(PRIMARY_KEY_NAME).append(" VARCHAR");
        for (Schema.Field child : schema.getFields()) {
            String sqlType = sqlType(child.schema());
            builder.append(", ").append(child.name()).append(" ").append(sqlType).append(" NULL");
        }
        builder.append(", PRIMARY KEY (").append(PRIMARY_KEY_NAME).append(") )");
        return builder.toString();
    }

    static Pair<String, String> ofsKVContent(final KV<String, GenericRecord> kv, final Configuration configuration, final String expressionTemplateName) throws Exception {
        String ofskvKey;
        Object value;
        GenericRecord record = kv.getValue();
        if (expressionTemplateName == null) {
            ofskvKey = (String) getFieldValue(record, "key");
            value = getFieldValue(record, "value");
        } else {
            Map<Object, Object> map = readJsonMap(evaluateFreemarker(configuration, expressionTemplateName, kv.getKey(), record, record.getSchema()));
            ofskvKey = (String) map.get("key");
            if (ofskvKey == null) {
                ofskvKey = (String) getFieldValue(record, "key");
            }
            value = map.get("value");
        }
        if (ofskvKey == null) {
            ofskvKey = kv.getKey();
        }
        String content = value == null ? record.toString() : jsonString(value);
        return Pair.of(ofskvKey, content);
    }

    static String ofsRelationalTableName(final Schema schema, final String nodeName, final String nodeType, final Configuration configuration, final String tableNameTemplateName) {
        String tableName;
        String recordSchemaId = schema.getFullName();
        if (tableNameTemplateName == null) {
            tableName = defaultTableName(schema);
        } else {
            tableName = TABLE_NAME_MAP.computeIfAbsent(recordSchemaId + "~~" + tableNameTemplateName, name -> {
                try {
                    return evaluateFreemarker(configuration, tableNameTemplateName, null, null, schema);
                } catch (Exception e) {
                    LOGGER.warn("Could not derive table name. Switching to default table name", Map.of(SCHEMA_ID, recordSchemaId, NODE_NAME, nodeName, NODE_TYPE, nodeType), e);
                    return defaultTableName(schema);
                }
            });
        }
        return tableName;
    }

    static String createRelationalTableWithIndex(final String key, final Schema schema, final String cacheName, final String nodeName, final String nodeType, final String baseUri, final Configuration configuration, final String tableNameTemplateName, final Map<String, Set<JdbcIndexOptions>> tableIndexes) {
        String tableName = ofsRelationalTableName(schema, nodeName, nodeType, configuration, tableNameTemplateName);
        String recordSchemaId = schema.getFullName();

        if (!TABLE_CREATE_STATUS_MAP.computeIfAbsent(recordSchemaId, s -> createRelationalTable(schema, tableName, cacheName, nodeName, nodeType, baseUri))) {
            if (createRelationalTable(schema, tableName, cacheName, nodeName, nodeType, baseUri)) {
                TABLE_CREATE_STATUS_MAP.put(recordSchemaId, true);
            }
        }
        tableIndexes.getOrDefault(tableName, Set.of()).forEach(idx -> {
            if (!TABLE_INDEX_MAP.computeIfAbsent(tableName + "_" + idx.getName(), idxName -> createRelationalIndex(schema, tableName, cacheName, nodeName, nodeType, baseUri, idx))) {
                if (createRelationalIndex(schema, tableName, cacheName, nodeName, nodeType, baseUri, idx)) {
                    TABLE_INDEX_MAP.put(tableName + "_" + idx.getName(), true);
                }
            }
        });
        return tableName;
    }

    static Boolean createRelationalTable(final Schema schema, final String tableName, final String cacheName, final String nodeName, final String nodeType, final String baseUri) {
        String createSql = createTableStatement(cacheName, tableName, schema);
        try {
            LOGGER.debug("Sending create table statement to localOfs", Map.of("sql", createSql, "cacheName", cacheName, SCHEMA_ID, schema.getFullName(), NODE_NAME, nodeName));
            WebUtils.invokeRestApi(baseUri + "/cache/sql/createtable/" + cacheName + "/" + tableName, createSql, String.class);
            return true;
        } catch (Exception e) {
            LOGGER.error("Could not create relational table. Reason : " + e.getMessage(), Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "tableName", tableName, SCHEMA_ID, schema.getFullName(), "cacheName", cacheName, NODE_NAME, nodeName), e);
            return false;
        }
    }

    static Boolean createRelationalIndex(final Schema schema, final String tableName, final String cacheName, final String nodeName, final String nodeType, final String baseUri, final JdbcIndexOptions index) {
        String createSql = createIndexStatement(tableName, schema, index);
        try {
            LOGGER.debug("Sending create index statement to localOfs", Map.of("sql", createSql, "cacheName", cacheName, SCHEMA_ID, schema.getFullName(), NODE_NAME, nodeName, "indexName", index.getName()));
            WebUtils.invokeRestApi(baseUri + "/cache/sql/createindex/" + cacheName + "/" + tableName + "/" + index.getName(), createSql, String.class);
            return true;
        } catch (Exception e) {
            LOGGER.error("Could not create relational index. Reason : " + e.getMessage(), Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "tableName", tableName, SCHEMA_ID, schema.getFullName(), "cacheName", cacheName, NODE_NAME, nodeName, "indexName", index.getName()), e);
            return false;
        }
    }

    static String createIndexStatement(final String tableName, final Schema schema, final JdbcIndexOptions index) {
        return String.format("CREATE %s INDEX IF NOT EXISTS %s on %s (%s)", index.isUnique() ? "UNIQUE" : EMPTY_STRING, index.getName(), tableName, index.getColumns().stream().collect(Collectors.joining(",")));
    }

    static String defaultTableName(final Schema schema) {
        return schema.getName().replace('-', '_').toLowerCase();
    }

    @SuppressWarnings("unchecked")
    private Configuration configuration() {
        if (configuration != null) {
            return configuration;
        }
        if (this.options.getTableName() != null) {
            loadFreemarkerTemplate(tableNameTemplateName, this.options.getTableName());
        }
        if (this.options.getExpression() != null) {
            loadFreemarkerTemplate(expressionTemplateName, this.options.getExpression());
        }
        configuration = freeMarkerConfiguration();
        return configuration;
    }

    @StartBundle
    public void startBundle() throws Exception {
        if (client == null) init();
    }

    @ProcessElement
    @SuppressWarnings("unchecked")
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
        log(options, nodeName, nodeType, kv);
        counter(nodeName, nodeType, kv.getValue().getSchema().getFullName(), ELEMENTS_IN).inc();
        Response response = null;
        String responseString = null;
        int responseCode = -1;
        String url = null;
        String tableName = null;
        try {
            Configuration configuration = configuration();
            String content;
            if ("kv".equalsIgnoreCase(type)) {
                Pair<String, String> pair = ofsKVContent(kv, configuration, expressionTemplateName);
                content = pair.getValue();
                url = baseUri + "/cache/kv/update/" + cacheName + "/" + URLEncoder.encode(pair.getKey(), Charset.defaultCharset());
            } else {
                GenericRecord record = kv.getValue();
                String key = kv.getKey().replaceAll("'", "~");
                Schema schema = record.getSchema();
                tableName = createRelationalTableWithIndex(key, schema, cacheName, nodeName, nodeType, baseUri, configuration, tableNameTemplateName, indexes);
                if (options.isPreferRaw()) {
                    url = baseUri + "/cache/sql/pojos/" + cacheName;
                    content = jsonString(Arrays.asList(Map.of("tableName", tableName, "dmlType", "MERGE", SCHEMA_ID, schema.getFullName(), "primaryKey", key, "data", record.toString())));
                } else if (options.isPreferMerge()) {
                    url = baseUri + "/cache/sql/dml/" + cacheName;
                    content = dmlStatement(key, record, tableName, schema, "MERGE", options.getDeleteAttributeName());
                } else {
                    url = baseUri + "/cache/sql/dmls/" + cacheName;
                    String deleteSql = String.format("delete from %s where %s = '%s'", tableName, PRIMARY_KEY_NAME, key);
                    String insertSql = dmlStatement(key, record, tableName, schema, "INSERT", options.getDeleteAttributeName());
                    content = jsonString(Arrays.asList(deleteSql, insertSql));
                }
            }
            RequestBody body = RequestBody.create(content, MEDIA_TYPE_APPLICATION_JSON);
            Request request = new Request.Builder().url(url).post(body).build();
            response = client.newCall(request).execute();
            responseCode = response.code();
            String responseMessage = response.message();
            ResponseBody responseBody = response.body();
            responseString = responseBody == null ? null : responseBody.string();
            if (!response.isSuccessful()) {
                throw new Exception("Non 200-OK response received for POST http request for url " + url + ". Response Code : " + responseCode + ". Response message : " + responseMessage);
            }
        } catch (Exception e) {
            counter(nodeName, nodeType, kv.getValue().getSchema().getFullName(), ELEMENTS_ERROR).inc();
            LOGGER.warn("Error saving to local ofs", Map.of("url", url, "responseCode", responseCode, "responseString", dropSensitiveKeys(responseString, true), "key", kv.getKey(), "type", type, "cacheName", cacheName, NODE_NAME, nodeName), e);
            ctx.output(ERROR_TAG, eRecord(nodeName, nodeType, "ofs_url_invoke", kv, e));
            return;
        } finally {
            if (response != null) response.close();
        }
        if (emit) {
            if ("kv".equalsIgnoreCase(type)) {
                incCounters(nodeName, nodeType, kv.getValue().getSchema().getFullName(), ELEMENTS_OUT, SOURCE_SCHEMA_ID, kv.getValue().getSchema().getFullName(), CACHE_NAME, cacheName);
            } else {
                incCounters(nodeName, nodeType, kv.getValue().getSchema().getFullName(), ELEMENTS_OUT, SOURCE_SCHEMA_ID, kv.getValue().getSchema().getFullName(), (tableName != null ? TABLE_NAME : URL), (tableName != null ? tableName : baseUri));
            }
            ctx.output(kv);
        }
    }
}
