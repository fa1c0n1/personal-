package com.apple.aml.stargate.beam.sdk.io.http;

import com.apple.aml.stargate.beam.sdk.io.file.AbstractWriter;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.JdbcIndexOptions;
import com.apple.aml.stargate.common.options.LocalOfsOptions;
import freemarker.template.Configuration;
import io.prometheus.client.Histogram;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.Instant;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.apple.aml.stargate.beam.sdk.io.http.LocalOfsInvoker.createRelationalTableWithIndex;
import static com.apple.aml.stargate.beam.sdk.io.http.LocalOfsInvoker.ofsKVContent;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.MEDIA_TYPE_APPLICATION_JSON;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.TABLE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.options.DMLOperation.dmlStatement;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.JsonUtils.dropSensitiveKeys;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.WebUtils.getHttpClient;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eDynamicRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;

public class LocalOfsWriter<Input> extends AbstractWriter<Input, Void> implements Serializable {
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
    protected transient Histogram histogram;
    private transient OkHttpClient client;
    private transient Configuration configuration;
    private StringBuilder builder;
    private List<Object> objects;
    private List<String> keys;

    public LocalOfsWriter(final StargateNode node, final LocalOfsOptions options, final Map<String, Set<JdbcIndexOptions>> indexes, final boolean emit) throws Exception {
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
        this.initClient();
    }

    private void initClient() throws Exception {
        if (client == null) {
            client = options.isDisableCertificateValidation() ? getHttpClient() : new OkHttpClient();
        }
    }

    @Override
    protected void setup() throws Exception {
        configuration();
        initClient();
    }

    @Override
    protected KV<String, GenericRecord> process(final KV<String, GenericRecord> kv, final Void batch, final WindowedContext context, final PaneInfo paneInfo, final BoundedWindow window, final Instant timestamp) throws Exception {
        long startTime = System.nanoTime();
        GenericRecord record = kv.getValue();
        Schema schema = record.getSchema();
        String schemaId = schema.getFullName();
        try {
            counter(nodeName, nodeType, schemaId, ELEMENTS_IN).inc();
            keys.add(kv.getKey());
            if ("kv".equalsIgnoreCase(type)) {
                Pair<String, String> pair = ofsKVContent(kv, configuration, expressionTemplateName);
                builder.append(",\"").append(pair.getKey().replaceAll("\"", "\\\"")).append("\":").append(pair.getValue());
                incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, SOURCE_SCHEMA_ID, schemaId);
            } else {
                String key = kv.getKey().replaceAll("'", "~");
                String tableName = createRelationalTableWithIndex(key, schema, cacheName, nodeName, nodeType, baseUri, configuration, tableNameTemplateName, indexes);
                if (options.isPreferRaw()) {
                    objects.add(Map.of("tableName", tableName, "dmlType", "MERGE", SCHEMA_ID, schemaId, "primaryKey", key, "data", record.toString()));
                } else {
                    objects.add(dmlStatement(key, record, tableName, schema, "MERGE", options.getDeleteAttributeName()));
                }
                incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, TABLE_NAME, tableName, SOURCE_SCHEMA_ID, schemaId);
            }

            return emit ? kv : null;
        } finally {
            histogramDuration(nodeName, nodeType, schemaId, "process").observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

    @Override
    public void closeBatch(final Void batch, final WindowedContext context, final PipelineConstants.BATCH_WRITER_CLOSE_TYPE batchType) throws Exception {
        if (keys == null || keys.isEmpty()) {
            builder = null;
            objects = null;
            keys = null;
            return;
        }
        Response response = null;
        String responseString = null;
        int responseCode = -1;
        String url = UNKNOWN;
        String content = null;
        try {
            if ("kv".equalsIgnoreCase(type)) {
                url = baseUri + "/cache/kv/updates/" + cacheName;
                content = "{" + builder.substring(1) + "}";
            } else if (options.isPreferRaw()) {
                url = baseUri + "/cache/sql/pojos/" + cacheName;
                content = jsonString(objects);
            } else {
                url = baseUri + "/cache/sql/dmls/" + cacheName;
                content = jsonString(objects);
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
            counter(nodeName, nodeType, UNKNOWN, ELEMENTS_ERROR).inc();
            LOGGER.warn("Error saving batch to local ofs", Map.of("url", url, "responseCode", responseCode, "responseString", dropSensitiveKeys(responseString, true), "type", type, "keys", keys, "cacheName", cacheName, NODE_NAME, nodeName), e);
            if (context == null) throw e;
            context.output(ERROR_TAG, eDynamicRecord(nodeName, nodeType, "ofs_url_invoke", KV.of(url, content), e));
        } finally {
            builder = null;
            objects = null;
            keys = null;
            if (response != null) {
                response.close();
            }
        }
    }

    @Override
    protected Void initBatch() throws Exception {
        keys = new ArrayList<>();
        if ("kv".equalsIgnoreCase(type)) {
            builder = new StringBuilder();
        } else {
            objects = new ArrayList<>();
        }
        configuration();
        initClient();
        return null;
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
}
