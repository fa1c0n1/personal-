package com.apple.aml.stargate.beam.sdk.io.http;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.HttpInvokerOptions;
import com.apple.aml.stargate.common.pojo.RestResponse;
import com.apple.aml.stargate.common.web.clients.RestClient;
import freemarker.template.Configuration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.beam.sdk.ts.JavaFunction.emitOutput;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.URL;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.getInternalSchema;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;

public class HttpInvoker extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final long serialVersionUID = 1L;
    private final String nodeName;
    private final String nodeType;
    private final String urlTemplateName;
    private final String bodyTemplateName;
    private final HttpInvokerOptions options;
    private final boolean emit;
    private final Schema schema;
    private final String schemaId;
    private final ObjectToGenericRecordConverter converter;
    private final ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap = new ConcurrentHashMap<>();
    private transient RestClient restClient;
    private transient Configuration configuration;

    public HttpInvoker(final StargateNode node, final HttpInvokerOptions options, final boolean emit) throws Exception {
        this.options = options;
        this.emit = emit;
        nodeName = node.getName();
        nodeType = node.getType();
        this.schema = getHttpResponseInternalSchema(node.environment());
        this.schemaId = this.schema.getFullName();
        this.converter = ObjectToGenericRecordConverter.converter(this.schema);
        urlTemplateName = nodeName + "~URI";
        bodyTemplateName = nodeName + "~BODY";
        if (options.getConnectionOptions() != null) LOGGER.debug("Http connection options specified", readJsonMap(jsonString(options.getConnectionOptions())));
        this.initClient();
    }

    public static Schema getHttpResponseInternalSchema(PipelineConstants.ENVIRONMENT environment) {
        return getInternalSchema(environment, "httpresponse.avsc", "{\"type\":\"record\",\"name\":\"HttpResponse\",\"namespace\":\"com.apple.aml.stargate.#{ENV}.internal\",\"fields\":[{\"name\":\"status\",\"type\":\"boolean\"},{\"name\":\"url\",\"type\":[\"null\",\"string\"]},{\"name\":\"code\",\"type\":[\"null\",\"int\"]},{\"name\":\"message\",\"type\":[\"null\",\"string\"]},{\"name\":\"body\",\"type\":[\"null\",\"string\"]},{\"name\":\"json\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"string\"}]},{\"name\":\"headers\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"string\"}]},{\"name\":\"duration\",\"type\":[\"null\",\"double\"]},{\"name\":\"payload\",\"type\":[\"null\",\"string\"]}]}");
    }

    private RestClient initClient() throws Exception {
        if (restClient == null) restClient = new RestClient(options, LOGGER, Map.of(NODE_NAME, nodeName));
        return restClient;
    }

    @StartBundle
    public void startBundle() throws Exception {
        initClient();
    }

    @ProcessElement
    @SuppressWarnings("unchecked")
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
        long startTime = System.nanoTime();
        log(options, nodeName, nodeType, kv);
        Map<String, Object> output = new HashMap<>();
        output.put("status", false);
        RestClient client;
        String key = kv.getKey();
        GenericRecord record = kv.getValue();
        Schema recordSchema = record.getSchema();
        try {
            client = initClient();
            counter(nodeName, nodeType, recordSchema.getFullName(), ELEMENTS_IN).inc();
            String url = evaluateFreemarker(configuration(), urlTemplateName, key, record, recordSchema).trim();
            if (options.getBaseUri() != null) url = options.getBaseUri() + url;
            output.put("url", url);
            String content = options.getMethod() == null || "post".equalsIgnoreCase(options.getMethod()) || "put".equalsIgnoreCase(options.getMethod()) || "patch".equalsIgnoreCase(options.getMethod()) ? evaluateFreemarker(configuration(), bodyTemplateName, key, record, recordSchema).trim() : null;
            startTime = System.nanoTime();
            RestResponse response = client.httpInvoke(url, content);
            int responseCode = response.getCode();
            output.put("code", responseCode);
            output.put("message", response.getMessage());
            byte[] responseBytes = response.getContent();
            String responseString = responseBytes == null ? null : new String(responseBytes);
            output.put("body", responseString);
            output.put("headers", response.getHeaders());
            double timeTaken = (System.nanoTime() - startTime) / 1000000.0;
            output.put("duration", timeTaken);
            histogramDuration(nodeName, nodeType, schemaId, "process").observe(timeTaken);
            if (responseString != null && emit) {
                responseString = responseString.stripLeading();
                if (options.isParse() && (responseString.startsWith("{") || responseString.startsWith("["))) {
                    try {
                        output.put("json", readJsonMap(responseString));
                    } catch (Exception je) {
                        LOGGER.warn("Could not parse response as json", Map.of("url", url, "responseCode", responseCode, NODE_NAME, nodeName), je);
                        output.put("json", null);
                    }
                }
            }
        } catch (Exception e) {
            double duration = (System.nanoTime() - startTime) / 1000000.0;
            counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
            histogramDuration(nodeName, nodeType, schemaId, "process_error").observe(duration);
            ctx.output(ERROR_TAG, eRecord(nodeName, nodeType, "http_invoke", kv, e));
            return;
        }
        if (emit) {
            emitOutput(kv, null, output, ctx, schema, schemaId, this.schemaId, this.converter, converterMap, nodeName, nodeType);
        } else {
            incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, URL, String.valueOf(options.getBaseUri()), SOURCE_SCHEMA_ID, recordSchema.getFullName());
        }
    }

    @SuppressWarnings("unchecked")
    private Configuration configuration() {
        if (configuration != null) {
            return configuration;
        }
        loadFreemarkerTemplate(urlTemplateName, this.options.getUri());
        if (this.options.getBody() != null) {
            loadFreemarkerTemplate(bodyTemplateName, this.options.getBody());
        }
        configuration = freeMarkerConfiguration();
        return configuration;
    }
}
