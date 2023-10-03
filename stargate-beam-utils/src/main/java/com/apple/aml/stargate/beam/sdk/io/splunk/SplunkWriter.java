package com.apple.aml.stargate.beam.sdk.io.splunk;

import com.apple.aml.stargate.beam.sdk.io.file.AbstractWriter;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.options.SplunkOptions;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import okhttp3.Headers;
import okhttp3.HttpUrl;
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
import org.joda.time.Instant;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static com.apple.aml.stargate.beam.sdk.io.http.HttpInvoker.getHttpResponseInternalSchema;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.MEDIA_TYPE_APPLICATION_JSON;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.nodes.StargateNode.nodeName;
import static com.apple.aml.stargate.common.utils.JsonUtils.dropSensitiveKeys;
import static com.apple.aml.stargate.common.utils.JsonUtils.fastJsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.WebUtils.newOkHttpClient;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.gauge;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramBytes;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramSize;

public class SplunkWriter<Input> extends AbstractWriter<Input, Void> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final String HEC_URL_PATH = "/services/collector/event";
    //    MediaType SPLUNK_MEDIA_TYPE = MediaType.get("application/json;profile=urn:splunk:event:1.0;charset=utf-8");
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final String hecUri;
    private final String hecToken;
    private final SplunkOptions options;
    private final boolean emitSuccessRecords;
    private final boolean emitHecErrors;
    private final String nodeName;
    private final String nodeType;
    private final ObjectToGenericRecordConverter converter;
    private final String schemaReference;
    private final String schemaId;
    private final Schema schema;
    private final SplunkEventTransformer transformer;
    protected transient Histogram histogram;
    protected transient Gauge gauge;
    private transient OkHttpClient okHttpClient;

    public SplunkWriter(final String nodeName, final String nodeType, final PipelineConstants.ENVIRONMENT env, final SplunkOptions options, final boolean emitSuccessRecords, final boolean emitHecErrors) throws Exception {
        this.options = options;
        this.hecUri = options.getUrl().trim() + HEC_URL_PATH;
        this.hecToken = String.format("Splunk %s", options.getToken());
        this.emitSuccessRecords = emitSuccessRecords;
        this.emitHecErrors = emitHecErrors;
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        transformer = new SplunkEventTransformer(nodeName(nodeName, "transform"), nodeType, options);
        if (this.options.getSchemaId() == null) {
            this.schema = getHttpResponseInternalSchema(env);
            this.schemaId = this.schema.getFullName();
            this.schemaReference = null;
        } else {
            this.schemaReference = this.options.getSchemaReference();
            this.schemaId = this.options.getSchemaId();
            this.schema = fetchSchemaWithLocalFallback(this.schemaReference, schemaId);
        }
        this.converter = converter(this.schema);
        this.initClient();
    }

    private OkHttpClient initClient() throws Exception {
        if (okHttpClient == null) okHttpClient = newOkHttpClient(options.getConnectionOptions());
        return okHttpClient;
    }

    @Override
    protected void setup() throws Exception {
        initClient();
    }

    @Override
    protected KV<String, GenericRecord> process(final KV<String, GenericRecord> kv, final Void batch, final WindowedContext context, final PaneInfo paneInfo, final BoundedWindow window, final Instant timestamp) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean enableRawConsumption() {
        return true;
    }

    @Override
    protected int consumeRaw(final List<KV<String, GenericRecord>> iterable, final Void ignored, final WindowedContext context, final PaneInfo paneInfo, final BoundedWindow window, final Instant timestamp) throws Exception {
        AtomicInteger atomicInteger = new AtomicInteger();
        Response response = null;
        String responseString = null;
        int responseCode = -1;
        String url = hecUri;
        String content;
        StringBuilder builder = new StringBuilder();
        List<KV<String, GenericRecord>> successRecords = new ArrayList<>();
        Map<String, Object> output = new HashMap<>();
        output.put("status", false);
        output.put("url", url);
        long startTime = System.nanoTime();
        OkHttpClient client;
        try {
            client = initClient();
            iterable.forEach(kv -> {
                long loopStartTime = System.nanoTime();
                Map se = null;
                String schemaId = kv.getValue().getSchema().getFullName();
                try {
                    counter(nodeName, nodeType, schemaId, ELEMENTS_IN).inc();
                    se = transformer.fetchSplunkEventMap(kv);
                    String jsonString = fastJsonString(se);
                    histogramSize(nodeName, nodeType, schemaId, "splunk_event_length").observe(jsonString.length());
                    builder.append("\n").append(jsonString);
                    atomicInteger.incrementAndGet();
                    successRecords.add(kv);
                } catch (Exception e) {
                    counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
                    counter(nodeName, nodeType, schemaId, "conversion_error").inc();
                    context.output(ERROR_TAG, eRecord(nodeName, nodeType, "event_conversion", kv, e));
                    LOGGER.warn("Error in converting to splunk event", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), NODE_NAME, nodeName, NODE_TYPE, nodeType, "key", kv.getKey(), SCHEMA_ID, schemaId), logEventInfo(se), e);
                } finally {
                    histogramDuration(nodeName, nodeType, schemaId, "splunk_event_prep").observe((System.nanoTime() - loopStartTime) / 1000000.0);
                }
            });
            histogramDuration(nodeName, nodeType, UNKNOWN, "hec_content_prep").observe((System.nanoTime() - startTime) / 1000000.0);
            if (atomicInteger.get() == 0) return 0;
            content = builder.substring(1);
            int contentBytesSize = content.getBytes().length;
            histogramBytes(nodeName, nodeType, UNKNOWN, "hec_content_size").observe(contentBytesSize);
            gauge(nodeName, nodeType, UNKNOWN, "hec_content_size").set(contentBytesSize);
            output.put("payload", content);
            Map<String, String> optionsHeaders = new HashMap<>();
            options.getHeaders().forEach((k, v) -> optionsHeaders.put(k, (String) v));
            optionsHeaders.put("Authorization", hecToken);
            Headers reqHeaders = Headers.of(optionsHeaders);
            RequestBody body = RequestBody.create(content, MEDIA_TYPE_APPLICATION_JSON); // TODO : Need to support non-json splunk native
            Request request = new Request.Builder().url(HttpUrl.get(url)).headers(reqHeaders).post(body).build();
            startTime = System.nanoTime();
            response = client.newCall(request).execute();
            histogramDuration(nodeName, nodeType, UNKNOWN, "hec_success_time").observe((System.nanoTime() - startTime) / 1000000.0);
            responseCode = response.code();
            output.put("code", responseCode);
            String responseMessage = response.message();
            output.put("message", responseMessage);
            ResponseBody responseBody = response.body();
            responseString = responseBody == null ? null : responseBody.string();
            output.put("body", responseString);
            gauge(nodeName, nodeType, UNKNOWN, "hec_response_code").set(responseCode);
            if (!response.isSuccessful()) {
                throw new Exception("Non 200-OK response received from Splunk HEC Url : " + url + ". Response Code : " + responseCode + ". Response message : " + responseMessage);
            }
            output.put("duration", (System.nanoTime() - startTime) / 1000000.0);
            successRecords.stream().forEach(kv -> {
                if (emitSuccessRecords) context.output(kv);
                counter(nodeName, nodeType, kv.getValue().getSchema().getFullName(), "process").inc();
            });
        } catch (Exception e) {
            double duration = (System.nanoTime() - startTime) / 1000000.0;
            if (e instanceof SocketTimeoutException || e instanceof SocketException) {
                okHttpClient = null;
            }
            histogramDuration(nodeName, nodeType, UNKNOWN, "hec_error_time").observe(duration);
            output.put("duration", duration);
            LOGGER.warn("Error posting batch to splunk hec endpoint. Ignoring this error!", Map.of("url", url, "responseCode", responseCode, "responseString", dropSensitiveKeys(responseString, true), NODE_NAME, nodeName), e);
            successRecords.stream().forEach(kv -> {
                counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
                context.output(ERROR_TAG, eRecord(nodeName, nodeType, "splunk_publish", kv, e));
            });
            if (emitHecErrors) {
                if (responseString != null) {
                    responseString = responseString.stripLeading();
                    if (options.isParse() && (responseString.startsWith("{") || responseString.startsWith("["))) {
                        try {
                            output.put("json", readJsonMap(responseString));
                        } catch (Exception je) {
                            LOGGER.warn("Could not parse response as json", Map.of("url", url, "responseCode", responseCode, NODE_NAME, nodeName, NODE_TYPE, nodeType), je);
                            output.put("json", null);
                        }
                    }
                    final Map<String, String> headers = new HashMap<>();
                    response.headers().iterator().forEachRemaining(p -> headers.put(p.getFirst(), p.getSecond()));
                    output.put("headers", headers);
                }
                GenericRecord errorRecord = converter.convert(output);
                context.output(KV.of(UUID.randomUUID().toString(), errorRecord));
            }
        } finally {
            if (response != null) {
                response.close();
            }
        }
        return atomicInteger.get();
    }

    public static Map<String, String> logEventInfo(final Map se) {
        return se == null ? Map.of() : Map.of("eventIndex", String.valueOf(se.get("index")), "eventHost", String.valueOf(se.get("host")), "eventSource", String.valueOf(se.get("source")), "eventSourceType", String.valueOf(se.get("sourceType")), "eventTime", String.valueOf(se.get("time")));
    }

    @Override
    public void closeBatch(final Void batch, final WindowedContext context, final PipelineConstants.BATCH_WRITER_CLOSE_TYPE batchType) throws Exception {

    }

    @Override
    protected Void initBatch() throws Exception {
        initClient();
        return null;
    }
}
