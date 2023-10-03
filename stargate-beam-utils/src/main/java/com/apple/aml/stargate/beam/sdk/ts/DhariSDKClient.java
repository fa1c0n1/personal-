package com.apple.aml.stargate.beam.sdk.ts;

import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.constants.A3Constants;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.DhariOptions;
import com.apple.aml.stargate.common.utils.A3CacheUtils;
import com.apple.aml.stargate.common.utils.AppConfig;
import freemarker.template.Configuration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.Map;

import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.utils.AppConfig.appId;
import static com.apple.aml.stargate.common.utils.AppConfig.environment;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;

public class DhariSDKClient extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private String nodeName;
    private String nodeType;
    private DhariOptions options;
    private transient Configuration configuration;
    private String payloadTemplateName;
    private Method ingestJsonMethod;

    @SuppressWarnings("rawtypes")
    public void initWrite(final Pipeline pipeline, final StargateNode node) throws Exception {
        initCommon(pipeline, node);
    }

    @SuppressWarnings("rawtypes")
    public void initCommon(final Pipeline pipeline, final StargateNode node) throws Exception {
        DhariOptions options = (DhariOptions) node.getConfig();
        this.options = options;
        this.nodeName = node.getName();
        this.nodeType = node.getType();
        this.payloadTemplateName = nodeName + "~payload";
        ensureMethod();
    }

    @SuppressWarnings("unchecked")
    private void ensureMethod() throws Exception {
        if (ingestJsonMethod == null) {
            Class clientClass = Class.forName("com.apple.aml.dataplatform.client.DhariClient");
            ingestJsonMethod = clientClass.getMethod("ingestJson", String.class, String.class, String.class, String.class);
        }
    }

    @SuppressWarnings("rawtypes")
    public void initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        initCommon(pipeline, node);
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> inputCollection) throws Exception {
        return inputCollection.apply(node.getName(), this);
    }

    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> inputCollection) throws Exception {
        return inputCollection.apply(node.getName(), this);
    }

    @Setup
    public void setup() throws Exception {
        ensureMethod();
        System.setProperty("aml.dhari.appId", String.valueOf(appId()));
        System.setProperty("aml.dhari.mode", AppConfig.mode());
        System.setProperty("aml.dhari.uri", this.options.getUri() == null ? environment().getConfig().getDhariUri() : this.options.getUri());
        if (options.isGrpc() || options.getGrpcEndpoint() != null) {
            System.setProperty("aml.dhari.comm.type", "GRPC");
            if (options.getGrpcEndpoint() != null) {
                System.setProperty("aml.dhari.grpc.endpoint", options.getGrpcEndpoint().trim());
            }
        }
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    @ProcessElement
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
        long startTime = System.nanoTime();
        log(options, nodeName, nodeType, kv);
        String key = kv.getKey();
        GenericRecord record = kv.getValue();
        Schema schema = record.getSchema();
        counter(nodeName, nodeType, schema.getFullName(), ELEMENTS_IN).inc();
        String schemaId;
        String jsonPayload;
        if (this.options.getPayload() == null) {
            jsonPayload = record.toString();
            schemaId = schema.getFullName();
        } else {
            jsonPayload = evaluateFreemarker(configuration(), payloadTemplateName, key, record, schema);
            schemaId = this.options.getSchemaId();
        }
        String token = this.options.getA3Token() == null ? A3CacheUtils.cachedA3Token(A3Constants.KNOWN_APP.DHARI.appId()) : this.options.getA3Token();
        try {
            ingestJsonMethod.invoke(null, token, options.getPublisherId(), schemaId, jsonPayload);
            histogramDuration(nodeName, nodeType, schemaId, "process_success").observe((System.nanoTime() - startTime) / 1000000.0);
        } catch (Exception e) {
            histogramDuration(nodeName, nodeType, schemaId, "process_error").observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.warn("Error invoking DhariClient sdk endpoint", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "key", key, NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, schemaId), e);
            counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
            ctx.output(ERROR_TAG, eRecord(nodeName, nodeType, "dhari_publish", kv, e));
            return;
        } finally {
            histogramDuration(nodeName, nodeType, schemaId, "process").observe((System.nanoTime() - startTime) / 1000000.0);
        }
        incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, SOURCE_SCHEMA_ID, schema.getFullName());
        ctx.output(kv);
    }

    private freemarker.template.Configuration configuration() {
        if (configuration != null) {
            return configuration;
        }
        if (this.options.getPayload() != null) {
            loadFreemarkerTemplate(payloadTemplateName, this.options.getPayload());
        }
        configuration = freeMarkerConfiguration();
        return configuration;
    }
}
