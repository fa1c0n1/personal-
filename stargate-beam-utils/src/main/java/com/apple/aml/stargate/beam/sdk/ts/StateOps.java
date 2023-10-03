package com.apple.aml.stargate.beam.sdk.ts;

import com.apple.aml.stargate.beam.sdk.options.StargateOptions;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.constants.PipelineConstants.ENVIRONMENT;
import com.apple.aml.stargate.common.converters.GenericRecordToMapConverter;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.StateOptions;
import freemarker.template.Configuration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.beam.sdk.ts.JavaFunction.emitOutput;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.pipelineId;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.JsonUtils.readNullableJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.jvm.commons.util.Strings.isBlank;

abstract public class StateOps extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    protected StateOptions options;
    protected String stateId;
    protected String preprocess;
    protected String callback;
    protected String expression;
    protected String evaluate;
    protected String schemaId;
    protected String schemaReference;
    protected Schema schema;
    protected ObjectToGenericRecordConverter converter;
    protected String nodeName;
    protected String nodeType;
    protected String pipelineId;
    protected String pipelineToken;
    protected transient Function<Map<String, Object>, Map<String, Object>> preFunction;
    protected transient Function<Map<String, Object>, Object> postFunction;
    protected String stateIdTemplateName;
    protected ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap = new ConcurrentHashMap<>();
    protected transient Configuration configuration;
    protected String evalTemplateName = null;
    private String expressionTemplateName = null;

    @SuppressWarnings("unchecked")
    public void initWrite(final Pipeline pipeline, final StargateNode node) throws Exception {
        initCommon(pipeline, (StateOptions) node.getConfig(), node.getName(), node.getType(), node.environment());
    }

    @SuppressWarnings("unchecked")
    public void initCommon(final Pipeline pipeline, final StateOptions inputOptions, final String nodeName, final String nodeType, final ENVIRONMENT environment) throws Exception {
        StateOptions options = inputOptions;
        if (options == null) {
            options = new StateOptions();
        }
        this.options = options;
        this.stateId = options.getStateId();
        this.preprocess = options.getPreprocess();
        this.callback = options.getCallback();
        this.expression = options.getExpression();
        this.evaluate = options.getEvaluate();
        this.schemaId = options.getSchemaId();
        this.schemaReference = options.getSchemaReference();
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.pipelineId = pipelineId();
        this.pipelineToken = isBlank(options.getPipelineToken()) ? pipeline.getOptions().as(StargateOptions.class).getPipelineToken() : options.getPipelineToken();
        this.preFunction();
        this.postFunction();
        if (this.schemaId != null) {
            this.schema = fetchSchemaWithLocalFallback(this.schemaReference, this.schemaId);
            this.converter = converter(this.schema);
            LOGGER.debug("Converter created successfully for", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName));
        }
        this.evalTemplateName = nodeName + "~EVAL";
        this.expressionTemplateName = nodeName + "~EXPRESSION";
        this.stateIdTemplateName = isBlank(stateId) ? null : nodeName + "~STATE_ID";
    }

    @SuppressWarnings("unchecked")
    private Function<Map<String, Object>, Map<String, Object>> preFunction() throws Exception {
        if (preFunction != null) {
            return preFunction;
        }
        if (this.preprocess == null) {
            return null;
        }
        LOGGER.debug("Creating preprocess instance of", Map.of(NODE_NAME, nodeName, "className", this.preprocess));
        preFunction = (Function<Map<String, Object>, Map<String, Object>>) Class.forName(this.preprocess).getDeclaredConstructor().newInstance();
        LOGGER.debug("Preprocess instance created successfully", Map.of(NODE_NAME, nodeName, "className", this.preprocess));
        return preFunction;
    }

    @SuppressWarnings("unchecked")
    private Function<Map<String, Object>, Object> postFunction() throws Exception {
        if (postFunction != null) {
            return postFunction;
        }
        if (this.callback == null) {
            return null;
        }
        LOGGER.debug("Creating callback instance of", Map.of(NODE_NAME, nodeName, "className", this.callback));
        postFunction = (Function<Map<String, Object>, Object>) Class.forName(this.callback).getDeclaredConstructor().newInstance();
        LOGGER.debug("Callback instance created successfully", Map.of(NODE_NAME, nodeName, "className", this.callback));
        return postFunction;
    }

    @SuppressWarnings("unchecked")
    public void initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        initCommon(pipeline, (StateOptions) node.getConfig(), node.getName(), node.getType(), node.environment());
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> inputCollection) throws Exception {
        return inputCollection.apply(node.getName(), this);
    }

    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> inputCollection) throws Exception {
        return inputCollection.apply(node.getName(), this);
    }

    @SuppressWarnings({"unchecked", "deprecation"})
    @ProcessElement
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
        log(options, nodeName, nodeType, kv);
        String key = kv.getKey();
        GenericRecord record = kv.getValue();
        Schema schema = record.getSchema();
        String recordSchemaId = schema.getFullName();
        counter(nodeName, nodeType, recordSchemaId, ELEMENTS_IN).inc();
        String id = stateIdTemplateName == null ? key : evaluateFreemarker(configuration(), stateIdTemplateName, key, record, schema);
        Map<String, Object> inputMap = GenericRecordToMapConverter.convert(record);
        Function<Map<String, Object>, Map<String, Object>> preFunction = preFunction();
        if (preFunction != null) {
            inputMap = preFunction.apply(inputMap);
            if (inputMap == null) {
                LOGGER.debug("Preprocess function returned null. Will skip this record", Map.of("className", this.preprocess, SCHEMA_ID, recordSchemaId, "key", key, NODE_NAME, nodeName));
                return;
            }
        }
        if (this.expression != null) {
            String outputString = evaluateFreemarker(configuration(), expressionTemplateName, key, inputMap, schema);
            inputMap = (Map) readNullableJsonMap(outputString);
            if (inputMap == null) {
                LOGGER.debug("Freemarker processing returned null. Will skip this record", Map.of(SCHEMA_ID, recordSchemaId, "stateId", id, "key", key, NODE_NAME, nodeName));
                return;
            }
        }
        Map status = process(pipelineId, pipelineToken, id, inputMap, nodeName, nodeType, kv);
        if (this.useProcessResponse()) {
            inputMap = status;
            if (inputMap == null) {
                LOGGER.debug("process function returned null. Will skip this record", Map.of(SCHEMA_ID, recordSchemaId, "key", key, "stateId", id, NODE_NAME, nodeName));
                return;
            }
        }
        Object response = inputMap;
        Function<Map<String, Object>, Object> postFunction = postFunction();
        if (postFunction != null) {
            response = postFunction.apply(inputMap);
            if (response == null) {
                LOGGER.debug("callback function returned null. Will skip this record", Map.of("className", this.callback, SCHEMA_ID, recordSchemaId, "key", key, "stateId", id, NODE_NAME, nodeName));
                return;
            }
        }
        emitOutput(kv, null, response, ctx, schema, recordSchemaId, this.schemaId, this.converter, converterMap, nodeName, nodeType);
    }

    @SuppressWarnings("unchecked")
    protected Configuration configuration() {
        if (configuration != null) {
            return configuration;
        }
        if (this.stateIdTemplateName != null) {
            loadFreemarkerTemplate(stateIdTemplateName, this.stateId);
        }
        if (this.expression != null) {
            loadFreemarkerTemplate(expressionTemplateName, this.expression);
        }
        if (this.evaluate != null) {
            loadFreemarkerTemplate(evalTemplateName, this.evaluate);
        }
        configuration = freeMarkerConfiguration();
        return configuration;
    }

    abstract public Map process(final String pipelineId, final String pipelineToken, final String stateId, final Map state, final String nodeName, final String nodeType, final KV<String, GenericRecord> kv) throws Exception;

    protected boolean useProcessResponse() {
        return false;
    }
}
