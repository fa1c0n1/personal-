package com.apple.aml.stargate.beam.sdk.ts;

import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.SPELOptions;
import com.apple.aml.stargate.common.utils.accessors.GenericRecordAccessor;
import com.apple.aml.stargate.common.utils.accessors.MapAccessor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.beam.sdk.ts.JavaFunction.emitOutput;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.KEY;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.RECORD;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.SCHEMA;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_NULL;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.HoconUtils.hoconToPojo;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.JsonUtils.yamlToPojo;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.ALIAS_MAP;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class SPELEvaluator extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private SPELOptions options;
    private String expression;
    private String nodeName;
    private String nodeType;
    private transient Expression expressionObject;
    private Schema schema;
    private ObjectToGenericRecordConverter converter;
    private ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap = new ConcurrentHashMap<>();
    private PipelineConstants.DATA_FORMAT outputType;
    private Class outputClass;
    private Function<String, Object> function;

    @SuppressWarnings("unchecked")
    public void initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        SPELOptions options = (SPELOptions) node.getConfig();
        this.options = options;
        this.expression = options.expression();
        this.nodeName = node.getName();
        this.nodeType = node.getType();
        PipelineConstants.ENVIRONMENT environment = node.environment();
        if (this.options.getSchemaId() != null) {
            this.schema = fetchSchemaWithLocalFallback(this.options.getSchemaReference(), this.options.getSchemaId());
            this.converter = converter(this.schema);
            LOGGER.debug("Java filter extractor created successfully for", Map.of(SCHEMA_ID, this.options.getSchemaId(), NODE_NAME, nodeName, NODE_TYPE, nodeType));
        }
        this.outputType = isBlank(options.getType()) ? PipelineConstants.DATA_FORMAT.json : PipelineConstants.DATA_FORMAT.valueOf(ALIAS_MAP.getOrDefault(options.getType(), "json"));
        this.outputClass = (isBlank(options.getClassName())) ? Map.class : Class.forName(this.options.getClassName().trim());
        this.function();
        this.expression();
    }

    @SuppressWarnings("unchecked")
    private Function<String, Object> function() {
        if (this.function != null) {
            return this.function;
        }
        if (this.outputType == PipelineConstants.DATA_FORMAT.json) {
            this.function = s -> {
                try {
                    return readJson(s, outputClass);
                } catch (Exception e) {
                    throw new GenericException("Error reading json", Map.of("json", String.valueOf(s), NODE_NAME, nodeName, NODE_TYPE, nodeType), e).wrap();
                }
            };
        } else if (this.outputType == PipelineConstants.DATA_FORMAT.yml) {
            this.function = s -> {
                try {
                    return yamlToPojo(s, outputClass);
                } catch (Exception e) {
                    throw new GenericException("Error reading yaml", Map.of("yaml", String.valueOf(s), NODE_NAME, nodeName, NODE_TYPE, nodeType), e).wrap();
                }
            };
        } else if (this.outputType == PipelineConstants.DATA_FORMAT.conf) {
            this.function = s -> {
                try {
                    return hoconToPojo(s, outputClass);
                } catch (Exception e) {
                    throw new GenericException("Error reading hocon", Map.of("hocon", String.valueOf(s), NODE_NAME, nodeName, NODE_TYPE, nodeType), e).wrap();
                }
            };
        } else {
            this.function = s -> {
                try {
                    return readJson(s, outputClass);
                } catch (Exception e) {
                    throw new GenericException("Error reading json", Map.of("json", String.valueOf(s), NODE_NAME, nodeName, NODE_TYPE, nodeType), e).wrap();
                }
            };
        }
        return this.function;
    }

    @SuppressWarnings("unchecked")
    private Expression expression() throws Exception {
        if (expressionObject != null) {
            return expressionObject;
        }
        expressionObject = (new SpelExpressionParser()).parseExpression(this.expression);
        return expressionObject;
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> inputCollection) throws Exception {
        return inputCollection.apply(node.getName(), this);
    }

    @ProcessElement
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
        log(options, nodeName, nodeType, kv);
        GenericRecord record = kv.getValue();
        Schema schema = record.getSchema();
        String recordSchemaId = schema.getFullName();
        counter(nodeName, nodeType, recordSchemaId, ELEMENTS_IN).inc();
        final Map<String, String> schemaMap = new HashMap<>();
        int i = 0;
        String fullName = schema.getFullName();
        for (final String token : fullName.split("\\.")) {
            schemaMap.put("token" + (i++), token);
        }
        schemaMap.put("name", schema.getName().replace('-', '_').toLowerCase());
        schemaMap.put("namespace", schema.getNamespace());
        schemaMap.put("fullName", fullName);
        Map<String, Object> object = Map.of(RECORD, record == null ? Map.of() : record, SCHEMA, schemaMap, KEY, String.valueOf(kv.getKey()));
        StandardEvaluationContext context = new StandardEvaluationContext(object);
        context.addPropertyAccessor(new GenericRecordAccessor());
        context.addPropertyAccessor(new MapAccessor());
        String outputString = expression().getValue(context, String.class);
        Object response = function().apply(outputString);
        if (response == null) {
            counter(nodeName, nodeType, recordSchemaId, ELEMENTS_NULL).inc();
            LOGGER.debug("SPEL Evaluator returned null. Will skip this record", Map.of("className", outputClass.getName(), SCHEMA_ID, recordSchemaId, NODE_NAME, nodeName, NODE_TYPE, nodeType));
            return;
        }
        emitOutput(kv, null, response, ctx, schema, recordSchemaId, this.options.getSchemaId(), this.converter, converterMap, nodeName, nodeType);
    }
}
