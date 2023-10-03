package com.apple.aml.stargate.connector.athena.attributes;

import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.AttributesOptions;
import freemarker.template.Configuration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.beam.sdk.ts.JavaFunction.emitOutput;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.AvroUtils.getFieldValue;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.connector.athena.attributes.AthenaAttributeService.attributeService;
import static com.apple.aml.stargate.connector.athena.attributes.AttributesIO.getGenericAttributesSchema;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class AttributesWriter extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final String templateName;
    private final boolean emit;
    private final AttributesOptions options;
    private final ObjectToGenericRecordConverter converter;
    private final String schemaReference;
    private final String schemaId;
    private final Schema schema;
    final private String keyName;
    final private String valueName;
    private String nodeName;
    private String nodeType;
    private ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap = new ConcurrentHashMap<>();
    private transient Configuration configuration;

    public AttributesWriter(final StargateNode node, final AttributesOptions options, final boolean emit) {
        this.nodeName = node.getName();
        this.nodeType = node.getType();
        this.options = options;
        this.keyName = this.options.getKeyAttribute() == null ? "key" : this.options.getKeyAttribute();
        this.valueName = this.options.getValueAttribute() == null ? "value" : this.options.getValueAttribute();
        this.emit = emit;
        if (this.emit) {
            if (isBlank(this.options.getSchemaId())) {
                this.schema = getGenericAttributesSchema(node.environment());
                this.schemaId = this.schema.getFullName();
                this.schemaReference = null;
            } else {
                this.schemaReference = this.options.getSchemaReference();
                this.schemaId = this.options.getSchemaId();
                this.schema = fetchSchemaWithLocalFallback(this.schemaReference, schemaId);
            }
            this.converter = converter(this.schema);
            LOGGER.debug("Converter created successfully for", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName));
        } else {
            this.converter = null;
            this.schemaReference = null;
            this.schemaId = null;
            this.schema = null;
        }
        this.templateName = nodeName + "~template";
    }

    @SuppressWarnings("unchecked")
    @ProcessElement
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
        log(options, nodeName, nodeType, kv);
        GenericRecord record = kv.getValue();
        Schema schema = record.getSchema();
        String recordSchemaId = schema.getFullName();
        counter(nodeName, nodeType, recordSchemaId, ELEMENTS_IN).inc();
        String lookupKey;
        Object lookupValue;
        Integer ttl;
        AthenaAttributeService client = attributeService(nodeName, options);
        if (options.getExpression() == null) {
            String lookupName = options.getLookupNameAttribute() == null ? options.getLookupName() : (String) getFieldValue(record, options.getLookupNameAttribute());
            lookupKey = (String) getFieldValue(record, keyName);
            if (lookupName != null) lookupKey = lookupName + ":" + lookupKey;
            lookupValue = getFieldValue(record, valueName);
            ttl = options.getTtlAttribute() == null ? options.getTtl() : (Integer) getFieldValue(record, options.getTtlAttribute());
        } else {
            String outputString = options.isEnableDirectAccess() ? evaluateFreemarker(configuration(), templateName, kv.getKey(), kv.getValue(), schema, "client", client) : evaluateFreemarker(configuration(), templateName, kv.getKey(), kv.getValue(), schema);
            Map<Object, Object> map = readJsonMap(outputString);
            String lookupName = options.getLookupNameAttribute() == null ? options.getLookupName() : (String) map.get(options.getLookupNameAttribute());
            lookupKey = (String) map.get(keyName);
            if (lookupName != null) lookupKey = lookupName + ":" + lookupKey;
            lookupValue = map.get(valueName);
            ttl = options.getTtlAttribute() == null ? options.getTtl() : (Integer) map.get(options.getTtlAttribute());
        }
        try {
            long attributesWriteStartTime = System.nanoTime();
            boolean status = client.writeFuture(lookupKey, lookupValue, ttl == null ? -1 : ttl.intValue()).get();
            histogramDuration(nodeName, nodeType, recordSchemaId, "attributes_write").observe((System.nanoTime() - attributesWriteStartTime) / 1000000.0);
            if (!status) throw new Exception("Attributes api returned status as false");
            if (!emit) return;
            Map returnValue = Map.of("key", lookupKey, "value", Map.of("status", String.valueOf(status)));
            GenericRecord response = converter.convert(returnValue);
            emitOutput(kv, options.isUseLookupKey() ? lookupKey : null, response, ctx, schema, recordSchemaId, this.schemaId, this.converter, converterMap, nodeName, nodeType);
        } catch (Exception e) {
            counter(nodeName, nodeType, recordSchemaId, ELEMENTS_ERROR).inc();
            LOGGER.warn("Could not write/save ofs entry", Map.of("key", lookupKey, ERROR_MESSAGE, String.valueOf(e.getMessage()), "ttl", String.valueOf(ttl)), e);
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
}
