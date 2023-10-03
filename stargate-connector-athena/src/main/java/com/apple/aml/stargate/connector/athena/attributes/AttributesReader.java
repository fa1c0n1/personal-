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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.beam.sdk.ts.JavaFunction.emitOutput;
import static com.apple.aml.stargate.common.constants.CommonConstants.DEFAULT_DELIMITER;
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

public class AttributesReader extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final String templateName;
    private final AttributesOptions options;
    private final ObjectToGenericRecordConverter converter;
    private final String schemaReference;
    private final String schemaId;
    private final Schema schema;
    private String nodeName;
    private String nodeType;
    private ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap = new ConcurrentHashMap<>();
    private transient Configuration configuration;


    public AttributesReader(final StargateNode node, final AttributesOptions options) {
        this.nodeName = node.getName();
        this.nodeType = node.getType();
        this.options = options;
        if (this.options.getSchemaId() == null) {
            this.schema = getGenericAttributesSchema(node.environment());
            this.schemaId = this.schema.getFullName();
            this.schemaReference = null;
        } else {
            this.schemaReference = this.options.getSchemaReference();
            this.schemaId = this.options.getSchemaId();
            this.schema = fetchSchemaWithLocalFallback(this.schemaReference, schemaId);
        }
        this.converter = converter(this.schema);
        this.templateName = nodeName + "~template";
        LOGGER.debug("Converter created successfully for", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName));
    }

    @SuppressWarnings("unchecked")
    @ProcessElement
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
        log(options, nodeName, nodeType, kv);
        GenericRecord record = kv.getValue();
        Schema schema = record.getSchema();
        String recordSchemaId = schema.getFullName();
        counter(nodeName, nodeType, recordSchemaId, ELEMENTS_IN).inc();
        Collection<String> keys = null;

        if (options.getExpression() == null) {
            // if there's a keys attribute, get list of keys from the record. otherwise use static list of keys from options.
            // if you still have no keys, but you have a key attribute - get list of keys from record by splitting on delimiter
            keys = options.getKeysAttribute() == null ? options.getKeys() : (List<String>) getFieldValue(record, options.getKeysAttribute());
            if (keys == null && options.getKeyAttribute() != null) {
                keys = Arrays.asList(((String) getFieldValue(record, options.getKeyAttribute())).split(DEFAULT_DELIMITER));
            }

            // if there's a lookup name attribute, get lookup name from record. otherwise use static lookup name from options
            final String lookupName = options.getLookupNameAttribute() == null ? options.getLookupName() : (String) getFieldValue(record, options.getLookupNameAttribute());
            if (lookupName != null) {
                // if there's a lookup keys attribute, get list of lookup keys from record. otherwise use static list of lookup keys from options
                // if you still have no lookup keys, but you have a lookup key attribute - get list of lookup keys from record by splitting on delimter
                List<String> lookupKeys = options.getLookupKeysAttribute() == null ? options.getLookupKeys() : (List<String>) getFieldValue(record, options.getLookupKeysAttribute());
                if (lookupKeys == null && options.getLookupKeyAttribute() != null) {
                    lookupKeys = Arrays.asList(((String) getFieldValue(record, options.getLookupKeyAttribute())).split(DEFAULT_DELIMITER));
                }
                if (lookupKeys != null) {
                    final Set<String> allkeys = new HashSet<String>();
                    if (keys != null) {
                        allkeys.addAll(keys);
                    }
                    for (String lk : lookupKeys) {
                        allkeys.add(lookupName + ":" + lk);
                    }
                    keys = allkeys;
                }
            }
        } else {
            String outputString = evaluateFreemarker(configuration(), templateName, kv.getKey(), kv.getValue(), schema);
            Map<Object, Object> map = readJsonMap(outputString);
            // if there's a keys attribute, get list of keys from the map. otherwise use static list of keys from options.
            // if you still have no keys, but you have a key attribute - get list of keys from map by splitting on delimiter
            keys = options.getKeysAttribute() == null ? options.getKeys() : (List<String>) map.get(options.getKeysAttribute());
            if (keys == null && options.getKeyAttribute() != null) {
                keys = Arrays.asList(((String) map.get(options.getKeyAttribute())).split(DEFAULT_DELIMITER));
            }
        }

        if (keys == null || keys.isEmpty()) {
            return;
        }
        final boolean useLookupKey = options.isUseLookupKey();
        AthenaAttributeService service = attributeService(nodeName, options);
        //TODO - need to figure out right way to add histogram around the attributes read
        keys.stream().map(k -> service.readFuture(k).thenApply(v -> {
            Map<String, Object> map = new HashMap<>();
            map.put("key", k);
            map.put("value", v);
            try {
                GenericRecord response = converter.convert(map);
                return emitOutput(kv, useLookupKey ? k : null, response, ctx, schema, recordSchemaId, this.schemaId, this.converter, converterMap, nodeName, nodeType);
            } catch (Exception e) {
                LOGGER.warn("Could not read/process ofs key", Map.of("key", k, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
            }
            return null;
        })).map(CompletableFuture::join).collect(Collectors.toList());
    }

    private Configuration configuration() {
        if (configuration != null) {
            return configuration;
        }
        loadFreemarkerTemplate(templateName, this.options.getExpression());
        configuration = freeMarkerConfiguration();
        return configuration;
    }
}
