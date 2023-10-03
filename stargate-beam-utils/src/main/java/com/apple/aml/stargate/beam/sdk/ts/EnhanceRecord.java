package com.apple.aml.stargate.beam.sdk.ts;

import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.CustomFieldOptions;
import com.apple.aml.stargate.common.options.EnhanceRecordOptions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.common.constants.CommonConstants.ENHANCE_RECORD_SCHEMA_SUFFIX;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_PASS_THROUGH;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_SKIPPED;
import static com.apple.aml.stargate.common.constants.CommonConstants.OfsConstants.CUSTOM_FIELD_DELIMITER;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.utils.ClassUtils.getAs;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.SchemaUtils.schema;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.saveLocalSchema;

public class EnhanceRecord extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private String nodeName;
    private String nodeType;
    private Level logPayloadLevel;
    private List<CustomFieldOptions> mappings;
    private Map<String, List<CustomFieldOptions>> schemaMappings;
    private boolean passThrough;
    private String delimiter;
    private ConcurrentHashMap<String, Schema> schemaMap = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public void initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        EnhanceRecordOptions options = (EnhanceRecordOptions) node.getConfig();
        this.nodeName = node.getName();
        this.nodeType = node.getType();
        this.logPayloadLevel = options.logPayloadLevel();
        this.mappings = Arrays.asList(getAs(Optional.ofNullable(options.getMappings()).orElse(List.of()), CustomFieldOptions[].class));
        this.passThrough = options.isPassThrough();
        this.schemaMappings = getAsCustomFieldOptions(options.getSchemaMappings());
        this.delimiter = options.getDelimiter() == null ? CUSTOM_FIELD_DELIMITER : options.getDelimiter();

        LOGGER.debug("Creating enhance record schema mappings for", mappings, Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType));
        for (Map.Entry<String, List<CustomFieldOptions>> schemaMapping : schemaMappings.entrySet()) {
            String schemaId = schemaMapping.getKey();
            List<CustomFieldOptions> customFieldOptions = Optional.ofNullable(schemaMapping.getValue()).orElse(mappings);
            getUpdatedSchema(schemaId, schema(schemaId), customFieldOptions);
            LOGGER.debug("Modified schema created for", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName, NODE_TYPE, nodeType));
        }
    }

    private Map<String, List<CustomFieldOptions>> getAsCustomFieldOptions(Map<String, Object> schemaMappings) throws Exception {
        Map<String, List<CustomFieldOptions>> result = new HashMap<>();
        if (schemaMappings == null || schemaMappings.isEmpty()) return result;

        for (Map.Entry<String, Object> schemaEntry : schemaMappings.entrySet()) {
            String schemaId = schemaEntry.getKey();
            Object value = schemaEntry.getValue();
            if (!(value instanceof List)) {
                LOGGER.warn("invalid custom field mappings for ", Map.of(SCHEMA_ID, schemaId, "schemaDefType", value.getClass(), NODE_NAME, nodeName, NODE_TYPE, nodeType));
                continue;
            }
            result.put(schemaId, Arrays.asList(getAs(value, CustomFieldOptions[].class)));
        }
        return result;
    }

    private Schema getUpdatedSchema(String schemaId, Schema orgSchema, List<CustomFieldOptions> customFieldOptions) {
        return schemaMap.computeIfAbsent(schemaId, id -> {
            //customFieldOptions.forEach(e -> orgSchema.addProp(e.getName(), "string")); // update schema with custom field
            List<Schema.Field> fields = new ArrayList<>(orgSchema.getFields());
            for (CustomFieldOptions customField : customFieldOptions) {
                Schema schema = Schema.create(Schema.Type.STRING);
                Schema.Field field = new Schema.Field(customField.getName(), schema, null, (Object) null);
                fields.add(field);
            }
            fields = fields.stream().map(f -> new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal())).collect(Collectors.toList());
            // note change in schema namespace here
            Schema newSchema = Schema.createRecord(orgSchema.getName(), orgSchema.getDoc(), orgSchema.getNamespace() + ENHANCE_RECORD_SCHEMA_SUFFIX, orgSchema.isError(), fields);
            saveLocalSchema(newSchema.getFullName(), newSchema.toString());
            LOGGER.debug("Modified schema created for", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName, NODE_TYPE, nodeType, "customField", customFieldOptions));
            return newSchema;
        });
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> inputCollection) throws Exception {
        return inputCollection.apply(node.getName(), this);
    }

    @ProcessElement
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
        log(logPayloadLevel, nodeName, nodeType, kv);
        GenericRecord record = kv.getValue();
        Schema schema = record.getSchema();
        String schemaId = schema.getFullName();
        counter(nodeName, nodeType, schemaId, ELEMENTS_IN).inc();

        // no default mapping and schemaMappings missing record schema, eval for pass through
        List<CustomFieldOptions> customFieldOptions = schemaMappings.getOrDefault(schemaId, mappings);
        if (customFieldOptions == null || customFieldOptions.isEmpty()) {
            LOGGER.debug("No custom filed mappings found, schema is unmodified for", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName));
            if (passThrough) {
                counter(nodeName, nodeType, schemaId, ELEMENTS_PASS_THROUGH).inc();
                LOGGER.debug("Schema pass through enabled. Will let it go through", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName));
                ctx.output(kv);
                return;
            }
            counter(nodeName, nodeType, schemaId, ELEMENTS_SKIPPED).inc();
            LOGGER.warn("Schema pass through disabled. Will drop the record", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName));
            return;
        }
        GenericRecord outRecord = getUpdatedRecord(schemaId, schema, record, customFieldOptions);
        incCounters(nodeName, nodeType, outRecord.getSchema().getFullName(), ELEMENTS_OUT, SOURCE_SCHEMA_ID, schemaId);
        ctx.output(KV.of(kv.getKey(), outRecord));
    }

    private GenericRecord getUpdatedRecord(String schemaId, Schema inSchema, GenericRecord inRecord, List<CustomFieldOptions> customFieldOptions) {
        // todo - handle nested records / structures
        List<Schema.Field> inFields = inSchema.getFields();
        Schema outSchema = getUpdatedSchema(schemaId, inSchema, customFieldOptions);
        GenericRecord outRecord = new GenericData.Record(outSchema); // new record with updated schema
        inFields.forEach(f -> outRecord.put(f.name(), inRecord.get(f.name()))); // copy over all entries from input record
        customFieldOptions.forEach(e -> outRecord.put(e.getName(), getCustomFieldValue(inRecord, e))); // set value(s) to custom field(s)
        return outRecord;
    }

    private Utf8 getCustomFieldValue(GenericRecord inRecord, CustomFieldOptions fieldOptions) {
        return new Utf8(fieldOptions.getFields().stream().map(e -> Objects.toString(inRecord.get(e))).collect(Collectors.joining(delimiter)));
    }
}
