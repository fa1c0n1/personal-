package com.apple.aml.stargate.beam.sdk.ts;

import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.BeamSQLOptions;
import com.apple.aml.stargate.common.options.WindowOptions;
import com.apple.aml.stargate.common.pojo.AvroRecord;
import com.apple.aml.stargate.common.pojo.ErrorRecord;
import com.apple.aml.stargate.common.utils.ClassUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.applyWindow;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.KEY;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.RECORD;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_SKIPPED;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_LATEST_VERSION;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.nodes.StargateNode.nodeName;
import static com.apple.aml.stargate.common.utils.AvroUtils.getFieldValue;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.SchemaUtils.rowSchema;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.freemarkerSchemaMap;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eJsonRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.saveLocalSchema;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;
import static org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils.convertAvroFieldStrict;
import static org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils.toAvroSchema;
import static org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils.toBeamRowStrict;
import static org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils.toBeamSchema;
import static org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils.toGenericRecord;

public class BeamSQL extends DoFn<Row, KV<String, GenericRecord>> implements Serializable {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final Set<String> FLATTENED_ADDITIONAL_FIELDS = new HashSet<>(asList(String.format("_%s", KEY), String.format("_%s", SCHEMA_ID)));

    public void initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        BeamSQLOptions options = (BeamSQLOptions) node.getConfig();
        options.initSchemaDeriveOptions();
        if (isBlank(options.getKeyAttribute())) options.setKeyAttribute(null);
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        BeamSQLOptions options = (BeamSQLOptions) node.getConfig();
        return applyTransform(pipeline, node.getName(), node.getType(), options, collection);
    }

    public static SCollection<KV<String, GenericRecord>> applyTransform(final Pipeline pipeline, final String nodeName, final String nodeType, final BeamSQLOptions options, final SCollection<KV<String, GenericRecord>> inputCollection) throws Exception {
        Set<String> schemaIds = options.schemaIds();
        if (schemaIds == null || schemaIds.isEmpty()) throw new InvalidInputException("SQL type of node needs predefined list of schemaIds/schemaId");
        SCollection<KV<String, GenericRecord>> collection = options.isWindowing() ? applyWindow(inputCollection, ClassUtils.getAs(options, WindowOptions.class), nodeName) : inputCollection;
        PCollectionList<KV<String, ErrorRecord>> errors = collection.getErrors();
        PCollection<KV<String, GenericRecord>> root = collection.collection();
        Map<String, TupleTag<Row>> outputTags = new HashMap<>();
        Map<String, org.apache.beam.sdk.schemas.Schema> beamSchemas = new HashMap<>();
        TupleTag<Row> mainTag = null;
        List<TupleTag<?>> additionalTags = new ArrayList<>();
        for (String schemaKey : schemaIds) {
            Schema schema = fetchSchemaWithLocalFallback(options.getSchemaReference(), schemaKey);
            String tagName = schema.getName().replace('-', '_').toLowerCase();
            String[] schemaSplit = schemaKey.split(":");
            TupleTag<Row> tag;
            if (schemaSplit.length > 1) {
                int version = parseInt(schemaSplit[1].trim());
                tag = new TupleTag<>(String.format("%s_%d", tagName, version)) {
                };
            } else {
                tag = new TupleTag<>(tagName) {
                };
            }
            UUID beamSchemaId = UUID.randomUUID();
            Schema rowSchema = rowSchema(schema, options.isFlatten(), true);
            org.apache.beam.sdk.schemas.Schema beamSchema = toBeamSchema(rowSchema);
            beamSchema.setUUID(beamSchemaId);
            saveLocalSchema(beamSchemaId.toString(), rowSchema.toString());
            beamSchemas.put(schemaKey, beamSchema);
            outputTags.put(schemaKey, tag);
            if (mainTag == null) mainTag = tag;
            else additionalTags.add(tag);
        }
        additionalTags.add(ERROR_TAG);
        PCollectionTuple tuple = root.apply(nodeName(nodeName, "to-beam-row"), ParDo.of(new ToBeamRow(options, nodeName(nodeName, "to-beam-row"), nodeType, outputTags, beamSchemas)).withOutputTags(mainTag, TupleTagList.of(additionalTags)));
        errors = errors.and(tuple.get(ERROR_TAG));
        PCollectionTuple output = null;
        for (Map.Entry<String, TupleTag<Row>> entry : outputTags.entrySet()) {
            PCollection<Row> rows = tuple.get(entry.getValue()).setCoder(RowCoder.of(beamSchemas.get(entry.getKey())));
            if (output == null) output = PCollectionTuple.of(entry.getValue(), rows);
            else output = output.and(entry.getValue(), rows);
        }
        List<String> sqls = new ArrayList<>();
        if (!isBlank(options.getSql())) sqls.add(options.getSql());
        if (options.getSqls() != null && !options.getSqls().isEmpty()) sqls.addAll(options.getSqls());
        PCollection<Row> rows = null;
        int sqlIndex = 0;
        for (String sql : sqls) {
            String name = nodeName(nodeName, "sql", String.valueOf(sqlIndex));
            if (rows == null) {
                rows = output.apply(name, SqlTransform.query(sql));
            } else {
                rows = rows.apply(name, SqlTransform.query(sql));
            }
            sqlIndex++;
        }
        return SCollection.of(pipeline, rows, errors).apply(nodeName(nodeName, "to-kv"), new ToKV(options, nodeName(nodeName, "to-kv"), nodeType));
    }

    public static class ToBeamRow extends DoFn<KV<String, GenericRecord>, Row> implements Serializable {
        private static final long serialVersionUID = 1L;
        private final boolean flatten;
        private final String nodeName;
        private final String nodeType;
        private final Map<String, TupleTag<Row>> outputTags;
        private final Map<String, org.apache.beam.sdk.schemas.Schema> beamSchemas;

        public ToBeamRow(final BeamSQLOptions options, final String nodeName, final String nodeType, final Map<String, TupleTag<Row>> outputTags, final Map<String, org.apache.beam.sdk.schemas.Schema> beamSchemas) {
            this.flatten = options.isFlatten();
            this.nodeName = nodeName;
            this.nodeType = nodeType;
            this.outputTags = outputTags;
            this.beamSchemas = beamSchemas;
        }


        @SuppressWarnings("unchecked")
        @ProcessElement
        public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
            long startTime = System.nanoTime();
            GenericRecord record = kv.getValue();
            Schema recordSchema = record.getSchema();
            String recordSchemaId = recordSchema.getFullName();
            counter(nodeName, nodeType, recordSchemaId, ELEMENTS_IN).inc();
            int version = record instanceof AvroRecord ? ((AvroRecord) record).getSchemaVersion() : SCHEMA_LATEST_VERSION;
            String schemaKey = version == SCHEMA_LATEST_VERSION ? recordSchemaId : String.format("%s:%d", recordSchemaId, version);
            TupleTag<Row> outputTag = outputTags.get(schemaKey);
            if (outputTag == null) {
                schemaKey = recordSchemaId;
                outputTag = outputTags.get(schemaKey);
            }
            if (outputTag == null) {
                counter(nodeName, nodeType, recordSchemaId, ELEMENTS_SKIPPED).inc();
                return;
            }
            org.apache.beam.sdk.schemas.Schema beamSchema = beamSchemas.get(schemaKey);
            try {
                Row.Builder builder = Row.withSchema(beamSchema);
                if (flatten) {
                    for (org.apache.beam.sdk.schemas.Schema.Field field : beamSchema.getFields().stream().filter(f -> !FLATTENED_ADDITIONAL_FIELDS.contains(f.getName())).collect(Collectors.toList())) {
                        Object value = getFieldValue(record, field.getName());
                        org.apache.avro.Schema fieldAvroSchema = recordSchema.getField(field.getName()).schema();
                        builder.addValue(convertAvroFieldStrict(value, fieldAvroSchema, field.getType()));
                    }
                    builder.addValue(kv.getKey());
                    builder.addValue(recordSchemaId);
                } else {
                    builder.addValue(kv.getKey());
                    builder.addValue(freemarkerSchemaMap(recordSchema, version));
                    builder.addValue(toBeamRowStrict(record, beamSchema.getField(RECORD).getType().getRowSchema()));
                }
                ctx.output(outputTag, builder.build());
            } catch (Exception e) {
                histogramDuration(nodeName, nodeType, recordSchemaId, "row_conversion").observe((System.nanoTime() - startTime) / 1000000.0);
                counter(nodeName, nodeType, recordSchemaId, ELEMENTS_ERROR).inc();
                LOGGER.warn("Error in converting kv to beam row", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "key", kv.getKey(), NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, recordSchemaId));
                ctx.output(ERROR_TAG, eRecord(nodeName, nodeType, "row_conversion", kv, e));
                return;
            }
            histogramDuration(nodeName, nodeType, recordSchemaId, "process").observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

    public static class ToKV extends DoFn<Row, KV<String, GenericRecord>> implements Serializable {
        private static final long serialVersionUID = 1L;
        private static final ConcurrentHashMap<String, Schema> AVRO_SCHEMA_MAP = new ConcurrentHashMap<>();
        private String nodeName;
        private String nodeType;
        private BeamSQLOptions options;
        private Schema schema;
        private String schemaId;
        private ObjectToGenericRecordConverter converter;

        public ToKV(final BeamSQLOptions options, final String nodeName, final String nodeType) {
            this.nodeName = nodeName;
            this.nodeType = nodeType;
            this.options = options;
            this.options.initSchemaDeriveOptions();
            if (!isBlank(this.options.getSchemaId())) {
                this.schema = fetchSchemaWithLocalFallback(options.getSchemaReference(), options.getSchemaId());
                this.schemaId = schema.getFullName();
                this.converter = converter(this.schema);
                LOGGER.debug("Beam SQL schema converter created successfully for", Map.of(SCHEMA_ID, this.schemaId, NODE_NAME, nodeName));
            } else if (options.getSchema() != null && ("override".equalsIgnoreCase(options.getSchemaType()) || "replace".equalsIgnoreCase(options.getSchemaType()))) {
                this.schema = new Schema.Parser().parse(options.getSchema() instanceof String ? (String) options.getSchema() : jsonString(options.getSchema()));
                this.schemaId = schema.getFullName();
                this.converter = converter(schema, options);
                saveLocalSchema(schemaId, schema.toString());
                LOGGER.debug("Beam SQL schema converter created successfully using schema override for", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName, NODE_TYPE, nodeType));
            }
        }

        @SuppressWarnings("unchecked")
        @ProcessElement
        public void processElement(@Element final Row row, final ProcessContext ctx) throws Exception {
            long startTime = System.nanoTime();
            String key = null;
            GenericRecord record;
            String rowSchemaId = UNKNOWN;
            try {
                if (schemaId == null) {
                    org.apache.beam.sdk.schemas.Schema beamSchema = row.getSchema();
                    Schema avroSchema = fetchAvroSchema(beamSchema);
                    record = toGenericRecord(row, avroSchema);
                } else {
                    Map<String, Object> map = new HashMap<>();
                    row.getSchema().getFields().stream().forEach(f -> {
                        map.put(f.getName(), row.getBaseValue(f.getName()));
                    });
                    record = converter.convert(map);
                }
                rowSchemaId = record.getSchema().getFullName();
                if (options.getKeyAttribute() == null) {
                    key = UUID.randomUUID().toString();
                } else {
                    Object value = getFieldValue(record, options.getKeyAttribute());
                    key = value == null ? UUID.randomUUID().toString() : String.valueOf(value);
                }
            } catch (Exception e) {
                histogramDuration(nodeName, nodeType, rowSchemaId, "kv_conversion").observe((System.nanoTime() - startTime) / 1000000.0);
                counter(nodeName, nodeType, rowSchemaId, ELEMENTS_ERROR).inc();
                LOGGER.warn("Error in converting beam row to kv", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "key", String.valueOf(key), NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, rowSchemaId));
                ctx.output(ERROR_TAG, eJsonRecord(nodeName, nodeType, "kv_conversion", KV.of(key, row), e));
                return;
            }
            histogramDuration(nodeName, nodeType, rowSchemaId, "process").observe((System.nanoTime() - startTime) / 1000000.0);
            ctx.output(KV.of(key, record));
        }

        private Schema fetchAvroSchema(final org.apache.beam.sdk.schemas.Schema beamSchema) {
            String namespace = String.format("com.apple.aml.stargate.local.beam.%s", nodeName).replace('-', '_').replaceAll(":", "\\.");
            String schemaName = "BeamAvroSchema";
            String schemaId = String.format("%s.%s", namespace, schemaName);
            return AVRO_SCHEMA_MAP.computeIfAbsent(schemaId, id -> {
                Schema avroSchema = toAvroSchema(beamSchema, schemaName, namespace);
                saveLocalSchema(schemaId, avroSchema.toString());
                return avroSchema;
            });
        }
    }
}
