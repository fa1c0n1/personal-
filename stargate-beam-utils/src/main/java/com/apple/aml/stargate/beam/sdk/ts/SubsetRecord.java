package com.apple.aml.stargate.beam.sdk.ts;

import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.converters.GenericSubsetRecordExtractor;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.SubsetRecordOptions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_PASS_THROUGH;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_SKIPPED;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.SchemaUtils.derivedSchema;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.saveLocalSchema;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class SubsetRecord extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private String nodeType;
    private String nodeName;
    private Level logPayloadLevel;
    private Map<String, Object> mappings;
    private String schemaReference;
    private boolean passThrough;
    private Set<String> includes;
    private Set<String> excludes;
    private Pattern regexPattern;
    private boolean enableSimpleSchema;
    private boolean enableHierarchicalSchemaFilters;
    private ConcurrentHashMap<String, GenericSubsetRecordExtractor> subSchemaExtractorMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Schema> schemaMap = new ConcurrentHashMap<>();


    @SuppressWarnings("unchecked")
    public void initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        SubsetRecordOptions options = (SubsetRecordOptions) node.getConfig();
        this.mappings = options.getMappings() == null ? Map.of() : options.getMappings();
        this.schemaReference = options.getSchemaReference();
        this.passThrough = options.isPassThrough();
        this.nodeName = node.getName();
        this.nodeType = node.getType();
        this.logPayloadLevel = options.logPayloadLevel();
        this.regexPattern = isBlank(options.getRegex()) ? null : Pattern.compile(options.getRegex().trim());
        this.includes = options.getIncludes() == null ? null : new HashSet<>(options.getIncludes() instanceof List ? (List<String>) options.getIncludes() : List.of(((String) options.getIncludes()).split(",")));
        this.excludes = options.getExcludes() == null ? null : new HashSet<>(options.getExcludes() instanceof List ? (List<String>) options.getExcludes() : List.of(((String) options.getExcludes()).split(",")));
        this.enableSimpleSchema = options.isEnableSimpleSchema();
        this.enableHierarchicalSchemaFilters = options.isEnableHierarchicalSchemaFilters();
        LOGGER.debug("Creating subset extractors for", mappings, Map.of(NODE_NAME, nodeName));
        for (Map.Entry<String, Object> entry : mappings.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue().toString();
            Schema targetSchema = fetchSchemaWithLocalFallback(schemaReference, value);
            GenericSubsetRecordExtractor extractor = GenericSubsetRecordExtractor.extractor(targetSchema);
            subSchemaExtractorMap.put(key, extractor);
            LOGGER.debug("Subset extractor created for", Map.of("sourceSchemaId", key, "targetSchemaId", value, NODE_NAME, nodeName));
        }
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> inputCollection) throws Exception {
        return inputCollection.apply(node.getName(), this);
    }

    @ProcessElement
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
        long startTime = System.nanoTime();
        log(logPayloadLevel, nodeName, nodeType, kv);
        GenericRecord record = kv.getValue();
        Schema schema = record.getSchema();
        String schemaId = schema.getFullName();
        counter(nodeName, nodeType, schemaId, ELEMENTS_IN).inc();
        try {
            GenericSubsetRecordExtractor extractor = subSchemaExtractorMap.computeIfAbsent(schemaId, s -> {
                Schema subsetSchema = schemaMap.computeIfAbsent(s, sid -> {
                    Schema derivedSchema = derivedSchema(schema, nodeName, null, includes, excludes, regexPattern, enableSimpleSchema, enableHierarchicalSchemaFilters);
                    String derivedSchemaId = derivedSchema.getFullName();
                    saveLocalSchema(derivedSchemaId, derivedSchema.toString());
                    return derivedSchema;
                });
                if (subsetSchema == null || subsetSchema.equals(schema)) return null;
                return GenericSubsetRecordExtractor.extractor(subsetSchema);
            });
            if (extractor == null) {
                if (passThrough) {
                    counter(nodeName, nodeType, schemaId, ELEMENTS_PASS_THROUGH).inc();
                    incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, SOURCE_SCHEMA_ID, schemaId);
                    ctx.output(kv);
                } else {
                    counter(nodeName, nodeType, schemaId, ELEMENTS_SKIPPED).inc();
                }
                return;
            }
            GenericRecord output = extractor.extract(record);
            incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, SOURCE_SCHEMA_ID, schemaId);
            ctx.output(KV.of(kv.getKey(), output));
        } finally {
            histogramDuration(nodeName, nodeType, schemaId, "process").observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }
}
