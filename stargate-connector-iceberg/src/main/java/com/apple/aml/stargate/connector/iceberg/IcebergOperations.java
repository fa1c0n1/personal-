package com.apple.aml.stargate.connector.iceberg;

import com.apple.aml.stargate.beam.sdk.transforms.CollectionFns;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.IcebergOpsOptions;
import com.apple.aml.stargate.common.pojo.AvroRecord;
import freemarker.ext.beans.BeansWrapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.KV;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;

import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.beam.sdk.ts.JavaFunction.emitOutput;
import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.applyWindow;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.KEY;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.RECORD;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.SCHEMA;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.STATICS;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_LATEST_VERSION;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.JsonUtils.readNullableJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.freemarkerSchemaMap;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;

public class IcebergOperations extends IcebergIO {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    public static void init(final PipelineOptions pipelineOptions, final StargateNode node) throws Exception {
        IcebergIO.init(pipelineOptions, node);
    }

    @SuppressWarnings("unchecked")
    public void initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        LOGGER.debug("Setting up iceberg operation configs - started", Map.of(NODE_NAME, node.getName(), NODE_TYPE, node.getType()));
        super.initTransform(pipeline, node);
        LOGGER.debug("Setting up iceberg operation configs - successful", Map.of(NODE_NAME, node.getName(), NODE_TYPE, node.getType()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        IcebergOpsOptions options = (IcebergOpsOptions) node.getConfig();
        PipelineConstants.ENVIRONMENT environment = node.environment();
        OPERATION_TYPE operationType = OPERATION_TYPE.valueOf(options.getOperation().toLowerCase());
        SCollection<KV<String, GenericRecord>> window = collection;

        if (operationType == OPERATION_TYPE.query) {
            window = applyWindow(collection, options, node.getName());
        }
        switch (operationType) {
            case query:
                LOGGER.info("Iceberg query table with options ", Map.of("icebergTableOptions", options, NODE_NAME, node.getName(), NODE_TYPE, node.getType()));
                return window.list(node.name("query"), Combine.globally(new QueryExecutor(node.getName(), node.getType(), options.getQuery(), options.getEvaluate(), options.isPassThrough(), options.getSchemaId(), options.getSchemaReference(), environment)).withoutDefaults()).apply(node.name("result"), new CollectionFns.IndividualRecords<>());
            case createtable:
                return window.apply(node.name("create-table"), new IcebergTableCreator(node.getName(), node.getType(), options));
            case commitdatafiles:
                return window.apply(node.name("commit"), new IcebergTableCommitter(node.getName(), node.getType(), options));
            case noop:
                return window;
        }
        return super.transform(pipeline, node, window);
    }

    private enum OPERATION_TYPE {
        query, createtable, commitdatafiles, noop
    }

    public static class QueryExecutor extends Combine.CombineFn<KV<String, GenericRecord>, Map<String, Set<GenericRecord>>, List<KV<String, GenericRecord>>> {
        private final String nodeName;
        private final String nodeType;
        private final String query;
        private final String evaluate;
        private final boolean passThrough;
        private final String schemaId;
        private final ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap = new ConcurrentHashMap<>();
        private final String queryTemplateName;
        private final String evaluateTemplateName;
        private freemarker.template.Configuration configuration;
        private SparkSession spark;
        private Schema schema;
        private ObjectToGenericRecordConverter converter;

        public QueryExecutor(final String nodeName, final String nodeType, final String query, final String evaluate, final boolean passThrough, final String schemaId, final String schemaReference, final PipelineConstants.ENVIRONMENT environment) {
            this.nodeName = nodeName;
            this.nodeType = nodeType;
            this.query = query;
            this.evaluate = evaluate;
            this.passThrough = passThrough;
            this.schemaId = schemaId;
            queryTemplateName = nodeName + "~query";
            evaluateTemplateName = nodeName + "~evaluate";
            if (this.schemaId != null) {
                this.schema = fetchSchemaWithLocalFallback(schemaReference, this.schemaId);
                this.converter = converter(this.schema);
                LOGGER.debug("Converter created successfully for", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName, NODE_TYPE, nodeType));
            }
        }

        @Override
        public Map<String, Set<GenericRecord>> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public Map<String, Set<GenericRecord>> addInput(final Map<String, Set<GenericRecord>> accumulator, final KV<String, GenericRecord> input) {
            accumulator.computeIfAbsent(input.getKey(), k -> new HashSet<>()).add(input.getValue());
            return accumulator;
        }

        @Override
        public Map<String, Set<GenericRecord>> mergeAccumulators(final Iterable<Map<String, Set<GenericRecord>>> accumulators) {
            Iterator<Map<String, Set<GenericRecord>>> itr = accumulators.iterator();
            if (itr.hasNext()) {
                Map<String, Set<GenericRecord>> first = itr.next();
                while (itr.hasNext()) {
                    for (Map.Entry<String, Set<GenericRecord>> entry : itr.next().entrySet()) {
                        first.computeIfAbsent(entry.getKey(), k -> new HashSet<>()).addAll(entry.getValue());
                    }
                }
                return first;
            } else {
                return new HashMap<>();
            }
        }

        @Override
        public List<KV<String, GenericRecord>> extractOutput(Map<String, Set<GenericRecord>> map) {
            final List<KV<String, GenericRecord>> list = new ArrayList<>();
            map.entrySet().forEach(entry -> {
                try {
                    Set<GenericRecord> values = entry.getValue();
                    if (values.isEmpty()) {
                        return;
                    }
                    for (GenericRecord record : values) {
                        KV<String, GenericRecord> kv = process(KV.of(entry.getKey(), record));
                        list.add(kv);
                    }
                } catch (Exception e) {
                    throw new GenericException("Error invoking iceberg operation", Map.of("key", entry.getKey(), "value", entry.getValue(), NODE_NAME, nodeName, NODE_TYPE, nodeType), e).wrap();
                }
            });
            return list;
        }

        @SuppressWarnings({"unchecked", "deprecation"})
        public KV<String, GenericRecord> process(final KV<String, GenericRecord> kv) throws Exception {
            String key = kv.getKey();
            GenericRecord record = kv.getValue();
            Schema schema = record.getSchema();
            String recordSchemaId = schema.getFullName();
            counter(nodeName, nodeType, recordSchemaId, ELEMENTS_IN).inc();
            Map<String, Object> object = Map.of(KEY, key, RECORD, record, SCHEMA, freemarkerSchemaMap(schema, record instanceof AvroRecord ? ((AvroRecord) record).getSchemaVersion() : SCHEMA_LATEST_VERSION), STATICS, BeansWrapper.getDefaultInstance().getStaticModels());
            String sql = evaluateFreemarker(configuration(), queryTemplateName, key, record, schema);
            SparkSession spark = spark();
            Dataset<Row> dataset = spark.sql(sql);
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.putAll(object);
            resultMap.put("results", dataset.collectAsList().stream().map(r -> {
                final String json = r.json();
                try {
                    return readNullableJsonMap(json);
                } catch (Exception e) {
                    throw new GenericException("Error reading json", Map.of("json", String.valueOf(json)), e).wrap();
                }
            }).collect(Collectors.toList()));
            if (this.evaluate != null) {
                StringWriter writer = new StringWriter();
                configuration().getTemplate(evaluateTemplateName).process(resultMap, writer);
                String outputString = writer.toString();
                resultMap = (Map) readNullableJsonMap(outputString);
            }
            if (resultMap == null) {
                LOGGER.debug("Freemarker processing returned null. Will skip this record", Map.of(SCHEMA_ID, recordSchemaId, "query", sql, "key", key, NODE_NAME, nodeName, NODE_TYPE, nodeType));
                return null;
            }
            if (passThrough) {
                incCounters(nodeName, nodeType, recordSchemaId, ELEMENTS_OUT, SOURCE_SCHEMA_ID, recordSchemaId);
                return kv;
            } else {
                return emitOutput(kv, null, resultMap, null, schema, recordSchemaId, this.schemaId, this.converter, converterMap, nodeName, nodeType);
            }
        }

        @SuppressWarnings("unchecked")
        protected freemarker.template.Configuration configuration() {
            if (configuration != null) {
                return configuration;
            }
            loadFreemarkerTemplate(queryTemplateName, this.query);
            if (this.evaluate != null) {
                loadFreemarkerTemplate(evaluateTemplateName, this.evaluate);
            }
            configuration = freeMarkerConfiguration();
            return configuration;
        }

        private SparkSession spark() {
            if (this.spark == null) {
                this.spark = SparkSession.active();
            }
            return this.spark;
        }
    }
}
