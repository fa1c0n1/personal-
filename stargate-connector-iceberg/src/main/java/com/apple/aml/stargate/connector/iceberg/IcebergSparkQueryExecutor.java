package com.apple.aml.stargate.connector.iceberg;

import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.SparkFacadeQueryOptions;
import com.apple.aml.stargate.common.pojo.Holder;
import com.apple.aml.stargate.common.utils.JsonUtils;
import com.apple.aml.stargate.spark.facade.rpc.proto.SparkSqlProtoServiceGrpc;
import com.apple.aml.stargate.spark.facade.rpc.proto.SqlRequestPayload;
import com.apple.aml.stargate.spark.facade.rpc.proto.SqlResponsePayload;
import com.fasterxml.jackson.databind.JsonNode;
import freemarker.template.Configuration;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.io.IOException;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.FORMATTER_DATE_TIME_1;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_NULL;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramSize;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;

public class IcebergSparkQueryExecutor extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> implements Serializable {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final long serialVersionUID = 1L;

    private String nodeName;
    private String nodeType;
    private Level logPayloadLevel;
    private String schemaId;

    private String expressionTemplateName;
    private String preconditionTemplateName;
    private String preconditionQueryTemplateName;

    private transient Configuration configuration;

    private String host = null;
    private int port;

    private String queryExpression = null;
    private String queryAttribute = "query";

    private String resultAttribute = "result";
    private String resultSizeAttribute = "resultSize";
    private String errorAttribute = "error";

    private String statusAttribute = "status";
    private String startTimeAttribute = "startTime";
    private String startTimeEpocAttribute = "startTimeEpoc";
    private String endTimeAttribute = "endTime";
    private String endTimeEpocAttribute = "endTimeEpoc";
    private String durationAttribute = "duration";

    private boolean latest;
    private boolean enableLogging;
    private boolean preconditionExists;
    private String preconditionQuery;
    private String precondition;

    private Duration timeout;

    private boolean asString;
    private boolean asArray;

    private ObjectToGenericRecordConverter converter;

    private final ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap = new ConcurrentHashMap<>();

    public void initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        LOGGER.debug("Setting up iceberg Facade query executor");
        SparkFacadeQueryOptions options = (SparkFacadeQueryOptions) node.getConfig();
        this.nodeName = node.getName();
        this.nodeType = node.getType();
        this.port = options.getPort();
        this.host = options.getHost();
        this.logPayloadLevel = options.logPayloadLevel();
        this.expressionTemplateName = nodeName + "~expression";
        this.preconditionTemplateName = nodeName + "~precondition";
        this.preconditionQueryTemplateName = nodeName + "~preconditionQuery";
        PipelineConstants.ENVIRONMENT environment = node.environment();
        this.schemaId = options.getSchemaId();
        Schema schema = fetchSchemaWithLocalFallback(null, this.schemaId);
        if (options.getQueryExpression() != null && !options.getQueryExpression().trim().isBlank()) {
            this.queryAttribute = null;
            this.queryExpression = options.getQueryExpression().trim();
        } else if (options.getQueryAttribute() != null) {
            this.queryAttribute = options.getQueryAttribute();
        }
        if (options.getResultAttribute() != null) {
            this.resultAttribute = options.getResultAttribute();
        }
        if (options.getResultSizeAttribute() != null) {
            this.resultSizeAttribute = options.getResultSizeAttribute();
        }
        if (options.getErrorAttribute() != null) {
            this.errorAttribute = options.getErrorAttribute();
        }
        if (options.getStatusAttribute() != null) {
            this.statusAttribute = options.getStatusAttribute();
        }
        if (options.getStartTimeAttribute() != null) {
            this.startTimeAttribute = options.getStartTimeAttribute();
        }
        if (options.getStartTimeEpocAttribute() != null) {
            this.startTimeEpocAttribute = options.getStartTimeEpocAttribute();
        }
        if (options.getEndTimeAttribute() != null) {
            this.endTimeAttribute = options.getEndTimeAttribute();
        }
        if (options.getEndTimeEpocAttribute() != null) {
            this.endTimeEpocAttribute = options.getEndTimeEpocAttribute();
        }
        if (options.getDurationAttribute() != null) {
            this.durationAttribute = options.getDurationAttribute();
        }
        this.latest = options.isLatest();
        this.enableLogging = options.isEnableLogging();
        this.timeout = options.getTimeout();
        this.preconditionQuery = options.getPreconditionQuery();
        this.precondition = options.getPrecondition();
        if (this.preconditionQuery != null && this.precondition == null) {
            this.precondition = "true";
        }
        this.preconditionExists = this.precondition != null && this.preconditionQuery != null && !this.precondition.trim().isBlank() && !this.preconditionQuery.trim().isBlank();
        try {
            Schema fieldSchema = schema.getField(this.resultAttribute).schema();
            if (fieldSchema.getType() == Schema.Type.UNION) {
                Schema nonNullableSchema = null;
                for (Schema type : fieldSchema.getTypes()) {
                    if (type.getType() != Schema.Type.NULL) {
                        nonNullableSchema = type;
                        break;
                    }
                }
                if (nonNullableSchema != null) {
                    fieldSchema = nonNullableSchema;
                }
            }
            this.asArray = fieldSchema.getType() == Schema.Type.ARRAY;
            this.asString = this.asArray ? (fieldSchema.getElementType().getType() == Schema.Type.STRING) : (fieldSchema.getType() == Schema.Type.STRING);
        } catch (Exception e) {
            LOGGER.debug("Could not determine resultAttribute schema. Will use defaults {}", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName, "resultAttribute", String.valueOf(this.resultAttribute)));
        }
        this.converter = ObjectToGenericRecordConverter.converter(schema);
        LOGGER.debug("Schema extractor created successfully for {}", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName));
        LOGGER.debug("Setting up iceberg query executor - successful");

    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return collection.apply(node.getName(), this);
    }

    @SuppressWarnings("unchecked")
    @DoFn.ProcessElement
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {

        log(logPayloadLevel, nodeName, nodeType, kv);
        GenericRecord record = kv.getValue();
        Schema schema = record.getSchema();
        String recordSchemaId = schema.getFullName();
        counter(nodeName, nodeType, recordSchemaId, ELEMENTS_IN).inc();
        ObjectToGenericRecordConverter converter = this.converter == null ? converterMap.computeIfAbsent(recordSchemaId, s -> converter(schema)) : this.converter;
        Map recordMap = readJsonMap(record.toString());
        long startTime = System.currentTimeMillis();
        long endTime;
        Pair<List<String>, Boolean> pair = fetchSQL(kv, expressionTemplateName, queryAttribute);
        List<Map> queryOutputMaps = new ArrayList<>();

        if (enableLogging) LOGGER.info("Processing record", Map.of("record", kv.getValue()));
        if (preconditionExists) {
            Pair<List<String>, Boolean> preQuery = fetchSQL(kv, preconditionQueryTemplateName, null);
            final String sql = String.valueOf(preQuery.getKey().get(0));
            try {
                final Holder<List<String>> datasetResult = new Holder<>();
                LOGGER.debug("Invoking pre-condition sql", Map.of("sql", sql));
                invokeSql(sql, datasetResult);
                List<String> rows = datasetResult.getValue();
                String outputString = evaluateFreemarker(configuration(), preconditionTemplateName, kv.getKey(), kv.getValue(), schema, "queryResults", rows.stream().map(r -> {
                    try {
                        return readJsonMap(r);
                    } catch (Exception e) {
                        throw new GenericException("Error reading json", Map.of("json", String.valueOf(r)), e).wrap();
                    }
                }).collect(Collectors.toList()));
                if (outputString == null || outputString.trim().isBlank() || !Boolean.parseBoolean(outputString.trim())) {
                    LOGGER.debug("Precondition failed !. Will skip processing this record {}", Map.of("record", kv.getValue()));
                    return;
                }
            } catch (Exception e) {
                LOGGER.debug("Precondition evaluation failed !. Will skip processing this record {}", Map.of("record", kv.getValue()));
                LOGGER.trace("Precondition evaluation failed !. Will skip processing this record", e);
                return;
            }
        }

        for (final String _sql : pair.getKey()) {
            final String sql = String.valueOf(_sql);
            Map map = new HashMap(recordMap);
            try {
                map.put(this.errorAttribute, null);
                map.put(this.statusAttribute, "SUCCESS");
                map.put(this.queryAttribute == null ? "query" : this.queryAttribute, sql);
                if (enableLogging) LOGGER.info("Invoking Query {}", Map.of("query", sql));
                startTime = System.currentTimeMillis();
                final Holder<List<String>> datasetResult = new Holder<>();
                if (timeout != null && !timeout.isZero()) {
                    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
                    long timeoutMilliseconds = timeout.toMillis();
                    try {
                        ScheduledFuture<?> future = executor.schedule(() -> invokeSql(sql, datasetResult), timeoutMilliseconds, TimeUnit.MILLISECONDS);
                        future.get(timeoutMilliseconds, TimeUnit.MILLISECONDS); // purposefully adding future.get() to block current thread so that `spark.sql` is always sequential and each query gets full spark resources
                    } finally {
                        executor.shutdown();
                    }
                } else {
                    invokeSql(sql, datasetResult);
                }
                endTime = System.currentTimeMillis();
                List<String> rows = datasetResult.getValue();
                long returnSize = rows == null ? 0 : rows.size();
                map.put(this.resultSizeAttribute, returnSize);
                histogramSize(nodeName, nodeType, recordSchemaId, "rows_out").observe(returnSize);
                if (enableLogging) LOGGER.info("Query executed successfully {}", Map.of("query", sql, "returnSize", returnSize, "timeTaken", (endTime - startTime)));
                if (this.asArray) {
                    if (this.asString) {
                        map.put(this.resultAttribute, rows.stream().collect(Collectors.toList()));
                    } else {
                        map.put(this.resultAttribute, rows.stream().map(r -> {
                            try {
                                return readJsonMap(r);
                            } catch (Exception e) {
                                throw new GenericException("Error reading json", Map.of("json", String.valueOf(r)), e).wrap();
                            }
                        }).collect(Collectors.toList()));
                    }
                } else {
                    if (returnSize == 0) {
                        map.put(this.resultAttribute, null);
                        counter(nodeName, nodeType, schemaId, ELEMENTS_NULL).inc();
                    } else if (returnSize == 1) {
                        if (this.asString) {
                            map.put(this.resultAttribute, rows.get(0));
                        } else {
                            map.put(this.resultAttribute, readJsonMap(rows.get(0)));
                        }
                    } else {
                        LOGGER.warn("Query returned more than 1 row, but schema is not configured to be an array. Please correct the schema for {}", Map.of(SCHEMA_ID, schemaId, "resultAttribute", resultAttribute));
                        throw new Exception(resultAttribute + " for schemaId " + schemaId + " is not configured as array");
                    }
                }
                histogramDuration(nodeName, nodeType, schemaId, "query_process_success").observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception e) {
                endTime = System.currentTimeMillis();
                histogramDuration(nodeName, nodeType, schemaId, "query_process_errror").observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.warn("Error in executing query {} {}", Map.of("query", sql, ERROR_MESSAGE, String.valueOf(e.getMessage()), NODE_NAME, nodeName, NODE_TYPE, nodeType), record, e);
                map.put(this.errorAttribute, "Error in executing query. Exception : " + e.getMessage());
                map.put(this.statusAttribute, (e instanceof TimeoutException || e instanceof InterruptedException) ? "TIMEOUT" : "ERROR");
                ctx.output(ERROR_TAG, eRecord(nodeName, nodeType, "query_process", kv, e));
            } finally {
                histogramDuration(nodeName, nodeType, schemaId, "query_process").observe((System.nanoTime() - startTime) / 1000000.0);
            }
            map.put(this.startTimeEpocAttribute, startTime);
            map.put(this.startTimeAttribute, FORMATTER_DATE_TIME_1.format(Instant.ofEpochMilli(startTime)));
            map.put(this.endTimeEpocAttribute, endTime);
            map.put(this.endTimeAttribute, FORMATTER_DATE_TIME_1.format(Instant.ofEpochMilli(endTime)));
            map.put(this.durationAttribute, endTime - startTime);
            queryOutputMaps.add(map);
        }
        GenericRecord output;
        if (pair.getValue() && !latest) {
            Map map = new HashMap(recordMap);
            map.put("outputs", queryOutputMaps);
            output = converter.convert(map);
        } else {
            output = converter.convert(queryOutputMaps.get(0));
        }
        incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, SOURCE_SCHEMA_ID, recordSchemaId);
        ctx.output(KV.of(kv.getKey(), output));
    }

    @SuppressWarnings("unchecked")
    private Pair<List<String>, Boolean> fetchSQL(final KV<String, GenericRecord> kv, final String templateName, final String attributeName) throws Exception {
        if (attributeName != null) {
            return Pair.of(Collections.singletonList(kv.getValue().get(attributeName).toString()), false);
        }
        Schema schema = kv.getValue().getSchema();
        String outputString = evaluateFreemarker(configuration(), templateName, kv.getKey(), kv.getValue(), schema).trim();
        if (outputString.startsWith("[")) {
            return Pair.of(readJson(outputString, ArrayList.class), true);
        }
        return Pair.of(List.of(outputString), false);
    }

    @SuppressWarnings("unchecked")
    private void invokeSql(String sql, Holder<List<String>> datasetResult) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        long startTime = System.nanoTime();
        try {
            SparkSqlProtoServiceGrpc.SparkSqlProtoServiceBlockingStub sparkSqlProtoServiceBlockingStub = SparkSqlProtoServiceGrpc.newBlockingStub(channel);
            SqlResponsePayload sqlResponsePayload = sparkSqlProtoServiceBlockingStub.sql(SqlRequestPayload.newBuilder().setSqlQuery(sql).build());
            String result = sqlResponsePayload.getResponse();
            List<String> rows = new ArrayList<>();
            JsonNode jsonNode = null;
            try {
                jsonNode = JsonUtils.jsonNode(result);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            jsonNode.elements().forEachRemaining(jn -> rows.add(jn.toString()));
            datasetResult.setValue(rows);
        } finally {
            histogramDuration(nodeName, nodeType, schemaId, "spark_sql_execution").observe((System.nanoTime() - startTime) / 1000000.0);
            channel.shutdown();
        }
    }

    private Configuration configuration() {
        if (configuration != null) {
            return configuration;
        }
        loadFreemarkerTemplate(expressionTemplateName, this.queryExpression);
        if (preconditionExists) {
            loadFreemarkerTemplate(preconditionTemplateName, this.precondition);
            loadFreemarkerTemplate(preconditionQueryTemplateName, this.preconditionQuery);
        }
        configuration = freeMarkerConfiguration();
        return configuration;
    }
}
