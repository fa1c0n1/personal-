package com.apple.aml.stargate.beam.sdk.io.solr;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.options.SolrOptions;
import com.apple.aml.stargate.common.options.SolrQueryOptions;
import freemarker.template.Configuration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.parquet.Strings;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CursorMarkParams;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.apple.aml.stargate.beam.sdk.io.solr.GenericSolrIO.solrClient;
import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.beam.sdk.ts.JavaFunction.emitOutput;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.REDACTED_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.SolrConstants.QUERY_ALL;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.ClassUtils.duplicate;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;


public class SolrReader extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final ConcurrentHashMap<String, SolrClient> CLIENT_MAP = new ConcurrentHashMap<>();
    private final SolrOptions options;
    private final String user;
    private final String password;
    private final ObjectToGenericRecordConverter converter;
    private final String schemaId;
    private final Schema schema;
    private final String nodeName;
    private final String nodeType;
    private final ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap = new ConcurrentHashMap<>();

    public SolrReader(final String nodeName, final String nodeType, final PipelineConstants.ENVIRONMENT env, final SolrOptions options) throws Exception {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.options = duplicate(options, SolrOptions.class);
        this.user = options.isUseBasicAuth() ? options.getUser() : null;
        this.password = options.isUseBasicAuth() ? options.getPassword() : null;
        this.options.setPassword(REDACTED_STRING);
        if (this.options.getSchemaId() == null) {
            this.schema = null; // TODO
            this.schemaId = this.schema.getFullName();
        } else {
            this.schemaId = this.options.getSchemaId();
            this.schema = fetchSchemaWithLocalFallback(this.options.getSchemaReference(), schemaId);
        }
        this.converter = converter(this.schema);
        this.converterMap.put(this.schema.getFullName(), this.converter);
        LOGGER.debug("Converter created successfully for", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName));
    }

    @SuppressWarnings("unchecked")
    @ProcessElement
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
        long startTime = System.nanoTime();
        log(options, nodeName, nodeType, kv);
        GenericRecord record = kv.getValue();
        Schema schema = record.getSchema();
        String recordSchemaId = schema.getFullName();
        counter(nodeName, nodeType, recordSchemaId, ELEMENTS_IN).inc();
        try {
            SolrClient client = CLIENT_MAP.computeIfAbsent(nodeName, n -> solrClient(options));
            List<SolrQueryOptions> queryOptions;
            if (options.isDynamic()) {
                queryOptions = Collections.singletonList(readJson(record.toString(), SolrQueryOptions.class));
            } else {
                queryOptions = options.getOptions() == null || options.getOptions().isEmpty() ? List.of(options) : options.getOptions();
            }
            for (SolrQueryOptions option : queryOptions) {
                SolrQuery query = constructSolrQuery(option);
                String targetSchemaId = option.getSchemaId() == null ? option.getSchemaId() : this.schemaId;
                ObjectToGenericRecordConverter converter = converterMap.computeIfAbsent(targetSchemaId, s -> converter(fetchSchemaWithLocalFallback(options.getSchemaReference(), s)));
                String templateName = null;
                Configuration configuration = null;
                String keyExpression = option.getKeyExpression();
                if (keyExpression != null) {
                    configuration = freeMarkerConfiguration();
                    templateName = nodeName + "~" + targetSchemaId + "~~" + option.getCollection();
                    loadFreemarkerTemplate(templateName, keyExpression);
                }
                String finalTemplateName = templateName;
                Configuration finalConfiguration = configuration;
                processSolrQuery(client, option, query, user, password, nodeName, nodeType, options.getQueryRetryCount(), options.getQueryRetryDuration(), doc -> {
                    String responseKey = null;
                    try {
                        GenericRecord response = converter.convert(doc);
                        if (finalTemplateName != null) {
                            responseKey = evaluateFreemarker(finalConfiguration, finalTemplateName, kv.getKey(), doc, schema);
                        }
                        emitOutput(kv, responseKey, response, ctx, schema, recordSchemaId, targetSchemaId, this.converter, converterMap, nodeName, nodeType);
                    } catch (Exception e) {
                        throw new GenericException(e.getMessage(), Map.of("responseKey", String.valueOf(responseKey)), e).wrap();
                    }
                });
            }
        } finally {
            histogramDuration(nodeName, nodeType, recordSchemaId, "process").observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

    public static SolrQuery constructSolrQuery(final SolrQueryOptions option) {
        return constructSolrQuery(option, null);
    }

    public static long processSolrQuery(final SolrClient client, final SolrQueryOptions option, final SolrQuery query, final String user, final String password, final String nodeName, final String nodeType, final int maxRetries, final Duration queryRetryDuration, final Consumer<SolrDocument> consumer) {
        boolean useCursor = option.getBatchSize() > 0 && option.getStart() < 0 && option.getRows() <= 0;
        long totalCount = 0;
        Map<String, Object> cursorLog = Map.of(NODE_NAME, nodeName, "collection", String.valueOf(option.getCollection()), "query", String.valueOf(query.getQuery()), "filters", query.getFilterQueries() == null || query.getFilterQueries().length == 0 ? "null" : Strings.join(query.getFilterQueries(), " , "));
        try {
            String cursorMark = CursorMarkParams.CURSOR_MARK_START;
            if (useCursor) {
                LOGGER.debug("Cursor enabled.", Map.of("batchSize", option.getBatchSize()), cursorLog);
                query.setRows(option.getBatchSize());
                query.setDistrib(false);
                query.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
            }
            Duration sleepDuration = queryRetryDuration == null ? Duration.ofSeconds(1) : queryRetryDuration;
            do {
                QueryRequest queryRequest = new QueryRequest(query);
                if (user != null && password != null) queryRequest.setBasicAuthCredentials(user, password);
                long queryStartTime = System.nanoTime();
                QueryResponse queryResponse = null;
                for (int i = 1; i <= maxRetries; i++) {
                    try {
                        queryResponse = (option.getCollection() == null) ? queryRequest.process(client) : queryRequest.process(client, option.getCollection());
                        if (queryResponse == null) {
                            throw new GenericException("Solr Query response is null");
                        }
                        break;
                    } catch (Exception qe) {
                        if (i >= maxRetries) {
                            LOGGER.error("Max retries reached to fetch solr response!", Map.of("sleepDuration", sleepDuration, "retryCounter", i, "maxRetries", maxRetries, ERROR_MESSAGE, String.valueOf(qe.getMessage())), cursorLog);
                            throw qe;
                        }
                        LOGGER.warn("Error fetching solr response. Will take a pause and retry", Map.of("sleepDuration", sleepDuration, "retryCounter", i, "maxRetries", maxRetries, ERROR_MESSAGE, String.valueOf(qe.getMessage())), cursorLog);
                        Thread.sleep(sleepDuration.toMillis());
                    }
                }
                histogramDuration(nodeName, nodeType, UNKNOWN, "batch_query_response").observe((System.nanoTime() - queryStartTime) / 1000000.0);
                if (useCursor) {
                    String currentMark = queryResponse.getNextCursorMark();
                    cursorLog = Map.of("cursorMark", String.valueOf(cursorMark), "currentMark", String.valueOf(currentMark), NODE_NAME, nodeName, "collection", String.valueOf(option.getCollection()), "query", String.valueOf(query.getQuery()), "filters", query.getFilterQueries() == null || query.getFilterQueries().length == 0 ? "null" : Strings.join(query.getFilterQueries(), " , "));
                    LOGGER.debug("Solr query invoked", Map.of("batchSize", option.getBatchSize()), cursorLog);
                    if (cursorMark.equals(currentMark)) {
                        break;
                    }
                    cursorMark = currentMark;
                    query.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
                }
                SolrDocumentList results = queryResponse.getResults();
                histogramDuration(nodeName, nodeType, UNKNOWN, "batch_query_results").observe((System.nanoTime() - queryStartTime) / 1000000.0);
                if (results == null || results.isEmpty()) {
                    LOGGER.debug("Solr query result is empty. Possibly reached end", Map.of("batchSize", option.getBatchSize()), cursorLog);
                    break;
                }
                LOGGER.debug("Solr query invoked successfully", Map.of("resultSize", results.size()), cursorLog);
                final AtomicInteger count = new AtomicInteger(0);
                long emitStartTime = System.nanoTime();
                for (SolrDocument doc : results) {
                    try {
                        consumer.accept(doc);
                        count.incrementAndGet();
                    } catch (Exception e) {
                        LOGGER.warn("Could not emit result solr document", doc, Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), cursorLog, e);
                        counter(nodeName, nodeType, UNKNOWN, ELEMENTS_ERROR).inc();
                    }
                }
                histogramDuration(nodeName, nodeType, UNKNOWN, "batch_emit").observe((System.nanoTime() - emitStartTime) / 1000000.0);
                totalCount += count.get();
                LOGGER.debug("Emitted solr results successfully", Map.of("count", count, "collection", String.valueOf(option.getCollection())), cursorLog);
            } while (useCursor);
        } catch (Exception e) {
            try {
                LOGGER.warn("Error in executing solr query", readJsonMap(jsonString(option)), Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), cursorLog, e);
            } catch (Exception ex) {
                LOGGER.warn("Error in executing solr query", option, Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), cursorLog, e);
            }
            counter(nodeName, nodeType, UNKNOWN, ELEMENTS_ERROR).inc();
        }
        if (totalCount == 0) {
            LOGGER.warn("Solr returned empty/null result", Map.of("totalCount", 0), cursorLog);
        } else {
            LOGGER.info("Total solr records emitted", Map.of("totalCount", totalCount), cursorLog);
        }
        return totalCount;
    }

    @SuppressWarnings("unchecked")
    public static SolrQuery constructSolrQuery(final SolrQueryOptions option, final String additionalQuery) {
        SolrQuery query = new SolrQuery();
        boolean useCursor = option.getBatchSize() > 0;
        Set<String> queries = option.queries();
        if (additionalQuery != null) {
            queries.add(additionalQuery);
        }
        if (queries.isEmpty()) {
            query.setQuery(QUERY_ALL);
        } else if (queries.size() == 1) {
            query.setQuery(queries.iterator().next());
        } else {
            query.setQuery(QUERY_ALL);
            query.addFilterQuery(queries.toArray(new String[queries.size()]));
        }
        if (option.getFields() != null && !option.getFields().isEmpty()) {
            query.setFields(option.getFields().toArray(new String[option.getFields().size()]));
        }
        if (option.getStart() >= 0) {
            query.setStart(option.getStart());
            useCursor = false;
        }
        if (option.getRows() > 0) {
            query.setRows(option.getRows());
            useCursor = false;
        }
        if (option.getSorts() != null && !option.getSorts().isEmpty()) {
            option.getSorts().stream().forEach(s -> {
                String[] split = s.split(",");
                query.addSort(SolrQuery.SortClause.create(split[0].trim(), split.length > 1 ? split[1].trim().toLowerCase() : "asc"));
            });
        } else if (useCursor) {
            query.addSort(SolrQuery.SortClause.create("score", "desc"));
        }
        if (option.getParams() != null && !option.getParams().isEmpty()) {
            option.getParams().forEach((k, v) -> {
                if (v instanceof List) {
                    List<String> list = (List<String>) v;
                    String[] paramValues = list.toArray(new String[list.size()]);
                    query.setParam(k, paramValues);
                } else {
                    query.setParam(k, v.toString());
                }
            });
        }
        return query;
    }
}
