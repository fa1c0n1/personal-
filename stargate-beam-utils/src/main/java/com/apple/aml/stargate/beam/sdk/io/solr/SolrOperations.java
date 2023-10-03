package com.apple.aml.stargate.beam.sdk.io.solr;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.options.SolrOptions;
import freemarker.template.Configuration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.parquet.Strings;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.beam.sdk.io.solr.GenericSolrIO.solrClient;
import static com.apple.aml.stargate.beam.sdk.io.solr.SolrReader.constructSolrQuery;
import static com.apple.aml.stargate.beam.sdk.io.solr.SolrReader.processSolrQuery;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.REDACTED_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.SolrConstants.QUERY_ALL;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.ClassUtils.duplicate;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.getState;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.saveState;
import static java.lang.Long.parseLong;
import static org.codehaus.groovy.runtime.InvokerHelper.asList;


public class SolrOperations extends DoFn<Long, KV<String, GenericRecord>> {
    protected static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    protected static final ConcurrentHashMap<String, SolrClient> CLIENT_MAP = new ConcurrentHashMap<>();
    private static final long serialVersionUID = 1L;
    protected final SolrOptions option;
    protected final String nodeName;
    protected final String collection;
    protected final TimeUnit timestampUnit;
    protected final String nodeType;
    protected final String keyTemplateName;
    protected final String user;
    protected final String password;
    protected final Instant pipelineInvokeTime;
    protected final String timestampFieldName;
    protected final ObjectToGenericRecordConverter converter;
    protected final Schema schema;
    private transient Configuration configuration;

    public SolrOperations(final String nodeName, final String nodeType, final PipelineConstants.ENVIRONMENT env, final Instant pipelineInvokeTime, final SolrOptions options) throws Exception {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.pipelineInvokeTime = pipelineInvokeTime;
        this.option = duplicate(options, SolrOptions.class);
        this.collection = this.option.getCollection();
        this.user = options.isUseBasicAuth() ? options.getUser() : null;
        this.password = options.isUseBasicAuth() ? options.getPassword() : null;
        this.option.setPassword(REDACTED_STRING);
        this.timestampFieldName = option.getTimestampFieldName() == null || option.getTimestampFieldName().trim().isBlank() ? null : option.getTimestampFieldName().trim();
        this.timestampUnit = option.getTimestampUnit() == null || option.getTimestampUnit().trim().isBlank() ? TimeUnit.MILLISECONDS : TimeUnit.valueOf(option.getTimestampUnit().trim().toUpperCase());
        String schemaId = this.option.getSchemaId();
        this.schema = fetchSchemaWithLocalFallback(this.option.getSchemaReference(), schemaId);
        this.converter = converter(this.schema);
        this.keyTemplateName = option.getKeyExpression() == null || option.getKeyExpression().trim().isBlank() ? null : (nodeName + "~" + option.getCollection());
        LOGGER.debug("Converter created successfully for", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName, "collection", this.option.getCollection()));
    }

    protected Configuration configuration() {
        if (configuration != null) return configuration;
        if (keyTemplateName != null) loadFreemarkerTemplate(keyTemplateName, option.getKeyExpression());
        configuration = freeMarkerConfiguration();
        return configuration;
    }

    protected long fetchLatestTimestampValue() throws SolrServerException, IOException {
        long start;
        SolrClient client = client();
        SolrQuery query = new SolrQuery();
        query.setQuery(QUERY_ALL);
        query.setStart(0);
        query.setRows(1);
        query.addSort(SolrQuery.SortClause.create(timestampFieldName, SolrQuery.ORDER.desc));
        query.addField(timestampFieldName);
        QueryRequest queryRequest = new QueryRequest(query);
        if (option.isUseBasicAuth()) queryRequest.setBasicAuthCredentials(this.user, this.password);
        QueryResponse queryResponse = (option.getCollection() == null) ? queryRequest.process(client) : queryRequest.process(client, option.getCollection());
        SolrDocumentList results = queryResponse.getResults();
        start = results == null || results.isEmpty() ? 0 : parseLong(String.valueOf(results.get(0).getFieldValue(timestampFieldName)));
        query.toString();
        LOGGER.debug("Queried successfully for latestTimestamp", Map.of("latestValue", start, NODE_NAME, nodeName, "collection", collection, "query", String.valueOf(query.getQuery()), "filters", query.getFilterQueries() == null || query.getFilterQueries().length == 0 ? "null" : Strings.join(query.getFilterQueries(), " , ")));
        return start;
    }

    private SolrClient client() {
        return CLIENT_MAP.computeIfAbsent(nodeName, n -> solrClient(option));
    }

    @SuppressWarnings("unchecked")
    protected List<OffsetRange> fetchDistinctTimestampRanges(final OffsetRange range) throws SolrServerException, IOException {
        SolrClient client = client();
        SolrQuery query = new SolrQuery();
        query.setQuery(QUERY_ALL);
        query.setFilterQueries(String.format("%s:[%d TO %d}", timestampFieldName, range.getFrom(), range.getTo()));
        query.setStart(0);
        query.setRows(0);
        query.setGetFieldStatistics(true);
        query.setGetFieldStatistics(timestampFieldName);
        query.addStatsFieldCalcDistinct(null, true);
        QueryRequest queryRequest = new QueryRequest(query);
        if (option.isUseBasicAuth()) queryRequest.setBasicAuthCredentials(this.user, this.password);
        QueryResponse queryResponse = (option.getCollection() == null) ? queryRequest.process(client) : queryRequest.process(client, option.getCollection());
        FieldStatsInfo stats = queryResponse.getFieldStatsInfo().get(timestampFieldName);
        Collection<Object> distinctValues = stats.getDistinctValues();
        LOGGER.debug("Queried successfully for distinctTimestamps", Map.of("distinctSize", distinctValues == null ? 0 : distinctValues.size(), NODE_NAME, nodeName, "collection", collection, "query", String.valueOf(query.getQuery()), "filters", query.getFilterQueries() == null || query.getFilterQueries().length == 0 ? "null" : Strings.join(query.getFilterQueries(), " , ")));
        if (distinctValues == null) {
            return asList(range);
        }
        Set<Long> uniqueTimestamps = new HashSet<>(distinctValues.stream().map(o -> o instanceof Number ? ((Number) o).longValue() : parseLong(o.toString())).collect(Collectors.toSet()));
        uniqueTimestamps.add(range.getFrom());
        uniqueTimestamps.add(range.getTo());
        ArrayList<Long> timestamps = new ArrayList<>(uniqueTimestamps);
        Collections.sort(timestamps);
        List<OffsetRange> ranges = new ArrayList<>();
        int limit = timestamps.size() - 1;
        for (int i = 0; i < limit; i++) {
            OffsetRange offsetRange = new OffsetRange(timestamps.get(i), timestamps.get(i + 1));
            ranges.add(offsetRange);
        }
        return ranges;
    }

    protected long getNextAvailableTimestamp(final long startInclusive, final long end) throws SolrServerException, IOException {
        SolrClient client = client();
        SolrQuery query = new SolrQuery();
        query.setQuery(QUERY_ALL);
        query.setFilterQueries(String.format("%s:[%d TO %d}", timestampFieldName, startInclusive, end));
        query.setStart(0);
        query.setRows(0);
        query.setGetFieldStatistics(true);
        query.setGetFieldStatistics(timestampFieldName);
        QueryRequest queryRequest = new QueryRequest(query);
        if (option.isUseBasicAuth()) queryRequest.setBasicAuthCredentials(this.user, this.password);
        QueryResponse queryResponse = (option.getCollection() == null) ? queryRequest.process(client) : queryRequest.process(client, option.getCollection());
        FieldStatsInfo stats = queryResponse.getFieldStatsInfo().get(timestampFieldName);
        Object min = stats.getMin();
        if (min == null || stats.getCount() <= 0) {
            return -1;
        }
        return min instanceof Number ? ((Number) min).longValue() : parseLong(stats.getMin().toString());
    }

    @SuppressWarnings("unchecked")
    protected boolean isBatchComplete(final long batchId) throws Exception {
        String stateId = getStateId(batchId);
        LOGGER.debug("Checking state for State ID", Map.of("batchId", batchId, "stateId", stateId, NODE_NAME, nodeName, "collection", collection));
        long count = getCountForBatch(batchId);
        LOGGER.debug("Current Count of records in batch", Map.of("batchId", batchId, "stateId", stateId, "countRecords", count, NODE_NAME, nodeName, "collection", collection));
        Map<String, List<Long>> stateMap = getState(stateId);
        int numStateEntries = 0;
        Set<Long> uniqueCountsSet = new HashSet<>();
        if (stateMap != null) {
            LOGGER.debug("Current Stored State", Map.of("batchId", batchId, "stateId", stateId, "stateMap", stateMap, NODE_NAME, nodeName, "collection", collection));
            List<Long> batchCountList = stateMap.get(stateId);
            List<Long> tail = batchCountList.subList(Math.max(batchCountList.size() - 3, 0), batchCountList.size());
            uniqueCountsSet.addAll(tail);
            LOGGER.debug("Current Number of unique counts", Map.of("batchId", batchId, "stateId", stateId, "uniqueCounts", uniqueCountsSet.size(), NODE_NAME, nodeName, "collection", collection));
            numStateEntries = batchCountList.size();
            LOGGER.debug("Current Number of state entries for batch", Map.of("batchId", batchId, "stateId", stateId, "numStateEntries", numStateEntries, NODE_NAME, nodeName, "collection", collection));
            batchCountList.add(count);
            stateMap.put(stateId, batchCountList);
        } else {
            stateMap = Map.of(stateId, List.of(count));
            LOGGER.debug("Adding new State", Map.of("batchId", batchId, "stateId", stateId, "stateMap", stateMap, NODE_NAME, nodeName, "collection", collection));
        }
        saveState(stateId, stateMap);

        // TODO: 4/25/22 Clear state entries upon batch completion. This data is small but for a very long running process this is constantly growing.
        return numStateEntries >= option.getBatchEndNumChecks() && uniqueCountsSet.size() == 1;
    }

    public String getStateId(final long batchId) {
        return schema.getName() + batchId;
    }

    protected long getCountForBatch(final long batchId) throws SolrServerException, IOException {
        SolrClient client = client();
        SolrQuery query = new SolrQuery();
        query.setQuery(QUERY_ALL);
        query.setFilterQueries(String.format("%s:%d", timestampFieldName, batchId));
        query.setStart(0);
        query.setRows(0);
        QueryRequest queryRequest = new QueryRequest(query);
        if (option.isUseBasicAuth()) queryRequest.setBasicAuthCredentials(this.user, this.password);
        LOGGER.debug("Count Query", Map.of("batchId", batchId, "query", query.toString(), NODE_NAME, nodeName, "collection", collection));
        QueryResponse queryResponse = (option.getCollection() == null) ? queryRequest.process(client) : queryRequest.process(client, option.getCollection());
        return queryResponse.getResults().getNumFound();
    }

    protected long processSolrClaim(final ProcessContext ctx, final Configuration config, final long start, final long end) {
        return processSolrClaim(ctx, config, start, end, false);
    }

    protected long processSolrClaim(final ProcessContext ctx, final Configuration config, final long start, final long end, final Boolean deleteBatch) {
        LOGGER.debug("Processing solr query with range", Map.of("start", start, "end", end, NODE_NAME, nodeName, "collection", collection));
        long claimStartTime = System.nanoTime();
        SolrQuery query = constructSolrQuery(option, timestampFieldName != null ? (start == end ? String.format("%s:%d", timestampFieldName, start) : String.format("%s:[%d TO %d}", timestampFieldName, start, end)) : null);
        long recordsProcessed = processSolrQuery(client(), option, query, user, password, nodeName, nodeType, option.getQueryRetryCount(), option.getQueryRetryDuration(), doc -> {
            counter(nodeName, nodeType, UNKNOWN, ELEMENTS_IN).inc();
            long startTime = System.nanoTime();
            String responseKey = null;
            String schemaId = UNKNOWN;
            try {
                if (deleteBatch) doc.setField(option.getDeleteAttributeName(), true); // TODO: 4/25/22 Need to understand long term performance implications
                GenericRecord response = converter.convert(doc);
                responseKey = keyTemplateName == null ? UUID.randomUUID().toString() : evaluateFreemarker(config, keyTemplateName, null, doc, schema);
                ctx.output(KV.of(responseKey, response));
                schemaId = response.getSchema().getFullName();
                histogramDuration(nodeName, nodeType, schemaId, "avro_conversion").observe((System.nanoTime() - startTime) / 1000000.0);
                incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, SOURCE_SCHEMA_ID, schemaId);
            } catch (Exception e) {
                counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
                throw new GenericException(e.getMessage(), Map.of("responseKey", String.valueOf(responseKey), ERROR_MESSAGE, String.valueOf(e.getMessage())), e).wrap();
            }
        });
        histogramDuration(nodeName, nodeType, UNKNOWN, "range_process").observe((System.nanoTime() - claimStartTime) / 1000000.0);
        return recordsProcessed;
    }

}
