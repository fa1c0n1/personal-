package com.apple.aml.stargate.beam.sdk.io.solr;

import com.apple.aml.stargate.beam.sdk.formatters.GenericRecordToPartitionText;
import com.apple.aml.stargate.beam.sdk.io.file.AbstractWriter;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.GenericRecordToSolrInputDocumentConverter;
import com.apple.aml.stargate.common.options.SchemaLevelOptions;
import com.apple.aml.stargate.common.options.SolrOptions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.joda.time.Instant;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.beam.sdk.formatters.GenericRecordToPartitionText.partitioner;
import static com.apple.aml.stargate.beam.sdk.io.solr.GenericSolrIO.solrClient;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.COLLECTION_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.PipelineConstants.DEFAULT_MAPPING;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_CONF;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_KEYTAB;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.performSolrKrb5Login;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class SolrWriter<Input> extends AbstractWriter<Input, Void> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final SolrOptions options;
    private final boolean emit;
    private final String nodeName;
    private final String nodeType;
    private final Map<String, String> authConfigFiles;
    private final String user;
    private final String password;
    private final Map<String, SchemaLevelOptions> mappings;
    private final SchemaLevelOptions defaultOptions;
    private final ConcurrentHashMap<String, GenericRecordToPartitionText> partitionerMap;
    private final String collection;
    private transient SolrClient client;

    public SolrWriter(final String nodeName, final String nodeType, final PipelineConstants.ENVIRONMENT env, final SolrOptions options, final boolean emit) throws Exception {
        this.options = options;
        this.emit = emit;
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.user = options.isUseBasicAuth() ? options.getUser() : null;
        this.password = options.isUseBasicAuth() ? options.getPassword() : null;
        this.collection = isBlank(options.getCollection()) ? null : options.getCollection().trim();
        this.mappings = collection == null ? options.mappings() : null;
        this.defaultOptions = collection == null ? this.mappings.get(DEFAULT_MAPPING) : null;
        this.partitionerMap = collection == null ? new ConcurrentHashMap<>() : null;
        authConfigFiles = new HashMap<>();
        authConfigFiles.put("KEYTAB", options.keytabFileName() + STARGATE_KEYTAB);
        authConfigFiles.put("KRB5", options.krb5FileName() + STARGATE_CONF);
        authConfigFiles.put("JAAS", options.jaasFileName() + STARGATE_CONF);
    }

    @Override
    protected void setup() throws Exception {
        if (options.isKerberizationEnabled()) performSolrKrb5Login(authConfigFiles, options.isUseKrb5AuthDebug());
    }

    @Override
    public void tearDown(final WindowedContext nullableContext) throws Exception {
        if (client != null) client.close();
    }

    @Override
    protected KV<String, GenericRecord> process(final KV<String, GenericRecord> kv, final Void batch, final WindowedContext context, final PaneInfo paneInfo, final BoundedWindow window, final Instant timestamp) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean enableRawConsumption() {
        return true;
    }

    @Override
    protected int consumeRaw(final List<KV<String, GenericRecord>> iterable, final Void ignored, final WindowedContext context, final PaneInfo paneInfo, final BoundedWindow window, final Instant timestamp) throws Exception {
        ConcurrentHashMap<Pair<String, String>, List<SolrInputDocument>> map = new ConcurrentHashMap<>();
        ConcurrentHashMap<Pair<String, String>, List<KV<String, GenericRecord>>> records = new ConcurrentHashMap<>();
        long startTime = System.nanoTime();
        int success = 0;
        iterable.forEach(kv -> {
            long docPrepStartTime = System.nanoTime();
            GenericRecord record = kv.getValue();
            final Schema schema = record.getSchema();
            final String schemaId = schema.getFullName();
            String collectionName = collection;
            Pair<String, String> mapKey;
            SolrInputDocument doc;
            try {
                counter(nodeName, nodeType, schemaId, ELEMENTS_IN).inc();
                if (collection == null) {
                    GenericRecordToPartitionText partitioner = partitionerMap.computeIfAbsent(schemaId, s -> partitioner(mappings.getOrDefault(schemaId, defaultOptions), this.nodeName, this.nodeType, null));
                    collectionName = partitioner.collectionName(kv, paneInfo, window, timestamp);
                }
                mapKey = Pair.of(schemaId, collectionName);
                doc = GenericRecordToSolrInputDocumentConverter.convertRecord(record);
                histogramDuration(nodeName, nodeType, schemaId, "solr_doc_prep_success").observe((System.nanoTime() - docPrepStartTime) / 1000000.0);
                map.computeIfAbsent(mapKey, k -> new ArrayList<>()).add(doc);
                records.computeIfAbsent(mapKey, k -> new ArrayList<>()).add(kv);
            } catch (Exception e) {
                histogramDuration(nodeName, nodeType, schemaId, "solr_doc_prep_error").observe((System.nanoTime() - docPrepStartTime) / 1000000.0);
                counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
                LOGGER.warn("Could not convert to solr document", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, "key", kv.getKey(), SCHEMA_ID, schemaId, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                context.output(ERROR_TAG, eRecord(nodeName, nodeType, "solr_doc_prep", kv, e));
            } finally {
                histogramDuration(nodeName, nodeType, schemaId, "solr_prep").observe((System.nanoTime() - docPrepStartTime) / 1000000.0);
            }
        });
        if (map.isEmpty()) return 0;
        histogramDuration(nodeName, nodeType, UNKNOWN, "batch_solr_doc_prep").observe((System.nanoTime() - startTime) / 1000000.0);
        List<KV<String, GenericRecord>> successRecords = new ArrayList<>();
        long clientStartTime = System.nanoTime();
        for (Map.Entry<Pair<String, String>, List<SolrInputDocument>> entry : map.entrySet()) {
            long batchTime = System.nanoTime();
            int batchSize = entry.getValue().size();
            String schemaId = entry.getKey().getLeft();
            String collectionName = entry.getKey().getRight();
            List<KV<String, GenericRecord>> kvs = records.remove(entry.getKey());
            try {
                UpdateRequest updateRequest = new UpdateRequest();
                if (options.isUseBasicAuth()) updateRequest.setBasicAuthCredentials(this.user, this.password);
                List<SolrInputDocument> updateDocsList = new ArrayList<>();
                List<String> deleteIdsList = new ArrayList<>();
                entry.getValue().stream().forEach(doc -> {
                    if (doc.containsKey(options.getDeleteAttributeName()) && "true".equalsIgnoreCase(String.valueOf(doc.getFieldValue(options.getDeleteAttributeName())))) {
                        deleteIdsList.add(String.valueOf(doc.getFieldValue(options.getSolrIdField())));
                    } else {
                        updateDocsList.add(doc);
                    }
                });
                if (updateDocsList.size() > 0) {
                    updateRequest.add(entry.getValue());
                }
                if (deleteIdsList.size() > 0) {
                    updateRequest.deleteById(deleteIdsList);
                }
                updateRequest.process(client, collectionName);
                if (options.isUseHardCommit()) {
                    updateRequest.commit(client, collectionName);
                }
                histogramDuration(nodeName, nodeType, schemaId, "solr_client_success", COLLECTION_NAME, collectionName).observe((System.nanoTime() - batchTime) / 1000000.0);
                success += batchSize;
                incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, batchSize, COLLECTION_NAME, collectionName, SOURCE_SCHEMA_ID, schemaId);
                successRecords.addAll(kvs);
            } catch (Exception e) {
                histogramDuration(nodeName, nodeType, schemaId, "solr_client_error").observe((System.nanoTime() - batchTime) / 1000000.0);
                counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
                LOGGER.warn("Could not save/persist solr document to remote server", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, "collection", collectionName, SCHEMA_ID, schemaId, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                kvs.forEach(kv -> context.output(ERROR_TAG, eRecord(nodeName, nodeType, "solr_client_request", kv, e)));
            } finally {
                histogramDuration(nodeName, nodeType, schemaId, "solr_client_time").observe((System.nanoTime() - batchTime) / 1000000.0);
            }
        }
        histogramDuration(nodeName, nodeType, UNKNOWN, "batch_solr_client_time").observe((System.nanoTime() - clientStartTime) / 1000000.0);
        histogramDuration(nodeName, nodeType, UNKNOWN, "process").observe((System.nanoTime() - startTime) / 1000000.0);
        if (emit) {
            successRecords.forEach(context::output);
        }
        return success;
    }

    @Override
    public void closeBatch(final Void batch, final WindowedContext context, final PipelineConstants.BATCH_WRITER_CLOSE_TYPE batchType) {
        // Closing client after each batch is not required for solr writer use case.
        // It will also cause problems during TGT renewal when using kerberos authentication.
    }

    @Override
    protected Void initBatch() throws Exception {
        initClient();
        return null;
    }

    private void initClient() {
        if (client == null) {
            client = solrClient(options);
        }
    }
}
