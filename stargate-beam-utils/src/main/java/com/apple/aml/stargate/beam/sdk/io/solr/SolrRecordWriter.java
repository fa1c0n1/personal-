package com.apple.aml.stargate.beam.sdk.io.solr;

import com.apple.aml.stargate.beam.sdk.formatters.GenericRecordToPartitionText;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.GenericRecordToSolrInputDocumentConverter;
import com.apple.aml.stargate.common.options.SchemaLevelOptions;
import com.apple.aml.stargate.common.options.SolrOptions;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.joda.time.Instant;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.beam.sdk.formatters.GenericRecordToPartitionText.partitioner;
import static com.apple.aml.stargate.beam.sdk.io.solr.GenericSolrIO.solrClient;
import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.COLLECTION_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
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

public class SolrRecordWriter extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final ConcurrentHashMap<String, SolrClient> CLIENT_MAP = new ConcurrentHashMap<>();
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

    public SolrRecordWriter(final String nodeName, final String nodeType, final PipelineConstants.ENVIRONMENT env, final SolrOptions options, final boolean emit) throws Exception {
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

    @ProcessElement
    @SuppressWarnings("unchecked")
    public void processElement(@Element final KV<String, GenericRecord> kv, final BoundedWindow window, final PaneInfo paneInfo, final ProcessContext ctx) throws Exception {
        long startTime = System.nanoTime();
        log(options, nodeName, nodeType, kv);
        final GenericRecord record = kv.getValue();
        final Schema schema = record.getSchema();
        final String schemaId = schema.getFullName();
        counter(nodeName, nodeType, schemaId, ELEMENTS_IN).inc();
        try {
            SolrClient client = CLIENT_MAP.computeIfAbsent(nodeName, n -> initClient());
            String collectionName = collection;
            SolrInputDocument doc;
            try {
                if (collection == null) {
                    GenericRecordToPartitionText partitioner = partitionerMap.computeIfAbsent(schemaId, s -> partitioner(mappings.getOrDefault(schemaId, defaultOptions), this.nodeName, this.nodeType, null));
                    collectionName = partitioner.collectionName(kv, paneInfo, window, Instant.now());
                }
                doc = GenericRecordToSolrInputDocumentConverter.convertRecord(record);
                histogramDuration(nodeName, nodeType, schemaId, "solr_doc_prep_success").observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception e) {
                histogramDuration(nodeName, nodeType, schemaId, "solr_doc_prep_error").observe((System.nanoTime() - startTime) / 1000000.0);
                counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
                LOGGER.warn("Could not convert to solr document", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, "key", kv.getKey(), SCHEMA_ID, schemaId, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                ctx.output(ERROR_TAG, eRecord(nodeName, nodeType, "solr_doc_prep", kv, e));
                return;
            }
            try {
                UpdateRequest updateRequest = new UpdateRequest();
                if (options.isUseBasicAuth()) updateRequest.setBasicAuthCredentials(this.user, this.password);
                updateRequest.add(doc);
                updateRequest.process(client, collectionName);
                if (options.isUseHardCommit()) updateRequest.commit(client, collectionName);
            } catch (Exception e) {
                histogramDuration(nodeName, nodeType, schemaId, "solr_client_error").observe((System.nanoTime() - startTime) / 1000000.0);
                counter(nodeName, nodeType, schemaId, ELEMENTS_ERROR).inc();
                LOGGER.warn("Could not save/persist solr document to remote server", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, "key", kv.getKey(), SCHEMA_ID, schemaId, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                ctx.output(ERROR_TAG, eRecord(nodeName, nodeType, "solr_client_request", kv, e));
                return;
            }
            histogramDuration(nodeName, nodeType, schemaId, "process_success", COLLECTION_NAME, collectionName).observe((System.nanoTime() - startTime) / 1000000.0);
            incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, COLLECTION_NAME, collectionName, SOURCE_SCHEMA_ID, schemaId);
            if (emit) ctx.output(kv);
        } finally {
            histogramDuration(nodeName, nodeType, schemaId, "process").observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

    @SneakyThrows
    private SolrClient initClient() {
        if (options.isKerberizationEnabled()) performSolrKrb5Login(authConfigFiles, options.isUseKrb5AuthDebug());
        return solrClient(options);
    }
}
