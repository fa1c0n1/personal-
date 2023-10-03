package com.apple.aml.stargate.beam.sdk.io.file;

import com.apple.aml.stargate.beam.sdk.formatters.GenericRecordToPartitionText;
import com.apple.aml.stargate.beam.sdk.utils.FileWriterFns.WriterKey;
import com.apple.aml.stargate.beam.sdk.utils.FileWriterFns.WriterValue;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.constants.PipelineConstants.DATA_FORMAT;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.options.FileOptions;
import com.apple.aml.stargate.common.options.KVFilterOptions;
import com.apple.aml.stargate.common.options.SchemaLevelOptions;
import com.apple.aml.stargate.common.pojo.AvroRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
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
import static com.apple.aml.stargate.beam.sdk.utils.FileWriterFns.codecName;
import static com.apple.aml.stargate.beam.sdk.utils.FileWriterFns.createFileWriter;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.ERROR_TAG;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_WRITTEN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.WRITE_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.WRITE_RETRY;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.WRITE_SUCCESS;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_LATEST_VERSION;
import static com.apple.aml.stargate.common.constants.PipelineConstants.BATCH_WRITER_CLOSE_TYPE.retryPrevious;
import static com.apple.aml.stargate.common.constants.PipelineConstants.BATCH_WRITER_CLOSE_TYPE.tearDown;
import static com.apple.aml.stargate.common.constants.PipelineConstants.DEFAULT_MAPPING;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.ErrorUtils.eRecord;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.getInternalSchema;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogram;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.isDynamicKerberosLoginEnabled;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.performKerberosLogin;
import static java.util.Arrays.asList;

public class BaseFileWriter<Input> extends AbstractWriter<Input, ConcurrentHashMap<WriterKey, WriterValue>> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final ConcurrentHashMap<String, ConcurrentHashMap<WriterKey, WriterValue>> GLOBAL_WRITERS = new ConcurrentHashMap<>();
    protected final KVFilterOptions filter;
    protected final SchemaLevelOptions defaultOptions;
    protected final String nodeName;
    protected final String nodeType;
    protected final String basePath;
    protected final DATA_FORMAT fileFormat;
    protected final CompressionCodecName codecName;
    protected final ConcurrentHashMap<String, GenericRecordToPartitionText> partitionerMap;
    protected final Schema outputSchema;
    protected final ObjectToGenericRecordConverter outputSchemaConverter;
    private final boolean emit;
    private final Map<String, SchemaLevelOptions> mappings;
    protected transient ConcurrentHashMap<WriterKey, WriterValue> writers;
    protected boolean kerberosLoginEnabled;
    private boolean useDirectBuffer;
    private boolean useBulkWriter;

    public BaseFileWriter(final String nodeName, final String nodeType, final PipelineConstants.ENVIRONMENT environment, final FileOptions options, final boolean emit) {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.filter = options.getOutputFilter();
        this.emit = emit;
        this.useDirectBuffer = options.isUseDirectBuffer();
        this.useBulkWriter = options.isUseBulkWriter();
        this.mappings = options.mappings();
        this.defaultOptions = this.mappings.get(DEFAULT_MAPPING);
        this.basePath = options.basePath();
        this.fileFormat = options.fileFormat();
        this.codecName = codecName(options);
        this.partitionerMap = new ConcurrentHashMap<>();
        this.outputSchema = getInternalSchema(environment, "filewriter.avsc", "{\"type\": \"record\",\"name\": \"FileDetails\",\"namespace\": \"com.apple.aml.stargate.#{ENV}.internal\",\"fields\": [{\"name\": \"fileName\",\"type\": \"string\"}]}");
        this.outputSchemaConverter = converter(this.outputSchema);
        this.kerberosLoginEnabled = isDynamicKerberosLoginEnabled();
    }

    public boolean enableDirectBuffer() {
        return useDirectBuffer;
    }

    public void setup() throws Exception {
        if (kerberosLoginEnabled) performKerberosLogin();
        writers = initBatch();
    }

    public void tearDown(final WindowedContext nullableContext) throws Exception {
        if (writers.isEmpty()) return;
        closeBatch(writers, nullableContext, tearDown);
    }

    @SuppressWarnings("unchecked")
    public KV<String, GenericRecord> process(final KV<String, GenericRecord> kv, final ConcurrentHashMap<WriterKey, WriterValue> writerMap, final WindowedContext context, final PaneInfo paneInfo, final BoundedWindow window, final Instant timestamp) throws Exception {
        return processWithRetry(kv, writerMap, context, paneInfo, window, timestamp, false);
    }

    public void consumeBuffer(final KV<String, GenericRecord> kv, final WindowedContext context, final PaneInfo paneInfo, final BoundedWindow window, final Instant timestamp) throws Exception {
        consume(kv, writers, context, window, paneInfo);
    }

    public void closeBatch(final ConcurrentHashMap<WriterKey, WriterValue> writerMap, final WindowedContext context, final PipelineConstants.BATCH_WRITER_CLOSE_TYPE batchType) throws Exception {
        if (writerMap.isEmpty()) {
            LOGGER.debug("No writers created in this batch. Will skip this batch");
            return;
        }
        Exception latestException = null;
        int writerCount = writerMap.size();
        LOGGER.debug("Processing generic file io closeBatch", Map.of("writerCount", writerCount, NODE_NAME, nodeName, NODE_TYPE, nodeType));
        int totalRecordCount = 0;

        for (WriterKey writerKey : new ArrayList<>(writerMap.keySet())) {
            WriterValue value = writerMap.remove(writerKey);
            if (value == null) continue;
            Map writerLogInfo = writerKey.logMap();
            int count = value.count();
            totalRecordCount += count;
            boolean closed = false;
            try {
                value.close();
                closed = true;
            } catch (Exception e) {
                LOGGER.error("Error while closing file writer for file : " + value.getFileName() + ". Reason - " + e.getMessage(), writerLogInfo, e);
                if (count > 0) {
                    latestException = e;
                }
            }
            if (closed) {
                LOGGER.debug("File writer closed successfully for", Map.of("fileName", value.getFileName(), "recordCount", count, "totalRecordCount", totalRecordCount), writerLogInfo);
            } else {
                LOGGER.warn("File writer could not be closed for", Map.of("fileName", value.getFileName(), "recordCount", count, "totalRecordCount", totalRecordCount), writerLogInfo);
            }
            if (count <= 0) {
                try {
                    ResourceId resourceId = FileSystems.matchNewResource(value.getFileName(), false);
                    LOGGER.debug("File has 0 records in it. Hence will delete it", Map.of("fileName", value.getFileName()), writerLogInfo);
                    FileSystems.delete(asList(resourceId));
                } catch (Exception e) {
                    LOGGER.warn("Could not delete empty file :" + value.getFileName(), writerLogInfo, e);
                }
            }
        }
        if (latestException != null) {
            throw latestException;
        }
        LOGGER.debug("Generic file io writers closed successfully", Map.of("writerCount", writerCount, "totalRecordCount", totalRecordCount, NODE_NAME, nodeName, NODE_TYPE, nodeType));
    }

    public ConcurrentHashMap<WriterKey, WriterValue> initBatch() {
        return useDirectBuffer ? writers == null ? GLOBAL_WRITERS.computeIfAbsent(nodeName, x -> new ConcurrentHashMap<>()) : writers : new ConcurrentHashMap<>();
    }

    @SuppressWarnings("unchecked")
    public KV<String, GenericRecord> processWithRetry(final KV<String, GenericRecord> kv, final ConcurrentHashMap<WriterKey, WriterValue> writerMap, final WindowedContext context, final PaneInfo paneInfo, final BoundedWindow window, final Instant timestamp, final boolean throwOnError) throws Exception {
        long startTime = System.nanoTime();
        final GenericRecord record = kv.getValue();
        final Schema schema = record.getSchema();
        final String schemaId = schema.getFullName();
        final String sourceSchemaId = schema.getFullName();
        final int version = record instanceof AvroRecord ? ((AvroRecord) record).getSchemaVersion() : SCHEMA_LATEST_VERSION;
        counter(nodeName, nodeType, schemaId, ELEMENTS_IN).inc();
        try {
            final String recordKey = kv.getKey();
            SchemaLevelOptions options = mappings.getOrDefault(schemaId, defaultOptions);
            GenericRecordToPartitionText partitioner = partitionerMap.computeIfAbsent(schemaId, s -> partitioner(options, this.nodeName, this.nodeType, fileFormat));
            WriterValue writer = getWriterValue(kv, writerMap, options, paneInfo, window, timestamp, schema, schemaId, version, partitioner);
            try {
                writer.write(kv);
                histogramDuration(nodeName, nodeType, schemaId, WRITE_SUCCESS).observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception e) {
                if (throwOnError) throw e;
                WriterKey failedKey = writer.getWriterKey();
                List<WriterValue> failedWriters = new ArrayList<>();
                WriterValue errorWriter = writerMap.remove(failedKey);
                failedWriters.add(writer);
                if (errorWriter != writer && errorWriter != null) failedWriters.add(errorWriter);
                histogram(nodeName, nodeType, schemaId, WRITE_ERROR).observe((System.nanoTime() - startTime) / 1000000.0);
                List<KV<String, GenericRecord>> previousRecords = new ArrayList<>();
                long retryLogicStartTime = System.nanoTime();
                List<KV<String, GenericRecord>> failedRecords = new ArrayList<>();
                failedRecords.add(kv);
                for (WriterValue failedWriter : failedWriters) {
                    try {
                        failedRecords.addAll(failedWriter.getSuccessRecords());
                        LOGGER.warn("Error writing record to destination. In most cases, file associated with this writer got corrupted. Hence aborting this file", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "previousRecordCount", failedWriter.getSuccessRecords().size()), failedKey.logMap(), e);
                        failedWriter.abort();
                    } catch (Exception closeError) {
                        LOGGER.warn("Error in aborting/closing corrupted batch file. Will do nothing..", Map.of(ERROR_MESSAGE, String.valueOf(closeError.getMessage()), "previousRecordCount", failedWriter.getSuccessRecords().size()), failedKey.logMap(), closeError);
                    }
                }
                if (!previousRecords.isEmpty()) {
                    ConcurrentHashMap<WriterKey, WriterValue> localMap = new ConcurrentHashMap<>();
                    try {
                        for (KV<String, GenericRecord> prev : previousRecords) {
                            counter(nodeName, nodeType, prev.getValue().getSchema().getFullName(), WRITE_RETRY).inc();
                            processWithRetry(prev, localMap, context, paneInfo, window, timestamp, true);
                        }
                        closeBatch(localMap, context, retryPrevious);
                    } catch (Exception retryError) {
                        LOGGER.warn("Error writing record to destination. Retry previous success records also failed. Will close this file and mark all records associated with this writer as failed", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "alreadyWrittenCount", previousRecords.size()), failedKey.logMap(), e);
                        for (WriterValue localWriter : localMap.values()) {
                            try {
                                localWriter.abort();
                            } catch (Exception closeError) {
                                LOGGER.warn("Error in aborting/closing corrupted batch file. Will do nothing..", Map.of(ERROR_MESSAGE, String.valueOf(closeError.getMessage()), "retryErrorMessage", String.valueOf(retryError.getMessage())), localWriter.getWriterKey().logMap(), closeError);
                            }
                        }
                        failedRecords.addAll(previousRecords);
                    }
                }
                int abortCount = failedRecords.size();
                counter(nodeName, nodeType, schemaId, "aborted").inc(abortCount);
                Map<String, Object> errorInfo = new HashMap<>(failedKey.logMap());
                errorInfo.put("failedKey", kv.getKey());
                errorInfo.put("failedSchemaId", schemaId);
                errorInfo.put("failedErrorMessage", String.valueOf(e.getMessage()));
                GenericException exception = new GenericException("Failing whole batch because of single record failure", errorInfo);
                for (KV<String, GenericRecord> failed : failedRecords) {
                    counter(nodeName, nodeType, schemaId, WRITE_ERROR).inc();
                    context.output(ERROR_TAG, eRecord(nodeName, nodeType, "write_record", failed, failed.equals(kv) ? e : exception));
                }
                histogramDuration(nodeName, nodeType, schemaId, "write_error_retry").observe((System.nanoTime() - retryLogicStartTime) / 1000000.0);
                return null;
            }
            counter(nodeName, nodeType, schemaId, ELEMENTS_WRITTEN).inc();
            if (emit && (this.filter == null || this.filter.shouldOutput(recordKey, schemaId))) {
                String outputKey = partitioner.outputKey(kv, paneInfo, window, timestamp);
                emitMetrics(schemaId, sourceSchemaId, writer.getWriterKey().getFilePath(), options);
                return KV.of(outputKey, options.isPassThrough() ? kv.getValue() : this.outputSchemaConverter.convert(Map.of("fileName", writer.getFileName(), "recordCount", writer.count())));
            }
            return null;
        } finally {
            histogramDuration(nodeName, nodeType, schemaId, "process").observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

    public void emitMetrics(final String schemaId, final String sourceSchemaId, final String filePath, final SchemaLevelOptions options) {
        incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, SOURCE_SCHEMA_ID, sourceSchemaId);
    }

    public void consume(final KV<String, GenericRecord> kv, final ConcurrentHashMap<WriterKey, WriterValue> writerMap, final WindowedContext context, final BoundedWindow window, final PaneInfo paneInfo) throws Exception {
        if (kv == null) {
            LOGGER.warn("Null kv record found. Will skip this record");
            return;
        }
        GenericRecord record = kv.getValue();
        if (record == null) {
            LOGGER.warn("Null avro record found. Will skip this record");
            return;
        }
        KV<String, GenericRecord> rkv = process(kv, writerMap, context, paneInfo, window, Instant.now());
        if (rkv != null) {
            context.output(rkv);
        }
    }

    public WriterValue getWriterValue(final KV<String, GenericRecord> kv, final ConcurrentHashMap<WriterKey, WriterValue> writerMap, final SchemaLevelOptions options, final PaneInfo paneInfo, final BoundedWindow window, final Instant timestamp, final Schema schema, final String schemaId, final int version, final GenericRecordToPartitionText partitioner) throws Exception {
        Triple<String, String, Instant> tokens = partitioner.tokens(kv, paneInfo, window, timestamp);
        final WriterKey writerKey = new WriterKey(nodeName, nodeType, schemaId, version, window, paneInfo, tokens.getLeft(), tokens.getMiddle(), tokens.getRight());
        WriterValue writer = writerMap.computeIfAbsent(writerKey, wk -> {
            try {
                return createFileWriter(wk, schema, fileFormat.name(), basePath, fileFormat, codecName, useBulkWriter, options);
            } catch (Exception e) {
                throw new GenericException("Could not create writer for writerKey", Map.of(SCHEMA_ID, schemaId, "paneIndex", writerKey.getPaneIndex(), "windowStart", writerKey.getStart(), "windowEnd", writerKey.getEnd(), "workerId", writerKey.getWorkerId(), "filePath", writerKey.getFilePath(), "partitionPath", writerKey.getPartitionPath()), e).wrap();
            }
        });
        return writer;
    }

}
