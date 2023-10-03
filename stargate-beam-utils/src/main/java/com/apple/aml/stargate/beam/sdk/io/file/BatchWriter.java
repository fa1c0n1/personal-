package com.apple.aml.stargate.beam.sdk.io.file;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.options.WindowOptions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.gauge;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramSize;

public class BatchWriter<A, B> extends DoFn<KV<A, KV<String, GenericRecord>>, KV<String, GenericRecord>> implements Serializable {
    protected static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final long serialVersionUID = 1L;
    private static final ConcurrentHashMap<String, ConcurrentHashMap<String, ArrayList<KV<String, GenericRecord>>>> BUFFER = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, ConcurrentHashMap<String, Timer>> BATCH_TIMER = new ConcurrentHashMap<>();
    protected final AbstractWriter<KV<A, KV<String, GenericRecord>>, B> fileWriter;
    protected final int batchSize;
    protected final Duration windowDelay;
    protected final Duration batchDuration;
    protected final String nodeName;
    protected final String nodeType;
    protected final boolean appendPartitionNoToKey;
    protected transient ProcessContext lastKnownContext;
    protected transient BoundedWindow lastKnownWindow;


    public BatchWriter(final String nodeName, final String nodeType, final PipelineConstants.ENVIRONMENT environment, final WindowOptions options, final AbstractWriter<KV<A, KV<String, GenericRecord>>, B> fileWriter) {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.fileWriter = fileWriter;
        this.batchSize = options.getBatchSize() <= 0 ? Integer.MAX_VALUE : options.getBatchSize();
        this.windowDelay = Duration.standardSeconds(options.getWindowLateness().getSeconds());
        this.batchDuration = Duration.standardSeconds(options.batchDuration().getSeconds());
        this.appendPartitionNoToKey = options.isAppendPartitionNoToKey();
        LOGGER.debug("Created batchWriter", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, "enableDirectBuffer", fileWriter.enableDirectBuffer(), "batchSize", this.batchSize, "batchDuration", this.batchDuration));
    }

    private static AtomicLong getCounter(final ConcurrentHashMap<String, ConcurrentHashMap<String, AtomicLong>> counter, final String nodeName, final String schemaId) {
        return counter.computeIfAbsent(nodeName, n -> new ConcurrentHashMap<>()).computeIfAbsent(schemaId, s -> new AtomicLong());
    }

    @Setup
    public void setup() throws Exception {
        fileWriter.setup();
    }

    @Teardown
    public void closeFileWriter() throws Exception {
        if (lastKnownContext != null) {
            BUFFER.get(nodeName).forEach((schemaId, buffer) -> {
                if (buffer.isEmpty()) return;
                List<KV<String, GenericRecord>> iterable;
                synchronized (buffer) {
                    iterable = new ArrayList<>(buffer);
                    buffer.clear();
                }
                long startTime = System.nanoTime();
                try {
                    consumeRecords(schemaId, lastKnownContext, lastKnownWindow, Instant.now(), lastKnownContext.pane(), PipelineConstants.BATCH_WRITER_CLOSE_TYPE.tearDown, iterable);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    histogramDuration(nodeName, nodeType, schemaId, "batch_consume_tear_down").observe((System.nanoTime() - startTime) / 1000000.0);
                }
            });
        }
        fileWriter.tearDown(lastKnownContext);
    }

    private void consumeRecords(final String schemaId, final WindowedContext context, final BoundedWindow window, final Instant timestamp, final PaneInfo paneInfo, final PipelineConstants.BATCH_WRITER_CLOSE_TYPE batchType, final List<KV<String, GenericRecord>> iterable) throws Exception {
        if (iterable.isEmpty()) return;
        long startTime = System.nanoTime();
        Timer batchTimer = BATCH_TIMER.computeIfAbsent(nodeName, n -> new ConcurrentHashMap<>()).remove(schemaId);
        if (batchTimer != null && batchType != PipelineConstants.BATCH_WRITER_CLOSE_TYPE.batchTimer) {
            try {
                batchTimer.cancel();
            } catch (Exception e) {
                LOGGER.debug("Error cancelling batch/stale timer! Will ignore this exception", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, schemaId, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            }
        }
        long currentBatchSize = iterable.size();
        LOGGER.debug("Batch consumption started", Map.of("currentCount", currentBatchSize, "type", batchType.name(), NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, schemaId));
        B batch = fileWriter.initBatch();
        histogramDuration(nodeName, nodeType, schemaId, "batch_init").observe((System.nanoTime() - startTime) / 1000000.0);
        histogramSize(nodeName, nodeType, schemaId, "batch_size").observe(currentBatchSize);
        gauge(nodeName, nodeType, schemaId, "batch_size").set(currentBatchSize);
        long count = 0;
        startTime = System.nanoTime();
        gauge(nodeName, nodeType, schemaId, "batch_epoc_nano").set(startTime);
        if (!fileWriter.enableDirectBuffer()) {
            if (fileWriter.enableRawConsumption()) {
                count = fileWriter.consumeRaw(iterable, batch, context, paneInfo, window, timestamp);
            } else {
                for (KV<String, GenericRecord> kv : iterable) {
                    KV<String, GenericRecord> rkv = fileWriter.process(kv, batch, context, paneInfo, window, timestamp);
                    count++;
                    if (rkv == null) continue;
                    context.output(rkv);
                }
            }
        }
        double timeTaken = (System.nanoTime() - startTime) / 1000000.0;
        histogramDuration(nodeName, nodeType, schemaId, "batch_consume").observe(timeTaken);
        gauge(nodeName, nodeType, schemaId, "batch_consume").set(timeTaken);
        startTime = System.nanoTime();
        fileWriter.closeBatch(batch, context, batchType);
        timeTaken = (System.nanoTime() - startTime) / 1000000.0;
        histogramDuration(nodeName, nodeType, schemaId, "batch_close").observe(timeTaken);
        gauge(nodeName, nodeType, schemaId, "batch_close").set(timeTaken);
        counter(nodeName, nodeType, schemaId, "batch_elements_out").inc(count);
    }

    @SuppressWarnings("unchecked")
    @ProcessElement
    public void processElement(final ProcessContext context, final BoundedWindow window, final PaneInfo paneInfo) throws Exception {
        long startTime = System.nanoTime();
        String schemaId = UNKNOWN;
        try {
            KV<String, GenericRecord> kv = updatedKv(context, window);
            if (kv == null) return;
            GenericRecord record = kv.getValue();
            lastKnownContext = context;
            lastKnownWindow = window;
            schemaId = record.getSchema().getFullName();
            counter(nodeName, nodeType, schemaId, "batch_elements_in").inc();
            String recordSchemaId = schemaId;
            KV<String, GenericRecord> bufferKV = fileWriter.enableDirectBuffer() ? KV.of(kv.getKey(), null) : kv;
            List<KV<String, GenericRecord>> buffer = BUFFER.computeIfAbsent(nodeName, n -> new ConcurrentHashMap<>()).computeIfAbsent(schemaId, s -> new ArrayList<>());
            synchronized (buffer) {
                buffer.add(bufferKV);
            }
            BATCH_TIMER.computeIfAbsent(nodeName, n -> new ConcurrentHashMap<>()).computeIfAbsent(schemaId, s -> {
                Timer timer = new Timer(String.format("batch-timer-%s-%s-%s", nodeName, recordSchemaId, window));
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        List<KV<String, GenericRecord>> buffer = BUFFER.computeIfAbsent(nodeName, n -> new ConcurrentHashMap<>()).computeIfAbsent(recordSchemaId, s -> new ArrayList<>());
                        if (buffer.isEmpty()) return;
                        List<KV<String, GenericRecord>> iterable;
                        synchronized (buffer) {
                            iterable = new ArrayList<>(buffer);
                            buffer.clear();
                        }
                        long startTime = System.nanoTime();
                        try {
                            consumeRecords(recordSchemaId, context, window, Instant.now(), paneInfo, PipelineConstants.BATCH_WRITER_CLOSE_TYPE.batchTimer, iterable);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        } finally {
                            histogramDuration(nodeName, nodeType, recordSchemaId, "batch_consume_timer").observe((System.nanoTime() - startTime) / 1000000.0);
                        }
                    }
                }, batchDuration.getMillis());
                return timer;
            });
            if (fileWriter.enableDirectBuffer()) {
                fileWriter.consumeBuffer(kv, context, paneInfo, window, Instant.now()); // this will create inconsistency with counter states ( coz one is persisted & one is in memory ); however there is no harm except counts are not honored
            }
            if (buffer.size() >= batchSize) {
                List<KV<String, GenericRecord>> iterable;
                synchronized (buffer) {
                    iterable = new ArrayList<>(buffer);
                    buffer.clear();
                }
                Timer timer = new Timer(String.format("batch-immediate-%s-%s-%s", nodeName, recordSchemaId, window));
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        long consumeStartTime = System.nanoTime();
                        try {
                            consumeRecords(recordSchemaId, context, window, Instant.now(), paneInfo, PipelineConstants.BATCH_WRITER_CLOSE_TYPE.batchSize, iterable);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        } finally {
                            histogramDuration(nodeName, nodeType, recordSchemaId, "batch_consume_size").observe((System.nanoTime() - consumeStartTime) / 1000000.0);
                        }
                    }
                }, 1);
            }
        } finally {
            histogramDuration(nodeName, nodeType, schemaId, "batch_queue").observe((System.nanoTime() - startTime) / 1000000.0);
        }
    }

    protected KV<String, GenericRecord> updatedKv(final ProcessContext context, final BoundedWindow window) {
        KV<String, GenericRecord> ikv = context.element().getValue();
        if (appendPartitionNoToKey) {
            String partition = context.element().getKey().toString();
            ikv = KV.of(ikv.getKey() + "~" + partition, ikv.getValue());
        }
        String key = ikv.getKey();
        GenericRecord record = ikv.getValue();
        if (record == null) {
            counter(nodeName, nodeType, UNKNOWN, "batch_elements_skipped").inc();
            LOGGER.warn("Null record found while batching. Will skip this record!", Map.of("key", String.valueOf(key), NODE_NAME, nodeName, NODE_TYPE, nodeType, "window", String.valueOf(window)));
            return null;
        }
        return ikv;
    }
}
