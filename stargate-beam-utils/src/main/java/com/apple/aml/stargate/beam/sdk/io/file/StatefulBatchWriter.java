package com.apple.aml.stargate.beam.sdk.io.file;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.options.WindowOptions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.springframework.util.unit.DataSize;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.PipelineConstants.BATCH_WRITER_CLOSE_TYPE;
import static com.apple.aml.stargate.common.constants.PipelineConstants.BATCH_WRITER_CLOSE_TYPE.batchTimer;
import static com.apple.aml.stargate.common.constants.PipelineConstants.BATCH_WRITER_CLOSE_TYPE.windowTimer;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.gauge;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramBytes;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramSize;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class StatefulBatchWriter<A, B> extends DoFn<KV<A, KV<String, GenericRecord>>, KV<String, GenericRecord>> implements Serializable {
    protected static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final long serialVersionUID = 1L;
    protected final AbstractWriter<KV<A, KV<String, GenericRecord>>, B> fileWriter;
    protected final int batchSize;
    protected final Duration windowDelay;
    protected final Duration batchDuration;
    protected final String nodeName;
    protected final String nodeType;
    protected final boolean appendPartitionNoToKey;
    @StateId("buffer")
    private final StateSpec<BagState<KV<String, GenericRecord>>> bufferStateSpec = StateSpecs.bag();
    @StateId("count")
    private final StateSpec<MapState<String, AtomicLong>> countStateSpec = StateSpecs.map();
    @StateId("bytes")
    private final StateSpec<MapState<String, AtomicLong>> bytesStateSpec = StateSpecs.map();
    @TimerId("windowTimer")
    private final TimerSpec windowTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);
    @TimerId("batchTimer")
    private final TimerSpec batchTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);
    private final long batchBytes;
    private final boolean batchBytesEnabled;
    protected transient ProcessContext lastKnownContext;
    protected transient BoundedWindow lastKnownWindow;

    public StatefulBatchWriter(final String nodeName, final String nodeType, final PipelineConstants.ENVIRONMENT environment, final WindowOptions options, final AbstractWriter<KV<A, KV<String, GenericRecord>>, B> fileWriter) {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.fileWriter = fileWriter;
        this.batchSize = options.getBatchSize() <= 0 ? Integer.MAX_VALUE : options.getBatchSize();
        this.windowDelay = Duration.standardSeconds(options.getWindowLateness().getSeconds());
        this.batchDuration = Duration.standardSeconds(options.batchDuration().getSeconds());
        this.appendPartitionNoToKey = options.isAppendPartitionNoToKey();
        this.batchBytes = isBlank(options.getBatchBytes()) ? 0 : DataSize.parse(options.getBatchBytes()).toBytes();
        this.batchBytesEnabled = this.batchBytes > 0;
        LOGGER.debug("Created batchWriter", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, "enableDirectBuffer", fileWriter.enableDirectBuffer(), "batchSize", this.batchSize, "batchDuration", this.batchDuration));
    }

    @Setup
    public void setup() throws Exception {
        fileWriter.setup();
    }

    @SuppressWarnings("unchecked")
    @ProcessElement
    public void processElement(final ProcessContext context, final BoundedWindow window, final PaneInfo paneInfo, final @StateId("buffer") BagState<KV<String, GenericRecord>> bufferState, final @StateId("count") MapState<String, AtomicLong> countState, final @StateId("bytes") MapState<String, AtomicLong> bytesState, final @TimerId("windowTimer") Timer windowTimer, final @TimerId("batchTimer") Timer batchTimer) throws Exception {
        KV<String, GenericRecord> kv = updatedKv(context, window);
        if (kv == null) return;
        GenericRecord record = kv.getValue();
        lastKnownContext = context;
        lastKnownWindow = window;
        String schemaId = record.getSchema().getFullName();
        counter(nodeName, nodeType, schemaId, "batch_elements_in").inc();
        windowTimer.set(window.maxTimestamp().plus(windowDelay));
        AtomicLong counter;
        long count = 0;
        synchronized (countStateSpec) {
            counter = countState.computeIfAbsent(schemaId, s -> new AtomicLong(0)).read();
            if (counter == null) counter = countState.get(schemaId).read();
            count = counter.addAndGet(1);
            countState.put(schemaId, counter);
        }
        if (count == 1) batchTimer.offset(batchDuration).setRelative();
        if (fileWriter.enableDirectBuffer()) {
            fileWriter.consumeBuffer(kv, context, paneInfo, window, Instant.now()); // this will create inconsistency with counter states ( coz one is persisted & one is in memory ); however there is no harm except counts are not honored
        } else {
            bufferState.add(kv);
        }
        long byteSize = 0;
        if (batchBytesEnabled) {
            AtomicLong byteSizer;
            synchronized (bytesStateSpec) {
                byteSizer = bytesState.computeIfAbsent(schemaId, s -> new AtomicLong(0)).read();
                if (byteSizer == null) byteSizer = bytesState.get(schemaId).read();
                byteSize = byteSizer.addAndGet(getByteSize(kv));
                bytesState.put(schemaId, byteSizer);
            }
        }
        if (count >= batchSize) {
            LOGGER.debug("Triggering batchSize reached consumption", Map.of("currentCount", counter, "currentByteSize", byteSize, "type", "batchSize", NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, schemaId));
            consume(schemaId, context, window, Instant.now(), paneInfo, bufferState, countState, bytesState, BATCH_WRITER_CLOSE_TYPE.batchSize);
            return;
        }
        if (batchBytesEnabled && byteSize >= batchBytes) {
            LOGGER.debug("Triggering batchBytes reached consumption", Map.of("currentCount", counter, "currentByteSize", byteSize, "type", "batchBytes", NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, schemaId));
            consume(schemaId, context, window, Instant.now(), paneInfo, bufferState, countState, bytesState, BATCH_WRITER_CLOSE_TYPE.batchBytes);
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

    protected long getByteSize(final KV<String, GenericRecord> kv) {
        // Please note : this is not the actual bytes occupied in storage. BatchWriter doesn't understand which Sink it will be linked to and how; Hence this is just approximation ( treating everything as JSON UTF8 String );
        // This is an intensive operation; Don't use unless really needed
        GenericRecord record = kv.getValue();
        return record.toString().getBytes(StandardCharsets.UTF_8).length;
    }

    private synchronized void consume(final String schemaId, final WindowedContext context, final BoundedWindow window, final Instant timestamp, final PaneInfo paneInfo, final BagState<KV<String, GenericRecord>> bufferState, final MapState<String, AtomicLong> countState, final MapState<String, AtomicLong> bytesState, final BATCH_WRITER_CLOSE_TYPE batchType) throws Exception {
        long startTime = System.nanoTime();
        AtomicLong counter = countState.get(schemaId).read();
        if (counter == null || counter.get() == 0) return;
        long countStateValue = counter.longValue();
        AtomicLong byteSizer = batchBytesEnabled ? bytesState.get(schemaId).read() : null;
        long bytesStateValue = byteSizer == null ? 0L : byteSizer.longValue();
        LOGGER.debug("Batch consumption started", Map.of("currentCount", countStateValue, "currentByteSize", bytesStateValue, "type", batchType.name(), NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, schemaId));
        B batch = fileWriter.initBatch();
        histogramDuration(nodeName, nodeType, schemaId, "batch_init").observe((System.nanoTime() - startTime) / 1000000.0);
        histogramSize(nodeName, nodeType, schemaId, "batch_size").observe(countStateValue);
        histogramBytes(nodeName, nodeType, schemaId, "batch_bytes").observe(bytesStateValue);
        long count = 0;
        startTime = System.nanoTime();
        gauge(nodeName, nodeType, schemaId, "batch_epoc_nano").set(startTime);
        if (!fileWriter.enableDirectBuffer()) {
            List<KV<String, GenericRecord>> remaining = new ArrayList<>();
            List<KV<String, GenericRecord>> iterable = new ArrayList<>((int) (countStateValue + 1));
            bufferState.read().forEach(kv -> {
                if (schemaId.equals(kv.getValue().getSchema().getFullName())) {
                    iterable.add(kv);
                } else {
                    remaining.add(kv);
                }
            });
            bufferState.clear();
            remaining.forEach(kv -> bufferState.add(kv));
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
        countState.remove(schemaId);
        if (byteSizer != null) {
            bytesState.remove(schemaId);
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

    @OnTimer("windowTimer")
    public void onWindowTimer(final OnTimerContext context, final @StateId("buffer") BagState<KV<String, GenericRecord>> bufferState, final @StateId("count") MapState<String, AtomicLong> countState, final @StateId("bytes") MapState<String, AtomicLong> bytesState) throws Exception {
        List<String> schemaIds = new ArrayList<>();
        countState.keys().read().forEach(s -> schemaIds.add(s));
        for (String schemaId : schemaIds) {
            LOGGER.debug("Triggering expiry timer consumption", Map.of("type", "windowTimer", NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, schemaId));
            consume(schemaId, context, context.window(), Instant.now(), null, bufferState, countState, bytesState, windowTimer);
        }
    }

    @OnTimer("batchTimer")
    public void onBatchTimer(final OnTimerContext context, final @StateId("buffer") BagState<KV<String, GenericRecord>> bufferState, final @StateId("count") MapState<String, AtomicLong> countState, final @StateId("bytes") MapState<String, AtomicLong> bytesState) throws Exception {
        List<String> schemaIds = new ArrayList<>();
        countState.keys().read().forEach(s -> schemaIds.add(s));
        for (String schemaId : schemaIds) {
            LOGGER.debug("Triggering batch/stale timer consumption", Map.of("type", "batchTimer", NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, schemaId));
            consume(schemaId, context, context.window(), Instant.now(), null, bufferState, countState, bytesState, batchTimer);
        }
    }

    @Teardown
    public void closeFileWriter() throws Exception {
        fileWriter.tearDown(lastKnownContext);
    }
}
