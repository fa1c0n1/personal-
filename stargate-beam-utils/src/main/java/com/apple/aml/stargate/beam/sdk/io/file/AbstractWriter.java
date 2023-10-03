package com.apple.aml.stargate.beam.sdk.io.file;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.List;

public abstract class AbstractWriter<Input, Batch> extends DoFn<Input, KV<String, GenericRecord>> implements Serializable {

    protected boolean enableDirectBuffer() {
        return false;
    }

    protected void setup() throws Exception {
    }

    protected void tearDown(final WindowedContext nullableContext) throws Exception {
    }

    protected abstract KV<String, GenericRecord> process(final KV<String, GenericRecord> kv, Batch batch, final WindowedContext context, final PaneInfo paneInfo, final BoundedWindow window, final Instant timestamp) throws Exception;

    protected boolean enableRawConsumption() {
        return false;
    }

    protected int consumeRaw(final List<KV<String, GenericRecord>> iterable, Batch batch, final WindowedContext context, final PaneInfo paneInfo, final BoundedWindow window, final Instant timestamp) throws Exception {
        return 0;
    }

    protected void consumeBuffer(final KV<String, GenericRecord> kv, final WindowedContext context, final PaneInfo paneInfo, final BoundedWindow window, final Instant timestamp) throws Exception {
    }

    protected abstract void closeBatch(final Batch batch, final WindowedContext context, final PipelineConstants.BATCH_WRITER_CLOSE_TYPE type) throws Exception;

    protected abstract Batch initBatch() throws Exception;

}
