package com.apple.aml.stargate.beam.sdk.io.file;

import com.apple.aml.stargate.beam.sdk.utils.FileWriterFns.WriterKey;
import com.apple.aml.stargate.beam.sdk.utils.FileWriterFns.WriterValue;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;

public class NoOpFileWriter<Input> extends AbstractWriter<Input, ConcurrentHashMap<WriterKey, WriterValue>> implements Serializable {
    private static final long serialVersionUID = 1L;
    protected final String nodeName;
    protected final String nodeType;

    public NoOpFileWriter(final String nodeName, final String nodeType) {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
    }

    @Override
    protected void setup() throws Exception {
    }

    @Override
    protected KV<String, GenericRecord> process(final KV<String, GenericRecord> kv, final ConcurrentHashMap<WriterKey, WriterValue> ignored, final WindowedContext context, final PaneInfo paneInfo, final BoundedWindow window, final Instant timestamp) throws Exception {
        final GenericRecord record = kv.getValue();
        final Schema schema = record.getSchema();
        final String schemaId = schema.getFullName();
        counter(nodeName, nodeType, schemaId, "elements_noop").inc();
        return null;
    }

    @Override
    public void closeBatch(final ConcurrentHashMap<WriterKey, WriterValue> batch, final WindowedContext context, final PipelineConstants.BATCH_WRITER_CLOSE_TYPE batchType) throws Exception {

    }

    @Override
    protected ConcurrentHashMap<WriterKey, WriterValue> initBatch() throws Exception {
        return null;
    }
}
