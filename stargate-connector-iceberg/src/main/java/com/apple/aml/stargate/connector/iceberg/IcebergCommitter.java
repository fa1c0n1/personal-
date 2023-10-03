package com.apple.aml.stargate.connector.iceberg;

import com.apple.aml.stargate.beam.sdk.io.file.AbstractWriter;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.constants.PipelineConstants.BATCH_WRITER_CLOSE_TYPE;
import com.apple.aml.stargate.common.constants.PipelineConstants.CATALOG_TYPE;
import com.apple.aml.stargate.common.options.IcebergOptions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.iceberg.DataFile;
import org.joda.time.Instant;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.PipelineConstants.BATCH_WRITER_CLOSE_TYPE.tearDown;
import static com.apple.aml.stargate.common.utils.AvroUtils.getFieldValue;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.connector.iceberg.IcebergBaseWriter.commitDataFiles;
import static com.apple.aml.stargate.connector.iceberg.IcebergIO.icebergCatalog;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.hiveConfiguration;

public class IcebergCommitter<Input> extends AbstractWriter<Input, ConcurrentHashMap<String, List<DataFile>>> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    protected final String nodeName;
    protected final String nodeType;
    private final int commitRetries;
    private final long commitRetryWait;
    private final CATALOG_TYPE catalogType;
    private final Map<String, Object> catalogProperties;
    private boolean useDirectBuffer;
    private transient ConcurrentHashMap<String, List<DataFile>> writers;


    public IcebergCommitter(final String nodeName, final String nodeType, final PipelineConstants.ENVIRONMENT environment, final IcebergOptions options) {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.commitRetries = options.getCommitRetries();
        this.commitRetryWait = options.getCommitRetryWait();
        this.catalogType = options.catalogType();
        this.catalogProperties = options.getCatalogProperties();
        this.useDirectBuffer = options.isUseDirectBufferForCommitBatch();
    }

    public boolean enableDirectBuffer() {
        return useDirectBuffer;
    }

    public void setup() throws Exception {
        writers = initBatch();
    }

    public void tearDown(final WindowedContext nullableContext) throws Exception {
        if (writers.isEmpty()) return;
        closeBatch(writers, nullableContext, tearDown);
    }

    @Override
    public KV<String, GenericRecord> process(final KV<String, GenericRecord> kv, final ConcurrentHashMap<String, List<DataFile>> batch, final WindowedContext context, final PaneInfo paneInfo, final BoundedWindow window, final Instant timestamp) throws Exception {
        String schemaId = kv.getValue().getSchema().getFullName();
        counter(nodeName, nodeType, schemaId, ELEMENTS_IN).inc();
        long startTime = System.nanoTime();
        try {
            batch.computeIfAbsent(kv.getKey(), key -> new ArrayList<>()).add(SerializationUtils.deserialize(Base64.getDecoder().decode(((String) getFieldValue(kv.getValue(), "dataFile")).getBytes(StandardCharsets.UTF_8))));
        } finally {
            histogramDuration(nodeName, nodeType, schemaId, "process").observe((System.nanoTime() - startTime) / 1000000.0);
        }
        return null;
    }

    public void consumeBuffer(final KV<String, GenericRecord> kv, final WindowedContext context, final PaneInfo paneInfo, final BoundedWindow window, final Instant timestamp) throws Exception {
        process(kv, writers, context, paneInfo, window, Instant.now());
    }

    @Override
    public void closeBatch(final ConcurrentHashMap<String, List<DataFile>> batch, final WindowedContext context, final BATCH_WRITER_CLOSE_TYPE type) throws Exception {
        commitDataFiles(batch, icebergCatalog(catalogType, catalogProperties, hiveConfiguration()), commitRetries, commitRetryWait, nodeName, nodeType);
    }

    @Override
    protected ConcurrentHashMap<String, List<DataFile>> initBatch() throws Exception {
        return useDirectBuffer ? writers == null ? new ConcurrentHashMap<>() : writers : new ConcurrentHashMap<>();
    }
}
