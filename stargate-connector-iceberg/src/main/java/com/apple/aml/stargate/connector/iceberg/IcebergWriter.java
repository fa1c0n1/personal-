package com.apple.aml.stargate.connector.iceberg;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.options.IcebergOptions;
import com.apple.aml.stargate.common.options.SchemaLevelOptions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.List;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.BUCKET_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.FILE_PATH;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.PipelineConstants.BATCH_WRITER_CLOSE_TYPE.finishBundle;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;

public class IcebergWriter<Input> extends IcebergBaseWriter<Input> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    public IcebergWriter(final String nodeName, final String nodeType, final PipelineConstants.ENVIRONMENT environment, final IcebergOptions options) {
        super(nodeName, nodeType, environment, options, true);
    }

    @Setup
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    public void emitMetrics(final String schemaId, final String sourceSchemaId, final String filePath, final SchemaLevelOptions options) {
        incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, BUCKET_NAME + "_" + FILE_PATH, String.format("%s_%s", bucket, filePath), SOURCE_SCHEMA_ID, sourceSchemaId);
    }

    @Teardown
    public void teardown() throws Exception {
        super.tearDown(null);
    }

    @StartBundle
    public void startBundle(final StartBundleContext context) throws Exception {
        writers = initBatch();
    }

    @FinishBundle
    public void finishBundle(final FinishBundleContext context) throws Exception {
        closeBatch(writers, null, finishBundle);
    }

    @SuppressWarnings("unchecked")
    @ProcessElement
    public void processElement(final ProcessContext context, final BoundedWindow window, final PaneInfo paneInfo) throws Exception {
        Input input = context.element();
        if (input instanceof List) {
            for (KV<String, GenericRecord> kv : (List<KV<String, GenericRecord>>) input) {
                consume(kv, writers, context, window, paneInfo);
            }
            return;
        }
        consume((KV<String, GenericRecord>) input, writers, context, window, paneInfo);
    }
}
