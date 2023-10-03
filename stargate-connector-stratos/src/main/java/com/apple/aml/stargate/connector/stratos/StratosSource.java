package com.apple.aml.stargate.connector.stratos;

import com.apple.aml.stargate.beam.sdk.options.StargateOptions;
import com.apple.aml.stargate.common.options.StratosOptions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.KV;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class StratosSource extends UnboundedSource<KV<String, GenericRecord>, StratosCheckMark> implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String nodeName;
    private final String nodeType;
    private final StratosOptions options;
    private final Coder<KV<String, GenericRecord>> coder;

    public StratosSource(final String nodeName, final String nodeType, final StratosOptions options, final Coder<KV<String, GenericRecord>> coder) {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.options = options;
        this.coder = coder;
    }

    @Override
    public List<? extends UnboundedSource<KV<String, GenericRecord>, StratosCheckMark>> split(final int desiredNumSplits, final PipelineOptions options) throws Exception {
        return Collections.singletonList(this);
    }

    @Override
    public UnboundedReader<KV<String, GenericRecord>> createReader(final PipelineOptions pipelineOptions, final StratosCheckMark checkpointMark) throws IOException {
        return new StratosReader(this, nodeName, nodeType, this.options, pipelineOptions.as(StargateOptions.class).getSharedDirectoryPath());
    }

    @Override
    public Coder<StratosCheckMark> getCheckpointMarkCoder() {
        return AvroCoder.of(StratosCheckMark.class);
    }

    @Override
    public Coder<KV<String, GenericRecord>> getOutputCoder() {
        return coder;
    }
}
