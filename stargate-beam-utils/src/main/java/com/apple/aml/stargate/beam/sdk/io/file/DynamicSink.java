package com.apple.aml.stargate.beam.sdk.io.file;

import com.apple.aml.stargate.common.constants.PipelineConstants.DATA_FORMAT;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.values.KV;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.constants.CommonConstants.DEFAULT_DELIMITER;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.isDynamicKerberosLoginEnabled;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.performKerberosLogin;

public class DynamicSink implements FileIO.Sink<KV<String, GenericRecord>> {
    private final String nodeName;
    private final String nodeType;
    private final FileIO.Sink backingSink;
    private final boolean kerberosLoginEnabled;

    public DynamicSink(final String nodeName, final String nodeType, final DATA_FORMAT fileType, final Schema schema, final CompressionCodecName codecName) {
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        switch (fileType) {
            case csv:
                backingSink = new GenericRecordTextSink(schema.getFields().stream().map(f -> f.name()).collect(Collectors.joining(DEFAULT_DELIMITER)), record -> schema.getFields().stream().map(f -> String.valueOf(record.get(f.name()))).collect(Collectors.joining(DEFAULT_DELIMITER)));
                break;
            case avro:
                backingSink = AvroIO.sink(schema);
                break;
            case parquet:
                backingSink = ParquetIO.sink(schema).withCompressionCodec(codecName);
                break;
            case json:
                backingSink = new GenericRecordTextSink(Object::toString);
                break;
            default:
                backingSink = TextIO.sink();
                break;
        }
        this.kerberosLoginEnabled = isDynamicKerberosLoginEnabled();
    }

    @SneakyThrows
    @Override
    @SuppressWarnings("unchecked")
    public void open(final WritableByteChannel channel) throws IOException {
        if (kerberosLoginEnabled) {
            performKerberosLogin();
        }
        long startTime = System.nanoTime();
        backingSink.open(channel);
        histogramDuration(nodeName, nodeType, UNKNOWN, "open").observe((System.nanoTime() - startTime) / 1000000.0);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    @Override
    public void write(final KV<String, GenericRecord> input) throws IOException {
        long startTime = System.nanoTime();
        GenericRecord record = input.getValue();
        backingSink.write(record);
        histogramDuration(nodeName, nodeType, record.getSchema().getFullName(), "write").observe((System.nanoTime() - startTime) / 1000000.0);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    @Override
    public void flush() throws IOException {
        long startTime = System.nanoTime();
        backingSink.flush();
        histogramDuration(nodeName, nodeType, UNKNOWN, "flush").observe((System.nanoTime() - startTime) / 1000000.0);
    }
}
