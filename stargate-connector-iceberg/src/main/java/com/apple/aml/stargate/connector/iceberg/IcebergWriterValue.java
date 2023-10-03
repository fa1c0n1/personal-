package com.apple.aml.stargate.connector.iceberg;

import com.apple.aml.stargate.beam.sdk.utils.FileWriterFns.WriterKey;
import com.apple.aml.stargate.beam.sdk.utils.FileWriterFns.WriterValue;
import lombok.Getter;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.io.TaskWriter;

import java.io.IOException;

public class IcebergWriterValue extends WriterValue {
    @Getter
    private final TaskWriter taskWriter;

    public IcebergWriterValue(final WriterKey writerKey, final TaskWriter taskWriter) {
        super(writerKey, writerKey.getFilePath());
        this.taskWriter = taskWriter;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writeRecord(final GenericRecord record) throws Exception {
        taskWriter.write(record);
    }

    @Override
    public void close() throws IOException {
        // do nothing; do not close here
    }

    @Override
    public void abort() throws IOException {
        this.taskWriter.abort();
    }
}
