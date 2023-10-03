package org.apache.iceberg.beam.writers;

import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.data.parquet.BaseParquetWriter;
import org.apache.iceberg.parquet.ParquetValueWriter;
import org.apache.iceberg.parquet.ParquetValueWriters;
import org.apache.parquet.schema.MessageType;

import java.util.List;

public class GenericAvroParquetWriter extends BaseParquetWriter<GenericRecord> {
    private static final GenericAvroParquetWriter INSTANCE = new GenericAvroParquetWriter();

    private GenericAvroParquetWriter() {
    }

    public static ParquetValueWriter<GenericRecord> buildWriter(final MessageType type) {
        return INSTANCE.createWriter(type);
    }

    @Override
    protected ParquetValueWriters.StructWriter<GenericRecord> createStructWriter(final List<ParquetValueWriter<?>> writers) {
        return new RecordWriter(writers);
    }

    private static class RecordWriter extends ParquetValueWriters.StructWriter<GenericRecord> {
        private RecordWriter(final List<ParquetValueWriter<?>> writers) {
            super(writers);
        }

        @Override
        protected Object get(final GenericRecord struct, final int index) {
            return struct.get(index);
        }
    }
}
