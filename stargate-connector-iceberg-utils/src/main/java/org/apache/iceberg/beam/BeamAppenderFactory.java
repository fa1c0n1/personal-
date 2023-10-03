package org.apache.iceberg.beam;

import com.apple.aml.stargate.common.exceptions.GenericException;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.BeamGenericAvroWriter;
import org.apache.iceberg.beam.writers.GenericAvroParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;

import java.io.IOException;
import java.util.Map;

public class BeamAppenderFactory<T extends GenericRecord> implements FileAppenderFactory<T> {
    private final Schema schema;
    private final Map<String, String> properties;
    private final PartitionSpec spec;
    private final MetricsConfig metricsConfig;

    public BeamAppenderFactory(final Table table) {
        this.schema = table.schema();
        this.properties = table.properties();
        this.spec = table.spec();
        this.metricsConfig = MetricsConfig.forTable(table);
    }

    @Override
    public FileAppender<T> newAppender(final OutputFile file, final FileFormat fileFormat) {
        try {
            switch (fileFormat) {
                case AVRO:
                    return Avro.write(file).createWriterFunc(BeamGenericAvroWriter::new).setAll(properties).schema(schema).overwrite().build();
                case PARQUET:
                    return Parquet.write(file).createWriterFunc(GenericAvroParquetWriter::buildWriter).setAll(properties).metricsConfig(metricsConfig).schema(schema).overwrite().build();
                default:
                    throw new UnsupportedOperationException("Cannot write unknown format: " + fileFormat);
            }
        } catch (IOException e) {
            throw new GenericException("Error creating new iceberg appender", Map.of("fileFormat", String.valueOf(fileFormat), "file", String.valueOf(file.location())), e).wrap();
        }
    }

    @Override
    public DataWriter<T> newDataWriter(final EncryptedOutputFile file, final FileFormat format, final StructLike partition) {
        return new DataWriter<>(newAppender(file.encryptingOutputFile(), format), format, file.encryptingOutputFile().location(), spec, partition, file.keyMetadata());
    }

    @Override
    public EqualityDeleteWriter<T> newEqDeleteWriter(final EncryptedOutputFile file, final FileFormat format, final StructLike partition) {
        return null;
    }

    @Override
    public PositionDeleteWriter<T> newPosDeleteWriter(final EncryptedOutputFile file, final FileFormat format, final StructLike partition) {
        return null;
    }
}