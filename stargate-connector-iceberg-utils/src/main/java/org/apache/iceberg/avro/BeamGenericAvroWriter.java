package org.apache.iceberg.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;

public class BeamGenericAvroWriter<T> extends GenericAvroWriter<T> implements DatumWriter<T> {
    public BeamGenericAvroWriter(final Schema schema) {
        super(schema);
    }
}
