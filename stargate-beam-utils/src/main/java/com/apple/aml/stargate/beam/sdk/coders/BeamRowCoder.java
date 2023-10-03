package com.apple.aml.stargate.beam.sdk.coders;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.getLocalSchema;
import static org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils.toBeamSchema;


public class BeamRowCoder extends AtomicCoder<Row> {
    private static final long serialVersionUID = 1L;
    private static final ConcurrentHashMap<String, RowCoder> CODERS = new ConcurrentHashMap<>();
    private static final BeamRowCoder INSTANCE = new BeamRowCoder();


    private BeamRowCoder() {
    }

    public static BeamRowCoder of() {
        return INSTANCE;
    }

    @Override
    public void encode(final Row row, final OutputStream outStream) throws IOException {
        Schema schema = row.getSchema();
        String schemaUUID = schema.getUUID().toString();
        RowCoder coder = CODERS.computeIfAbsent(schemaUUID, id -> RowCoder.of(schema));
        StringUtf8Coder.of().encode(schemaUUID, outStream);
        coder.encode(row, outStream);
    }

    @Override
    public Row decode(final InputStream inStream) throws IOException {
        String schemaUUID = StringUtf8Coder.of().decode(inStream);
        RowCoder coder = CODERS.computeIfAbsent(schemaUUID, id -> {
            Schema beamSchema = toBeamSchema(getLocalSchema(schemaUUID));
            beamSchema.setUUID(UUID.fromString(schemaUUID));
            return RowCoder.of(beamSchema);
        });
        return coder.decode(inStream);
    }
}
