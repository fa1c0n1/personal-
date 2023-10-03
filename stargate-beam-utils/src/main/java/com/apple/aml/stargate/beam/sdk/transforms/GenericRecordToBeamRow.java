package com.apple.aml.stargate.beam.sdk.transforms;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

public final class GenericRecordToBeamRow extends DoFn<GenericRecord, Row> {
    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void processElement(@Element final GenericRecord record, final ProcessContext ctx) {
        Row row = AvroUtils.toBeamRowStrict(record, AvroUtils.toBeamSchema(record.getSchema()));
        ctx.output(row);
    }
}
