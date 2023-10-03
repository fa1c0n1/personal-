package com.apple.aml.stargate.beam.sdk.formatters;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.SimpleFunction;

public final class GenericRecordToText extends SimpleFunction<GenericRecord, String> {
    private static final long serialVersionUID = 1L;

    @Override
    public String apply(final GenericRecord input) {
        return input.toString();
    }
}
