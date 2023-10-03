package com.apple.aml.stargate.beam.sdk.formatters;

import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;

public final class BeamRowToText extends SimpleFunction<Row, String> {
    private static final long serialVersionUID = 1L;

    @Override
    public String apply(final Row input) {
        return input.toString();
    }
}
