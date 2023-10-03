package com.apple.aml.stargate.beam.sdk.transforms;

import org.apache.beam.sdk.transforms.DoFn;

import java.util.List;

public final class CollectionFns {
    public static class IndividualRecords<I> extends DoFn<List<I>, I> {
        private static final long serialVersionUID = 1L;

        @ProcessElement
        public void processElement(@Element final List<I> collection, final ProcessContext ctx) {
            collection.forEach(ctx::output);
        }
    }
}
