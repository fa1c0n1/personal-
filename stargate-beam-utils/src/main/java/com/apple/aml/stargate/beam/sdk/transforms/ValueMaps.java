package com.apple.aml.stargate.beam.sdk.transforms;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.HashMap;

public final class ValueMaps extends PTransform<PCollection<? extends KV<?, GenericRecord>>, PCollection<HashMap>> {
    private ValueMaps() {
    }

    public static ValueMaps create() {
        return new ValueMaps();
    }

    @Override
    public PCollection<HashMap> expand(final PCollection<? extends KV<?, GenericRecord>> in) {
        return in.apply(
                "ValueMaps",
                MapElements.via(
                        new SimpleFunction<KV<?, GenericRecord>, HashMap>() {
                            @Override
                            public HashMap apply(final KV<?, GenericRecord> kv) {
                                GenericRecord value = kv.getValue();
                                GenericData.Record record = new GenericData.Record(value.getSchema());
                                return null;
                            }
                        }));
    }
}

