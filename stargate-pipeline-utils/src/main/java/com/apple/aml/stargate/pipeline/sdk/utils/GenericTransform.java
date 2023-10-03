package com.apple.aml.stargate.pipeline.sdk.utils;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;


public class   GenericTransform<I, O>{
    private final Function<I, List<O>> transformation;

    public GenericTransform(Function<I, List<O>> transformation) {
        this.transformation = transformation;
    }

    public void transform(I input, Consumer<List<O>> consumer) {
        List<O> result = transformation.apply(input);
        consumer.accept(result);
    }


    public static <I, O> DoFn<I, O> createBeamDoFn(Function<I, List<O>> transformation) {
        return new DoFn<I, O>() {
            GenericTransform<I, O> genericTransform = new GenericTransform<>(transformation);

            @ProcessElement
            public void processElement(ProcessContext c) {
                Instant currentTimestamp = Instant.now();
                genericTransform.transform(c.element(), results -> {
                    for (O result : results) {
                        c.outputWithTimestamp(result, currentTimestamp);
                    }
                });
            }
        };
    }

    public static <I, O> FlatMapFunction<I, O> createFlinkFlatMapFunction(Function<I, List<O>> transformation) {
        return new RichFlatMapFunction<I, O>()  {
            GenericTransform<I, O> genericTransform = null;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                genericTransform = new GenericTransform<>(transformation);
            }

            @Override
            public void flatMap(I value, Collector<O> out) throws Exception {
                genericTransform.transform(value, results -> results.forEach(out::collect));
            }
        };
    }
}