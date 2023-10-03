package com.apple.aml.stargate.beam.sdk.utils;

import com.apple.aml.stargate.beam.sdk.io.file.AbstractWriter;
import com.apple.aml.stargate.beam.sdk.io.file.BatchWriter;
import com.apple.aml.stargate.beam.sdk.io.file.StatefulBatchWriter;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.options.WindowOptions;
import com.google.common.base.CaseFormat;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;
import org.joda.time.Duration;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.pipelineId;
import static com.apple.aml.stargate.common.nodes.StargateNode.nodeName;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.NetworkUtils.hostName;
import static com.apple.jvm.commons.util.Strings.isBlank;

public final class WindowFns {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    private WindowFns() {

    }

    public static <O> SCollection<O> applyWindow(final SCollection<O> inputCollection, final WindowOptions options, final String nodeName) {
        SCollection<O> window;
        if (options.getWindowDuration() == null || options.getWindowDuration().isNegative() || options.getWindowDuration().isZero()) {
            LOGGER.debug("Window size not set. Will skip windowing");
            return inputCollection;
        }
        if (options.isTriggering()) {
            LOGGER.debug("Triggering enabled. Will create windows with triggering support", Map.of(NODE_NAME, nodeName, "windowSize", options.getWindowDuration(), "windowDelay", options.getWindowLateness()));
            window = inputCollection.apply(nodeName(nodeName, "window"), triggeringFixedWindow(options, nodeName));
        } else {
            LOGGER.debug("Triggering not enabled. Will create windows", Map.of(NODE_NAME, nodeName, "windowSize", options.getWindowDuration()));
            window = inputCollection.apply(nodeName(nodeName, "window"), Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowDuration().getSeconds()))));
        }
        return window;
    }

    public static <O> Window<O> triggeringFixedWindow(final WindowOptions options, final String nodeName) {
        WindowingStrategy.AccumulationMode accumulationMode = options.getWindowAccumulationMode() == null || options.getWindowAccumulationMode().trim().isBlank() ? null : WindowingStrategy.AccumulationMode.valueOf(options.getWindowAccumulationMode().contains("_") ? options.getWindowAccumulationMode().toUpperCase() : CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, options.getWindowAccumulationMode()).toUpperCase());
        if (accumulationMode == WindowingStrategy.AccumulationMode.ACCUMULATING_FIRED_PANES) {
            LOGGER.debug("Setting accumulationMode", Map.of(NODE_NAME, nodeName, "accumulationMode", "accumulatingFiredPanes"));
            return Window.<O>into(FixedWindows.of(Duration.standardSeconds(options.getWindowDuration().getSeconds()))).withAllowedLateness(Duration.standardSeconds(options.getWindowLateness().getSeconds()), closingBehavior(options)).accumulatingFiredPanes().triggering(getWindowTrigger(options, nodeName));
        }
        LOGGER.debug("Setting accumulationMode", Map.of(NODE_NAME, nodeName, "accumulationMode", "discardingFiredPanes"));
        return Window.<O>into(FixedWindows.of(Duration.standardSeconds(options.getWindowDuration().getSeconds()))).withAllowedLateness(Duration.standardSeconds(options.getWindowLateness().getSeconds()), closingBehavior(options)).discardingFiredPanes().triggering(getWindowTrigger(options, nodeName));
    }

    public static Window.ClosingBehavior closingBehavior(final WindowOptions options) {
        if (isBlank(options.getWindowClosingBehavior())) {
            return Window.ClosingBehavior.FIRE_IF_NON_EMPTY;
        }
        Window.ClosingBehavior behavior = Window.ClosingBehavior.valueOf(options.getWindowClosingBehavior().contains("_") ? options.getWindowClosingBehavior().toUpperCase() : CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, options.getWindowClosingBehavior()).toUpperCase());
        return behavior == null ? Window.ClosingBehavior.FIRE_IF_NON_EMPTY : behavior;
    }

    public static Trigger getWindowTrigger(final WindowOptions options, final String nodeName) {
        if ("mode1".equalsIgnoreCase(options.getTriggerMode())) {
            LOGGER.debug("Setting triggerMode", Map.of(NODE_NAME, nodeName, "triggerMode", "mode1"));
            return Repeatedly.forever(AfterWatermark.pastEndOfWindow().withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(options.getWindowLateness().getSeconds()))).withLateFirings(AfterPane.elementCountAtLeast(1)));
        }
        if ("mode2".equalsIgnoreCase(options.getTriggerMode())) {
            LOGGER.debug("Setting triggerMode", Map.of(NODE_NAME, nodeName, "triggerMode", "mode2"));
            return Repeatedly.forever(AfterWatermark.pastEndOfWindow().withLateFirings(AfterPane.elementCountAtLeast(1)));
        }
        if ("mode3".equalsIgnoreCase(options.getTriggerMode())) {
            LOGGER.debug("Setting triggerMode", Map.of(NODE_NAME, nodeName, "triggerMode", "mode3"));
            return Repeatedly.forever(AfterWatermark.pastEndOfWindow().withLateFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(options.getWindowDuration().getSeconds() + options.getWindowLateness().getSeconds()))));
        }
        //AfterFirst.of(AfterPane.elementCountAtLeast(options.getWindowMinRecordCount()), AfterWatermark.pastEndOfWindow());
        LOGGER.debug("Setting triggerMode", Map.of(NODE_NAME, nodeName, "triggerMode", "default"));
        return AfterWatermark.pastEndOfWindow().withLateFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(options.getWindowDuration().getSeconds() + options.getWindowLateness().getSeconds())));
    }

    public static <A, B> DoFn<KV<A, KV<String, GenericRecord>>, KV<String, GenericRecord>> batchWriter(final String nodeName, final String nodeType, final PipelineConstants.ENVIRONMENT environment, final WindowOptions options, final AbstractWriter<KV<A, KV<String, GenericRecord>>, B> fileWriter) {
        if (options.isStatefulBatch()) {
            return new StatefulBatchWriter<>(nodeName, nodeType, environment, options, fileWriter);
        }
        return new BatchWriter<>(nodeName, nodeType, environment, options, fileWriter);
    }

    public static DoFn<KV<String, GenericRecord>, KV<String, KV<String, GenericRecord>>> batchPartitionKey(final int partitions, final String partitionStrategy) {
        if (partitions <= 0) return batchPartitionKey(partitionStrategy);
        if (isBlank(partitionStrategy)) return new VMRoundRobinBatchPartitionKey(partitions);
        PipelineConstants.PARTITION_STRATEGY strategy = PipelineConstants.PARTITION_STRATEGY.valueOf(partitionStrategy.trim().toLowerCase());
        if (strategy == null) return new VMRoundRobinBatchPartitionKey(partitions);
        switch (strategy) {
            case hash:
                return new BatchPartitionKey(partitions);
            case random:
                return new RandomBatchPartitionKey(partitions);
            case roundrobin:
                return new RoundRobinBatchPartitionKey(partitions);
            case hostrandom:
                return new HostRandomBatchPartitionKey(partitions);
            case hostroundrobin:
                return new HostRoundRobinBatchPartitionKey(partitions);
            default:
                return new VMRoundRobinBatchPartitionKey(partitions);
        }
    }

    public static DoFn<KV<String, GenericRecord>, KV<String, KV<String, GenericRecord>>> batchPartitionKey(final String partitionStrategy) {
        if (isBlank(partitionStrategy)) return new HostFixedPartitionKey();
        PipelineConstants.PARTITION_STRATEGY strategy = PipelineConstants.PARTITION_STRATEGY.valueOf(partitionStrategy.trim().toLowerCase());
        if (strategy == null) return new HostFixedPartitionKey();
        switch (strategy) {
            case nullkey:
                return new NullPartitionKey();
            case pipeline:
                return new PipelinePartitionKey();
            default:
                return new HostFixedPartitionKey();
        }
    }

    public static class GatherBundlesPerWindowFn<T> extends DoFn<T, List<T>> {
        private transient Multimap<BoundedWindow, T> bundles = null;

        @StartBundle
        public void startBundle() {
            bundles = ArrayListMultimap.create();
        }

        @ProcessElement
        public void process(ProcessContext c, BoundedWindow w) {
            bundles.put(w, c.element());
        }

        @FinishBundle
        public void finishBundle(FinishBundleContext c) throws Exception {
            for (BoundedWindow w : bundles.keySet()) {
                c.output(Lists.newArrayList(bundles.get(w)), w.maxTimestamp(), w);
            }
        }
    }

    public static class GatherResults<ResultT> extends PTransform<PCollection<ResultT>, PCollection<List<ResultT>>> {

        @SuppressWarnings("deprecation")
        @Override
        public PCollection<List<ResultT>> expand(PCollection<ResultT> input) {
            return input.apply("Add void key", WithKeys.of((Void) null)).apply("Reshuffle", Reshuffle.of()).apply("Drop key", Values.create()).apply("Gather bundles", ParDo.of(new GatherBundlesPerWindowFn<>())).apply(Reshuffle.viaRandomKey());
        }
    }

    public static class BatchPartitionKey extends DoFn<KV<String, GenericRecord>, KV<String, KV<String, GenericRecord>>> {
        private final int partitions;

        public BatchPartitionKey(final int partitions) {
            this.partitions = partitions;
        }

        @ProcessElement
        public void processElement(final @Element KV<String, GenericRecord> kv, final ProcessContext ctx) {
            ctx.output(KV.of(String.valueOf(Math.abs(kv.hashCode() % partitions)), kv));
        }
    }

    public static class RandomBatchPartitionKey extends DoFn<KV<String, GenericRecord>, KV<String, KV<String, GenericRecord>>> {
        private final Random rand;
        private final int partitions;

        public RandomBatchPartitionKey(final int partitions) {
            this.partitions = partitions;
            this.rand = new Random(System.nanoTime());
        }

        @ProcessElement
        public void processElement(final @Element KV<String, GenericRecord> kv, final ProcessContext ctx) {
            ctx.output(KV.of(String.valueOf(rand.nextInt(partitions)), kv));
        }
    }

    public static class HostRandomBatchPartitionKey extends DoFn<KV<String, GenericRecord>, KV<String, KV<String, GenericRecord>>> {
        private final String hostName;
        private final Random rand;
        private final int partitions;

        public HostRandomBatchPartitionKey(final int partitions) {
            this.hostName = hostName();
            this.partitions = partitions;
            this.rand = new Random(System.nanoTime());
        }

        @ProcessElement
        public void processElement(final @Element KV<String, GenericRecord> kv, final ProcessContext ctx) {
            ctx.output(KV.of(String.format("%s-%d", hostName, rand.nextInt(partitions)), kv));
        }
    }

    public static class RoundRobinBatchPartitionKey extends DoFn<KV<String, GenericRecord>, KV<String, KV<String, GenericRecord>>> {
        private final int partitions;
        private AtomicInteger atomic; // TODO : fyi, this is not truely RoundRobin coz each slot will have its own copy of this

        public RoundRobinBatchPartitionKey(final int partitions) {
            this.partitions = partitions;
            this.atomic = new AtomicInteger();
        }

        @ProcessElement
        public void processElement(final @Element KV<String, GenericRecord> kv, final ProcessContext ctx) {
            ctx.output(KV.of(String.valueOf(atomic.getAndIncrement() % partitions), kv));
        }
    }

    public static class VMRoundRobinBatchPartitionKey extends DoFn<KV<String, GenericRecord>, KV<String, KV<String, GenericRecord>>> {
        private static final AtomicInteger ATOMIC = new AtomicInteger(); // TODO : fyi, this is not truely RoundRobin coz each vm has its own copy of this
        private final int partitions;

        public VMRoundRobinBatchPartitionKey(final int partitions) {
            this.partitions = partitions;
        }

        @ProcessElement
        public void processElement(final @Element KV<String, GenericRecord> kv, final ProcessContext ctx) {
            ctx.output(KV.of(String.valueOf(ATOMIC.getAndIncrement() % partitions), kv));
        }
    }

    public static class HostRoundRobinBatchPartitionKey extends DoFn<KV<String, GenericRecord>, KV<String, KV<String, GenericRecord>>> {
        private static final AtomicInteger ATOMIC = new AtomicInteger(); // TODO : fyi, this is not truely RoundRobin coz each vm has its own copy of this
        private final String hostName;
        private final int partitions;

        public HostRoundRobinBatchPartitionKey(final int partitions) {
            this.hostName = hostName();
            this.partitions = partitions;
        }

        @ProcessElement
        public void processElement(final @Element KV<String, GenericRecord> kv, final ProcessContext ctx) {
            ctx.output(KV.of(String.format("%s-%d", hostName, ATOMIC.getAndIncrement() % partitions), kv));
        }
    }

    public static class HostFixedPartitionKey extends DoFn<KV<String, GenericRecord>, KV<String, KV<String, GenericRecord>>> {
        private final String hostName;

        public HostFixedPartitionKey() {
            this.hostName = hostName();
        }

        @ProcessElement
        public void processElement(final @Element KV<String, GenericRecord> kv, final ProcessContext ctx) {
            ctx.output(KV.of(hostName, kv));
        }
    }

    public static class PipelinePartitionKey extends DoFn<KV<String, GenericRecord>, KV<String, KV<String, GenericRecord>>> {
        private final String pipelineId;

        public PipelinePartitionKey() {
            this.pipelineId = pipelineId();
        }

        @ProcessElement
        public void processElement(final @Element KV<String, GenericRecord> kv, final ProcessContext ctx) {
            ctx.output(KV.of(pipelineId, kv));
        }
    }

    public static class NullPartitionKey extends DoFn<KV<String, GenericRecord>, KV<String, KV<String, GenericRecord>>> {

        @ProcessElement
        public void processElement(final @Element KV<String, GenericRecord> kv, final ProcessContext ctx) {
            ctx.output(KV.of(null, kv));
        }
    }
}
