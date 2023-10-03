package com.apple.aml.stargate.beam.sdk.triggers;

import com.apple.aml.stargate.common.options.SchedulerOptions;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class Scheduler {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    @DefaultCoder(AvroCoder.class)
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CheckMark implements UnboundedSource.CheckpointMark {
        private long epoc;

        @Override
        public void finalizeCheckpoint() throws IOException {

        }
    }

    public static class Source extends UnboundedSource<KV<String, GenericRecord>, CheckMark> implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String nodeName;
        private final String nodeType;
        private final SchedulerOptions options;

        public Source(final String nodeName, final String nodeType, final SchedulerOptions options) {
            this.nodeName = nodeName;
            this.nodeType = nodeType;
            this.options = options;
        }

        @Override
        public List<? extends UnboundedSource<KV<String, GenericRecord>, CheckMark>> split(final int desiredNumSplits, final PipelineOptions options) throws Exception {
            return Collections.singletonList(this);
        }

        @Override
        public UnboundedReader<KV<String, GenericRecord>> createReader(final PipelineOptions pipelineOptions, final CheckMark checkpointMark) throws IOException {
            return new Scheduler.Reader(this, nodeName, nodeType, this.options);
        }

        @Override
        public Coder<CheckMark> getCheckpointMarkCoder() {
            return AvroCoder.of(CheckMark.class);
        }
    }

    public static class Reader extends UnboundedSource.UnboundedReader<KV<String, GenericRecord>> {
        private final Source source;
        private final String nodeName;
        private final String nodeType;
        private final SchedulerOptions options;

        public Reader(final Source source, final String nodeName, final String nodeType, final SchedulerOptions options) {
            this.source = source;
            this.nodeName = nodeName;
            this.nodeType = nodeType;
            this.options = options;
        }

        @Override
        public boolean start() throws IOException {
            return false;
        }

        @Override
        public boolean advance() throws IOException {
            return false;
        }

        @Override
        public Instant getWatermark() {
            return null;
        }

        @Override
        public UnboundedSource.CheckpointMark getCheckpointMark() {
            return null;
        }

        @Override
        public UnboundedSource<KV<String, GenericRecord>, ?> getCurrentSource() {
            return source;
        }

        @Override
        public KV<String, GenericRecord> getCurrent() throws NoSuchElementException {
            return null;
        }

        @Override
        public Instant getCurrentTimestamp() throws NoSuchElementException {
            return null;
        }

        @Override
        public void close() throws IOException {

        }
    }
}
