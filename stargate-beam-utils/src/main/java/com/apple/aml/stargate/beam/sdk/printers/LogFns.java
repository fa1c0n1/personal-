package com.apple.aml.stargate.beam.sdk.printers;

import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.pipeline.sdk.printers.BaseLogFns;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.event.Level;

import static com.apple.aml.stargate.common.nodes.StargateNode.nodeName;

public final class LogFns extends BaseLogFns {

    public static SCollection<KV<String, GenericRecord>> applyLogFns(final String nodeName, final String nodeType, final String loglevel, final SCollection<KV<String, GenericRecord>> collection) {
        if (loglevel == null) {
            return collection;
        }
        Level level = Level.valueOf(loglevel.toUpperCase());
        String name = nodeName(nodeName, String.format("log-as-%s", nodeName, level.name().toLowerCase()));
        if (level == Level.ERROR) {
            return collection.apply(name, new LogAsError(nodeName, nodeType));
        }
        if (level == Level.WARN) {
            return collection.apply(name, new LogAsWarn(nodeName, nodeType));
        }
        if (level == Level.INFO) {
            return collection.apply(name, new LogAsInfo(nodeName, nodeType));
        }
        if (level == Level.DEBUG) {
            return collection.apply(name, new LogAsDebug(nodeName, nodeType));
        }
        return collection;
    }

    private static class LogAsError extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> {
        private static final long serialVersionUID = 1L;
        private final String nodeName;
        private final String nodeType;

        public LogAsError(final String nodeName, final String nodeType) {
            this.nodeName = nodeName;
            this.nodeType = nodeType;
        }

        @SuppressWarnings("unchecked")
        @ProcessElement
        public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) {
            logAsError(nodeName, nodeType, kv);
            ctx.output(kv);
        }
    }

    private static class LogAsWarn extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> {
        private static final long serialVersionUID = 1L;
        private final String nodeName;
        private final String nodeType;

        public LogAsWarn(final String nodeName, final String nodeType) {
            this.nodeName = nodeName;
            this.nodeType = nodeType;
        }

        @SuppressWarnings("unchecked")
        @ProcessElement
        public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) {
            logAsWarn(nodeName, nodeType, kv);
            ctx.output(kv);
        }
    }

    private static class LogAsInfo extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> {
        private static final long serialVersionUID = 1L;
        private final String nodeName;
        private final String nodeType;

        public LogAsInfo(final String nodeName, final String nodeType) {
            this.nodeName = nodeName;
            this.nodeType = nodeType;
        }

        @SuppressWarnings("unchecked")
        @ProcessElement
        public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) {
            logAsInfo(nodeName, nodeType, kv);
            ctx.output(kv);
        }
    }

    private static class LogAsDebug extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> {
        private static final long serialVersionUID = 1L;
        private final String nodeName;
        private final String nodeType;

        public LogAsDebug(final String nodeName, final String nodeType) {
            this.nodeName = nodeName;
            this.nodeType = nodeType;
        }

        @SuppressWarnings("unchecked")
        @ProcessElement
        public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) {
            logAsDebug(nodeName, nodeType, kv);
            ctx.output(kv);
        }
    }
}
