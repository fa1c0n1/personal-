package com.apple.aml.stargate.beam.sdk.transforms;

import com.apple.aml.stargate.common.utils.AvroUtils;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;
import java.util.UUID;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;

public final class UtilFns {
    public static EnsureNonNullableKey ensureNonNullableKey() {
        return new UtilFns.EnsureNonNullableKey();
    }

    public static <E> RateLimit<E> rateLimiter(final int maxTps) {
        return new RateLimit<E>(maxTps);
    }

    public static AddKey addKey(final String nodeName, final String nodeType, final String recordIdentifier) {
        return new AddKey(nodeName, nodeType, recordIdentifier);
    }

    public static String recordKey(final GenericRecord record, final String recordIdentifier) {
        Object recordKey = null;
        if (recordIdentifier != null) recordKey = AvroUtils.getFieldValue(record, recordIdentifier);
        if (recordKey == null) recordKey = UUID.randomUUID();
        String key = recordKey.toString();
        return key;
    }

    public static class EnsureNonNullableKey extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> implements Serializable {
        private static final long serialVersionUID = 1L;

        @ProcessElement
        public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
            ctx.output(KV.of(kv.getKey() == null ? UUID.randomUUID().toString() : kv.getKey(), kv.getValue()));
        }
    }

    public static class RateLimit<E> extends DoFn<E, E> implements Serializable {
        private static final long serialVersionUID = 1L;
        private final int maxTps;
        private transient RateLimiter rateLimiter;

        public RateLimit(final int maxTps) {
            this.maxTps = maxTps;
            this.rateLimiter = RateLimiter.create(maxTps);
        }

        @ProcessElement
        public void processElement(@Element final E element, final ProcessContext ctx) throws Exception {
            if (rateLimiter == null) rateLimiter = RateLimiter.create(maxTps);
            rateLimiter.acquire();
            ctx.output(element);
        }
    }

    public static class AddKey extends DoFn<GenericRecord, KV<String, GenericRecord>> implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String nodeName;
        private final String nodeType;
        private final String recordIdentifier;

        public AddKey(final String nodeName, final String nodeType, final String recordIdentifier) {
            this.nodeName = nodeName;
            this.nodeType = nodeType;
            this.recordIdentifier = recordIdentifier;
        }

        @ProcessElement
        public void processElement(@Element final GenericRecord record, final ProcessContext ctx) {
            String schemaId = record.getSchema().getFullName();
            counter(nodeName, nodeType, schemaId, ELEMENTS_IN).inc();
            Object recordKey = null;
            if (recordIdentifier != null) recordKey = AvroUtils.getFieldValue(record, recordIdentifier);
            if (recordKey == null) recordKey = UUID.randomUUID();
            String key = recordKey.toString();
            ctx.output(KV.of(key, record));
            counter(nodeName, nodeType, schemaId, ELEMENTS_OUT).inc();
        }
    }
}
