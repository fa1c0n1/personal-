package com.apple.aml.stargate.pipeline.sdk.printers;

import com.apple.aml.stargate.common.options.BaseOptions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class BaseLogFns {
    protected static final Logger LOG = logger(MethodHandles.lookup().lookupClass());

    public static void log(final BaseOptions options, final String nodeName, final String nodeType, final KV<String, GenericRecord> kv) {
        log(options.logPayloadLevel(), nodeName, nodeType, kv);
    }

    public static void log(final Level level, final String nodeName, final String nodeType, final KV<String, GenericRecord> kv) {
        if (level == null) return;
        switch (level) {
            case INFO:
                logAsInfo(nodeName, nodeType, kv);
                break;
            case DEBUG:
                logAsDebug(nodeName, nodeType, kv);
                break;
            case ERROR:
                logAsError(nodeName, nodeType, kv);
                break;
            case WARN:
                logAsWarn(nodeName, nodeType, kv);
                break;
            case TRACE:
                logAsTrace(nodeName, nodeType, kv);
                break;
        }
    }

    public static void logAsError(final String nodeName, final String nodeType, final KV<String, GenericRecord> kv) {
        try {
            LOG.error(EMPTY_STRING, Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, "key", String.valueOf(kv.getKey()), "record", kv.getValue() == null ? EMPTY_STRING : kv.getValue()));
        } catch (Exception ignored) {
            LOG.error(String.valueOf(kv), Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType));
        }
    }

    public static void logAsDebug(final String nodeName, final String nodeType, final KV<String, GenericRecord> kv) {
        try {
            LOG.debug(EMPTY_STRING, Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, "key", String.valueOf(kv.getKey()), "record", kv.getValue() == null ? EMPTY_STRING : kv.getValue()));
        } catch (Exception ignored) {
            LOG.debug(String.valueOf(kv), Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType));
        }
    }

    public static void logAsWarn(final String nodeName, final String nodeType, final KV<String, GenericRecord> kv) {
        try {
            LOG.warn(EMPTY_STRING, Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, "key", String.valueOf(kv.getKey()), "record", kv.getValue() == null ? EMPTY_STRING : kv.getValue()));
        } catch (Exception ignored) {
            LOG.warn(String.valueOf(kv), Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType));
        }
    }

    public static void logAsInfo(final String nodeName, final String nodeType, final KV<String, GenericRecord> kv) {
        try {
            LOG.info(EMPTY_STRING, Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, "key", String.valueOf(kv.getKey()), "record", kv.getValue() == null ? EMPTY_STRING : kv.getValue()));
        } catch (Exception ignored) {
            LOG.info(String.valueOf(kv), Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType));
        }
    }

    public static void logAsTrace(final String nodeName, final String nodeType, final KV<String, GenericRecord> kv) {
        try {
            LOG.trace(EMPTY_STRING, Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, "key", String.valueOf(kv.getKey()), "record", kv.getValue() == null ? EMPTY_STRING : kv.getValue()));
        } catch (Exception ignored) {
            LOG.trace(String.valueOf(kv), Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType));
        }
    }
}
