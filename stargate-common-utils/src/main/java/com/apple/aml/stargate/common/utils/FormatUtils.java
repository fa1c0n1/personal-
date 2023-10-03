package com.apple.aml.stargate.common.utils;

import com.apple.aml.stargate.common.pojo.AvroRecord;
import com.apple.aml.stargate.common.utils.accessors.GenericRecordAccessor;
import com.apple.aml.stargate.common.utils.accessors.MapAccessor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.io.IOException;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.FAST_DATE_PARSERS;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.KEY;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.RECORD;
import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.SCHEMA;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_LATEST_VERSION;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public final class FormatUtils {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    private FormatUtils() {
    }

    public static <K, V> String formattedString(final Map<K, V> map) {
        if (map == null) {
            return null;
        }
        if (map.isEmpty()) {
            return "{}";
        }

        final StringBuilder sb = new StringBuilder(map.size() * 50);
        sb.append('{');
        boolean appendSeparator = false;
        for (Map.Entry<K, V> entry : map.entrySet()) {
            if (appendSeparator) {
                sb.append(", ");
            }
            sb.append(entry).append('=').append(entry.getValue());
            appendSeparator = true;
        }
        sb.append('}');
        return sb.toString();
    }

    public static Instant parseDateTime(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Instant) {
            return (Instant) value;
        }
        Instant dateTime = null;
        if (value instanceof Long) {
            try {
                dateTime = Instant.ofEpochMilli((Long) value);
                if (dateTime != null) {
                    return dateTime;
                }
            } catch (Exception ignored) {
                LOGGER.trace("Could not create instance of java.time.Instant using {} as epoc", value);
            }
        }
        for (FastDateFormat parser : FAST_DATE_PARSERS) {
            try {
                dateTime = Instant.ofEpochMilli(parser.parse(value.toString()).getTime());
                if (dateTime != null) {
                    return dateTime;
                }
            } catch (Exception ignored) {
                LOGGER.trace("Could not parse dateTime using format {}. Reason : {}", parser, ignored.getMessage());
            }
        }
        return dateTime;
    }

    public static PrometheusMetricWriter prometheusMetricWriter() {
        return new PrometheusMetricWriter(new StringBuilder());
    }

    public static String defaultName(final String key, final Schema schema, final GenericRecord record, final String name, final Expression expression) {
        String defaultName;
        if (name == null) {
            defaultName = schema.getName().replace('-', '_').toLowerCase();
        } else if (expression != null) {
            defaultName = evaluateSpel(key, schema, record, expression);
        } else {
            defaultName = name;
        }
        return defaultName;
    }

    public static String evaluateSpel(final String key, final Schema schema, final Object record, final Expression expression) {
        final Map<String, String> schemaMap = new HashMap<>();
        int i = 0;
        String fullName = schema.getFullName();
        for (final String token : fullName.split("\\.")) {
            schemaMap.put("token" + (i++), token);
        }
        schemaMap.put("name", schema.getName().replace('-', '_').toLowerCase());
        schemaMap.put("namespace", schema.getNamespace());
        schemaMap.put("fullName", fullName);
        int version = record instanceof AvroRecord ? ((AvroRecord) record).getSchemaVersion() : SCHEMA_LATEST_VERSION;
        schemaMap.put("version", version <= -1 ? "latest" : String.valueOf(version));
        Map<String, Object> object = Map.of(RECORD, record == null ? Map.of() : record, SCHEMA, schemaMap, KEY, String.valueOf(key));
        StandardEvaluationContext context = new StandardEvaluationContext(object);
        context.addPropertyAccessor(new GenericRecordAccessor());
        context.addPropertyAccessor(new MapAccessor());
        return expression.getValue(context, String.class);
    }

    public static class PrometheusMetricWriter extends Writer {
        private final StringBuilder buffer;

        public PrometheusMetricWriter(final StringBuilder buffer) {
            this.buffer = buffer;
        }

        @Override
        public void write(final char[] charArray, final int offset, final int length) throws IOException {
            buffer.append(new String(new String(charArray, offset, length).getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8));
        }

        @Override
        public void flush() throws IOException {

        }

        @Override
        public void close() throws IOException {

        }

        public StringBuilder buffer() {
            return this.buffer;
        }

        public String content() {
            return this.buffer.toString();
        }
    }
}
