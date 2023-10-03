package com.apple.aml.stargate.beam.sdk.metrics;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Histogram;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.util.HistogramData;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.common.constants.CommonConstants.METRIC_DELIMITER;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class PrometheusMetricsContainer implements MetricsContainer, Closeable {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final ConcurrentHashMap<MetricName, PCounter> COUNTER_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<MetricName, PDistribution> DIST_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<MetricName, PGauge> GAUGE_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<MetricName, PHistogram> HIST_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, io.prometheus.client.Counter> METRICS_COUNTER = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, io.prometheus.client.Summary> METRICS_SUMMARY = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, io.prometheus.client.Gauge> METRICS_GAUGE = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, io.prometheus.client.Histogram> METRICS_HISTOGRAM = new ConcurrentHashMap<>();


    private static String name(final MetricName metricName) {
        return metricName.getName().replaceAll("([ \\.\\\\\\t-:]+)", METRIC_DELIMITER).toLowerCase();
    }

    @Override
    public Counter getCounter(final MetricName metricName) {
        return COUNTER_MAP.computeIfAbsent(metricName, PCounter::new);
    }

    @Override
    public Distribution getDistribution(final MetricName metricName) {
        return DIST_MAP.computeIfAbsent(metricName, PDistribution::new);
    }

    @Override
    public Gauge getGauge(final MetricName metricName) {
        return GAUGE_MAP.computeIfAbsent(metricName, PGauge::new);
    }

    @Override
    public Histogram getHistogram(final MetricName metricName, final HistogramData.BucketType bucketType) {
        return HIST_MAP.computeIfAbsent(metricName, PHistogram::new);
    }

    @Override
    public void close() throws IOException {
        // don't do anything
    }

    public static class PCounter implements Counter {
        private final MetricName metricName;
        private final String nodeName;
        private final io.prometheus.client.Counter metric;

        public PCounter(final MetricName metricName) {
            this.metricName = metricName;
            this.nodeName = this.metricName.getNamespace();
            String name = String.format("counter%s%s", METRIC_DELIMITER, name(metricName));
            this.metric = METRICS_COUNTER.computeIfAbsent(name, n -> {
                LOGGER.debug("Creating new counter metric", Map.of(NODE_NAME, nodeName, "metricName", name, "beamMetric", metricName));
                return io.prometheus.client.Counter.build().name(name).help(metricName.toString()).labelNames(NODE_NAME).register();
            });
        }

        @Override
        public void inc() {
            metric.labels(nodeName).inc();
        }

        @Override
        public void inc(long n) {
            metric.labels(nodeName).inc(n);
        }

        @Override
        public void dec() {
            metric.labels(nodeName).inc(-1);
        }

        @Override
        public void dec(long n) {
            metric.labels(nodeName).inc(-1 * n);
        }

        @Override
        public MetricName getName() {
            return metricName;
        }

    }

    public static class PDistribution implements Distribution {
        private final MetricName metricName;
        private final String nodeName;
        private final io.prometheus.client.Summary metric;

        public PDistribution(final MetricName metricName) {
            this.metricName = metricName;
            this.nodeName = this.metricName.getNamespace();
            String name = String.format("distribution%s%s", METRIC_DELIMITER, name(metricName));
            this.metric = METRICS_SUMMARY.computeIfAbsent(name, n -> {
                LOGGER.debug("Creating new distribution metric", Map.of(NODE_NAME, nodeName, "metricName", name, "beamMetric", metricName));
                return io.prometheus.client.Summary.build().name(name).help(metricName.toString()).labelNames(NODE_NAME).register();
            });
        }

        @Override
        public void update(long value) {
            metric.labels(nodeName).observe(value);
        }

        @Override
        public void update(long sum, long count, long min, long max) {
            throw new UnsupportedOperationException("");
        }

        @Override
        public MetricName getName() {
            return metricName;
        }

    }

    public static class PGauge implements Gauge {
        private final MetricName metricName;
        private final String nodeName;
        private final io.prometheus.client.Gauge metric;

        public PGauge(final MetricName metricName) {
            this.metricName = metricName;
            this.nodeName = this.metricName.getNamespace();
            String name = String.format("gauge%s%s", METRIC_DELIMITER, name(metricName));
            this.metric = METRICS_GAUGE.computeIfAbsent(name, n -> {
                LOGGER.debug("Creating new gauge metric", Map.of(NODE_NAME, nodeName, "metricName", name, "beamMetric", metricName));
                return io.prometheus.client.Gauge.build().name(name).help(metricName.toString()).labelNames(NODE_NAME).register();
            });
        }

        @Override
        public void set(long value) {
            metric.labels(nodeName).set(value);
        }

        @Override
        public MetricName getName() {
            return metricName;
        }

    }

    public static class PHistogram implements Histogram {
        private final MetricName metricName;
        private final String nodeName;
        private final io.prometheus.client.Histogram metric;

        public PHistogram(final MetricName metricName) {
            this.metricName = metricName;
            this.nodeName = this.metricName.getNamespace();
            String name = String.format("histogram%s%s", METRIC_DELIMITER, name(metricName));
            this.metric = METRICS_HISTOGRAM.computeIfAbsent(name, n -> {
                LOGGER.debug("Creating new histogram metric", Map.of(NODE_NAME, nodeName, "metricName", name, "beamMetric", metricName));
                return io.prometheus.client.Histogram.build().name(name).help(metricName.toString()).labelNames(NODE_NAME).register();
            });
        }

        @Override
        public void update(double value) {
            metric.labels(nodeName).observe(value);
        }

        @Override
        public MetricName getName() {
            return metricName;
        }

    }
}
