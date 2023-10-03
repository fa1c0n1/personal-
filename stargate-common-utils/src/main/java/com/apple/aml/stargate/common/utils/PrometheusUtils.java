package com.apple.aml.stargate.common.utils;

import com.apple.aml.stargate.common.metrics.PrometheusMetricsService;
import com.apple.ihubble.client.stats.HubbleAgent;
import com.typesafe.config.Config;
import io.opencensus.exporter.stats.prometheus.PrometheusStatsCollector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.exporter.common.TextFormat;
import io.prometheus.client.hotspot.DefaultExports;
import io.prometheus.jmx.BuildInfoCollector;
import io.prometheus.jmx.JmxCollector;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.IDMS_APP_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.METRIC_DELIMITER;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricNames.APP_JVM_TIME;
import static com.apple.aml.stargate.common.utils.AppConfig.config;
import static com.apple.aml.stargate.common.utils.AppConfig.env;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.TemplateUtils.yyyyMMddHHmmss;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;

public final class PrometheusUtils {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final ConcurrentHashMap<String, Histogram> METRICS_HISTOGRAM = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Counter> METRICS_COUNTER = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Gauge> METRICS_GAUGE = new ConcurrentHashMap<>();
    private static boolean initializedMetrics;
    private static PrometheusMetricsService metricsService;

    private PrometheusUtils() {

    }

    public static StringBuilder metricSnapshot() throws IOException {
        FormatUtils.PrometheusMetricWriter writer = FormatUtils.prometheusMetricWriter();
        TextFormat.write004(writer, CollectorRegistry.defaultRegistry.metricFamilySamples());
        writer.close();
        return writer.buffer();
    }

    public static void startPrometheusServerThread(final AtomicReference<HTTPServer> reference) {
        Thread thread = new Thread(() -> {
            int port = AppConfig.config().getInt("stargate.prometheus.server.port");
            LOGGER.info("Starting Prometheus compatible Metric Server on port : {}", port);
            try {
                initializeMetrics();
                reference.set(new HTTPServer(new InetSocketAddress(port), CollectorRegistry.defaultRegistry, true));
            } catch (Exception e) {
                LOGGER.error("Could not start Prometheus compatible Metric Server", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            }
        });
        thread.setDaemon(true);
        thread.run();
    }

    public static synchronized void initializeMetrics() throws Exception {
        if (!initializedMetrics) {
            Config config = AppConfig.config();
            if (config.getBoolean("prometheus.metrics.buildinfo.enabled")) new BuildInfoCollector().register();
            if (config.getBoolean("prometheus.metrics.jmx.enabled")) new JmxCollector(EMPTY_STRING).register();
            if (config.getBoolean("prometheus.metrics.jvmdefaults.enabled")) DefaultExports.initialize();
            if (config.getBoolean("prometheus.metrics.opencensus.enabled")) PrometheusStatsCollector.createAndRegister();
            metricsService = new PrometheusMetricsService().init();
            Instant now = Instant.now();
            long milli = now.toEpochMilli();
            Gauge jvmTimeMetric = jvmTimeMetric();
            jvmTimeMetric.labels("init_time").set(milli);
            jvmTimeMetric.labels("init_time_formatted").set(Long.parseLong(yyyyMMddHHmmss(milli)));
            initializedMetrics = true;
        }
    }

    public static Gauge jvmTimeMetric() {
        return METRICS_GAUGE.computeIfAbsent(APP_JVM_TIME, name -> {
            Gauge.Builder builder = Gauge.build().name(APP_JVM_TIME).help("JVM Time").labelNames(TYPE);
            LOGGER.debug("Creating new gauge metric for JVM Time", Map.of("metricName", APP_JVM_TIME));
            return builder.register();
        });
    }

    public static PrometheusMetricsService metricsService() {
        if (metricsService != null) return metricsService;
        try {
            initializeMetrics();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return metricsService;
    }

    public static void publishToHubble(final String prefix) {
        publishToHubble(MetricUtils.getHubbleAgent(), CollectorRegistry.defaultRegistry, prefix, AppConfig.appId() + "");
    }

    public static void publishToHubble(final HubbleAgent agent, final CollectorRegistry registry, final String prefix, final String appId) {
        registry.metricFamilySamples().asIterator().forEachRemaining(metric -> {
            String metricName = prefix + metric.name.replace(":", METRIC_DELIMITER);
            switch (metric.type) {
                case COUNTER:
                    metric.samples.forEach(sample -> {
                        String sampleName = prefix + sample.name.replace(":", METRIC_DELIMITER);
                        long sampleValue = ((Double) sample.value).longValue();
                        int i = 0;
                        for (final String labelName : sample.labelNames) {
                            String labelValue = sample.labelValues.get(i++);
                            agent.captureCount(labelName, labelValue, sampleName, sampleValue);
                        }
                        agent.captureCount(IDMS_APP_ID, appId, metricName, sampleValue);
                        agent.captureCount(metricName, sampleValue);
                    });
                    break;
                case GAUGE:
                    metric.samples.forEach(sample -> {
                        String sampleName = prefix + sample.name.replace(":", METRIC_DELIMITER);
                        double sampleValue = sample.value;
                        int i = 0;
                        for (final String labelName : sample.labelNames) {
                            String labelValue = sample.labelValues.get(i++);
                            agent.captureGauge(labelName, labelValue, sampleName, sampleValue);
                        }
                        agent.captureGauge(IDMS_APP_ID, appId, metricName, sampleValue);
                        agent.captureGauge(metricName, sampleValue);
                    });
                    break;
                default:
                    metric.samples.forEach(sample -> {
                        String sampleName = prefix + sample.name.replace(":", METRIC_DELIMITER);
                        long sampleValue = ((Double) sample.value).longValue();
                        int i = 0;
                        for (final String labelName : sample.labelNames) {
                            String labelValue = sample.labelValues.get(i++);
                            agent.captureStat(labelName, labelValue, sampleName, sampleValue);
                        }
                        agent.captureStat(IDMS_APP_ID, appId, metricName, sampleValue);
                        agent.captureStat(metricName, sampleValue);
                    });
            }
        });
    }

    public static Histogram.Builder bucketBuilder(final String metricName, final Histogram.Builder inputBuilder) {
        return bucketBuilder(metricName, inputBuilder, null);
    }

    public static Histogram.Builder bucketBuilder(final String metricName, final Histogram.Builder inputBuilder, final String defaultStart) {
        Histogram.Builder builder = inputBuilder;
        builder = builder.name(metricName);
        Config config = config();
        String envVariable = metricName.toUpperCase();
        String type = env(String.format("APP_METRIC_BUCKET_TYPE_%s", envVariable), config.getString("prometheus.buckets.default.type"));
        double start;
        String startString = env(String.format("APP_METRIC_BUCKET_START_%s", envVariable), defaultStart);
        if (isBlank(startString)) {
            start = config.getDouble("prometheus.buckets.default.start");
        } else {
            start = parseDouble(startString);
        }
        int count;
        String countString = env(String.format("APP_METRIC_BUCKET_COUNT_%s", envVariable), null);
        if (isBlank(countString)) {
            count = config.getInt("prometheus.buckets.default.count");
        } else {
            count = parseInt(countString);
        }

        if ("linear".equalsIgnoreCase(type)) {
            double width;
            String widthString = env(String.format("APP_METRIC_BUCKET_WIDTH_%s", envVariable), null);
            if (isBlank(widthString)) {
                width = config.getDouble("prometheus.buckets.default.width");
            } else {
                width = parseDouble(widthString);
            }
            builder = builder.linearBuckets(start, width, count);
        } else {
            double factor;
            String factorString = env(String.format("APP_METRIC_BUCKET_FACTOR_%s", envVariable), null);
            if (isBlank(factorString)) {
                factor = config.getDouble("prometheus.buckets.default.factor");
            } else {
                factor = parseDouble(factorString);
            }
            builder = builder.exponentialBuckets(start, factor, count);
        }
        return builder;
    }

    public static Counter counterMetric(final String metricName, final String... labelNames) {
        return METRICS_COUNTER.computeIfAbsent(metricName, name -> {
            Counter.Builder builder = Counter.build().name(name.replaceAll("([ \\.\\\\\\t-]+)", METRIC_DELIMITER)).help(name).labelNames(labelNames);
            LOGGER.debug("Creating new counter metric", Map.of("metricName", metricName));
            return builder.register();
        });
    }

    public static Histogram histogramMetric(final String metricName, final String defaultStart, final String... labelNames) {
        return METRICS_HISTOGRAM.computeIfAbsent(metricName, name -> {
            Histogram.Builder builder = Histogram.build().help(name).labelNames(labelNames);
            builder = bucketBuilder(name.replaceAll("([ \\.\\\\\\t-]+)", METRIC_DELIMITER), builder, defaultStart);
            LOGGER.debug("Creating new histogram metric", Map.of("metricName", metricName));
            return builder.register();
        });
    }

    public static Gauge gaugeMetric(final String metricName, final String... labelNames) {
        return METRICS_GAUGE.computeIfAbsent(metricName, name -> {
            Gauge.Builder builder = Gauge.build().name(name.replaceAll("([ \\.\\\\\\t-]+)", METRIC_DELIMITER)).help(name).labelNames(labelNames);
            LOGGER.debug("Creating new gauge metric", Map.of("metricName", metricName));
            return builder.register();
        });
    }

    public static Histogram existingHistogram(final String metricName) {
        return METRICS_HISTOGRAM.get(metricName);
    }

}
