package com.apple.aml.stargate.common.utils;

import com.typesafe.config.Config;
import io.prometheus.client.Gauge;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.constants.CommonConstants.METRIC_DELIMITER;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.ThreadUtils.threadFactory;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class KafkaPrometheusMetricSyncer implements MetricsReporter {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    private static final HashSet<String> ALLOWED_METRICS = new HashSet<>();
    private static final AtomicReference<ScheduledExecutorService> SCHEDULER = new AtomicReference<>();
    private static final ConcurrentMap<String, Pair<Gauge, List<String>>> gaugeMap = new ConcurrentHashMap<>();
    private static final ConcurrentMap<MetricName, Triple<KafkaMetric, Gauge, String[]>> prometheusMap = new ConcurrentHashMap<>();

    @Override
    public void init(final List<KafkaMetric> metrics) {
        Config config = AppConfig.config();
        ALLOWED_METRICS.addAll(config.getStringList("stargate.kafka.prometheus.metrics.sync.list"));
        String additonal = config.getString("stargate.kafka.prometheus.metrics.sync.additional.list");
        if (!isBlank(additonal)) {
            for (String metricName : additonal.trim().split(",")) {
                ALLOWED_METRICS.add(metricName.trim());
            }
        }
        metrics.forEach(metric -> addPrometheusMetric(metric));
        if (SCHEDULER.get() != null || !AppConfig.config().getBoolean("prometheus.metrics.kafkaclient.enabled")) {
            return;
        }
        ScheduledExecutorService updateService = Executors.newSingleThreadScheduledExecutor(threadFactory("kafka-prometheus-sync-thread", true));
        SCHEDULER.set(updateService);
        long frequency = config.getLong("prometheus.kafka.metrics.sync.frequency");
        LOGGER.info("Configuring Kafka Prometheus Syncer now..");
        updateService.scheduleAtFixedRate(() -> {
            try {
                updatePrometheus();
            } catch (Exception e) {
                LOGGER.warn("Could not sync kafka metrics to prometheus", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            }
        }, frequency, frequency, TimeUnit.SECONDS);
        LOGGER.info("Kafka Prometheus syncer configured successfully with sync frequency of " + frequency + " seconds");
    }

    @Override
    public void metricChange(final KafkaMetric metric) {
        addPrometheusMetric(metric);
    }

    @Override
    public void metricRemoval(final KafkaMetric metric) {
        prometheusMap.remove(metric.metricName());
    }

    @Override
    public void close() {

    }

    private void addPrometheusMetric(final KafkaMetric metric) {
        final MetricName metricName = metric.metricName();
        String name = name(metricName);
        if (!ALLOWED_METRICS.contains(name)) {
            LOGGER.trace("Metric not present in allowed list. Will skip for prometheus sync", name);
            return;
        }
        Pair<Gauge, List<String>> gaugeDetails;
        try {
            gaugeDetails = gaugeMap.computeIfAbsent(name, x -> {
                String description = metricName.description() == null || metricName.description().trim().isBlank() ? name : metricName.description().trim();
                Gauge.Builder builder = Gauge.build().name(name).help(description);
                List<Pair<String, String>> tags = null;
                String[] labelNames = null;
                if (metricName.tags() != null && !metricName.tags().isEmpty()) {
                    tags = metricName.tags().keySet().stream().sorted().map(l -> Pair.of(l, l.replaceAll("([ \\.\\\\\\t-]+)", METRIC_DELIMITER))).collect(Collectors.toList());
                    labelNames = tags.stream().map(t -> t.getValue()).collect(Collectors.toList()).toArray(new String[tags.size()]);
                    builder.labelNames(labelNames);
                }
                LOGGER.debug("Creating new gauge metric", Map.of("metricName", name, "description", description, "labelNames", labelNames == null ? "null" : Arrays.stream(labelNames).collect(Collectors.joining(","))), tags);
                return Pair.of(builder.register(), tags == null ? null : tags.stream().map(t -> t.getKey()).collect(Collectors.toList()));
            });
        } catch (Exception e) {
            LOGGER.warn("Error registering metric", Map.of("metricName", metricName, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            gaugeDetails = gaugeMap.get(name);
        }
        if (gaugeDetails == null) {
            return;
        }
        final Pair<Gauge, List<String>> details = gaugeDetails;
        prometheusMap.computeIfAbsent(metricName, m -> {
            if (details.getRight() == null) {
                return Triple.of(metric, details.getKey(), null);
            }
            final Map<String, String> tags = metricName.tags();
            String[] labelValues = details.getRight().stream().map(l -> tags.getOrDefault(l, "default")).collect(Collectors.toList()).toArray(new String[details.getRight().size()]);
            return Triple.of(metric, details.getKey(), labelValues);
        });
    }

    private void updatePrometheus() {
        prometheusMap.values().parallelStream().forEach(p -> {
            KafkaMetric metric = p.getLeft();
            Object o = metric.metricValue();
            if (o instanceof String) {
                return;
            }
            Gauge gauge = p.getMiddle();
            String[] labels = p.getRight();
            try {
                double value = o instanceof Double ? (Double) o : (o instanceof Long ? ((Long) o).doubleValue() : (o instanceof Integer ? ((Integer) o).doubleValue() : Double.parseDouble(o.toString())));
                if (Double.isInfinite(value) || Double.isNaN(value)) {
                    return;
                }
                if (labels == null) {
                    gauge.set(value);
                } else {
                    gauge.labels(labels).set(value);
                }
            } catch (Exception e) {
                LOGGER.warn("Error in updating prometheus metric", Map.of("metric", metric.metricName().name(), "value", String.valueOf(o), "exception", e, ERROR_MESSAGE, String.valueOf(e.getMessage())));
            }
        });
    }

    private static String name(final MetricName metricName) {
        String name = metricName.name();
        return "kafka" + METRIC_DELIMITER + name.replaceAll("([ \\.\\\\\\t-]+)", METRIC_DELIMITER);
    }

    @Override
    public void configure(final Map<String, ?> configs) {

    }
}
