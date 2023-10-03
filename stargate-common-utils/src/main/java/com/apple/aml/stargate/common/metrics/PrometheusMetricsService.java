package com.apple.aml.stargate.common.metrics;

import com.apple.aml.stargate.common.services.MetricsService;
import com.apple.aml.stargate.common.services.PrometheusService;
import com.apple.aml.stargate.common.utils.ContextHandler;
import com.apple.jvm.commons.metrics.Counter;
import com.apple.jvm.commons.metrics.DimensionalMetricsEngine;
import com.apple.jvm.commons.metrics.DoubleGauge;
import com.apple.jvm.commons.metrics.LongGauge;
import com.apple.jvm.commons.metrics.Timer;
import com.apple.jvm.commons.metrics.prometheus.PrometheusMetricsEngine;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;

import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.utils.PrometheusUtils.counterMetric;
import static com.apple.aml.stargate.common.utils.PrometheusUtils.gaugeMetric;
import static com.apple.aml.stargate.common.utils.PrometheusUtils.histogramMetric;

public class PrometheusMetricsService implements PrometheusService, MetricsService, DimensionalMetricsEngine {
    private static final String METRIC_PREFIX = "pipeline";
    private static final ConcurrentHashMap<String, Histogram> HISTOGRAMS = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, io.prometheus.client.Counter> COUNTERS = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, io.prometheus.client.Gauge> GAUGES = new ConcurrentHashMap<>();

    private PrometheusMetricsEngine engine;

    public PrometheusMetricsService init() {
        engine = new PrometheusMetricsEngine.Builder().build(CollectorRegistry.defaultRegistry);
        return this;
    }

    @Override
    public Timer newTimer(final String name) {
        return engine.newTimer(translatedName(name, "timer"));
    }

    String translatedName(final String givenName, final String suffix) {
        return String.format("%s_%s_%s", METRIC_PREFIX, givenName, suffix);
    }

    @Override
    public Counter newCounter(final String name) {
        return engine.newCounter(translatedName(name, "counter"));
    }

    @Override
    public DoubleGauge newDoubleGauge(final String name) {
        return engine.newDoubleGauge(translatedName(name, "gauge"));
    }

    @Override
    public LongGauge newLongGauge(final String name) {
        return engine.newLongGauge(translatedName(name, "gauge"));
    }

    @Override
    public Timer newTimer(final String name, final String... dimensions) {
        return engine.newTimer(translatedName(name, "timer"), dimensions);
    }

    @Override
    public Counter newCounter(final String name, final String... dimensions) {
        return engine.newCounter(translatedName(name, "counter"), dimensions);
    }

    @Override
    public DoubleGauge newDoubleGauge(final String name, final String... dimensions) {
        return engine.newDoubleGauge(translatedName(name, "gauge"), dimensions);
    }

    @Override
    public LongGauge newLongGauge(final String name, final String... dimensions) {
        return engine.newLongGauge(translatedName(name, "gauge"), dimensions);
    }

    @Override
    public Histogram newPipelineHistogram(final String name, final String... labelNames) {
        return HISTOGRAMS.computeIfAbsent(name, n -> histogramMetric(translatedName(name, "histogram"), null, getUpdatedLabelNames(labelNames)));
    }

    @Override
    public io.prometheus.client.Counter newPipelineCounter(final String name, final String... labelNames) {
        return COUNTERS.computeIfAbsent(name, n -> counterMetric(translatedName(name, "counter"), getUpdatedLabelNames(labelNames)));
    }

    @Override
    public Gauge newPipelineGauge(final String name, final String... labelNames) {
        return GAUGES.computeIfAbsent(name, n -> gaugeMetric(translatedName(name, "gauge"), getUpdatedLabelNames(labelNames)));
    }

    @Override
    public Histogram.Child pipelineHistogram(final String name, final String... labelValues) {
        try {
            return HISTOGRAMS.get(name).labels(getUpdatedLabelValues(labelValues));
        } catch (Exception e) {
            if (HISTOGRAMS.get(name) == null) {
                if (labelValues == null || labelValues.length == 0) {
                    newPipelineHistogram(name);
                    return HISTOGRAMS.get(name).labels(getUpdatedLabelValues(labelValues));
                }
                throw new RuntimeException("Missing histogram! Did you create/initialize histogram ?", e);
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public io.prometheus.client.Counter.Child pipelineCounter(final String name, final String... labelValues) {
        try {
            return COUNTERS.get(name).labels(getUpdatedLabelValues(labelValues));
        } catch (Exception e) {
            if (COUNTERS.get(name) == null) {
                if (labelValues == null || labelValues.length == 0) {
                    newPipelineCounter(name);
                    return COUNTERS.get(name).labels(getUpdatedLabelValues(labelValues));
                }
                throw new RuntimeException("Missing counter! Did you create/initialize counter ?", e);
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public Gauge.Child pipelineGauge(final String name, final String... labelValues) {
        try {
            return GAUGES.get(name).labels(getUpdatedLabelValues(labelValues));
        } catch (Exception e) {
            if (GAUGES.get(name) == null) {
                if (labelValues == null || labelValues.length == 0) {
                    newPipelineGauge(name);
                    return GAUGES.get(name).labels(getUpdatedLabelValues(labelValues));
                }
                throw new RuntimeException("Missing gauge! Did you create/initialize gauge ?", e);
            }
            throw new RuntimeException(e);
        }
    }

    private static String[] getUpdatedLabelValues(final String[] labelValues) {
        String[] updatedLabelValues = new String[1 + labelValues.length];
        ContextHandler.Context ctx = ContextHandler.ctx();
        updatedLabelValues[0] = (ctx == null || ctx.getNodeName() == null) ? UNKNOWN : ctx.getNodeName();
        if (labelValues.length > 0) System.arraycopy(labelValues, 0, updatedLabelValues, 1, labelValues.length);
        return updatedLabelValues;
    }

    private static String[] getUpdatedLabelNames(final String[] labelNames) {
        String[] updatedLabelNames = new String[1 + labelNames.length];
        updatedLabelNames[0] = NODE_NAME;
        if (labelNames.length > 0) System.arraycopy(labelNames, 0, updatedLabelNames, 1, labelNames.length);
        return updatedLabelNames;
    }
}
