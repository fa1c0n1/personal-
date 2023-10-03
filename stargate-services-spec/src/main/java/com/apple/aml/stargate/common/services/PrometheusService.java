package com.apple.aml.stargate.common.services;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;

public interface PrometheusService extends MetricsService {
    Histogram newPipelineHistogram(final String name, final String... labelNames);

    Counter newPipelineCounter(final String name, final String... labelNames);

    Gauge newPipelineGauge(final String name, final String... labelNames);

    Histogram.Child pipelineHistogram(final String name, final String... labelValues);

    Counter.Child pipelineCounter(final String name, final String... labelValues);

    Gauge.Child pipelineGauge(final String name, final String... labelValues);

}
