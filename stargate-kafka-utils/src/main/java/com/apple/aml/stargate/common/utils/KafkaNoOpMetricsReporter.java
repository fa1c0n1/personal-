package com.apple.aml.stargate.common.utils;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import java.util.List;
import java.util.Map;

public class KafkaNoOpMetricsReporter implements MetricsReporter {
    @Override
    public void init(final List<KafkaMetric> metrics) {

    }

    @Override
    public void metricChange(final KafkaMetric metric) {

    }

    @Override
    public void metricRemoval(final KafkaMetric metric) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(final Map<String, ?> configs) {

    }
}
