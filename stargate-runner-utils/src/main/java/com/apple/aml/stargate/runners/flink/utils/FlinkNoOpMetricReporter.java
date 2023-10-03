package com.apple.aml.stargate.runners.flink.utils;

import io.prometheus.client.exporter.HTTPServer;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import java.util.concurrent.atomic.AtomicReference;

import static com.apple.aml.stargate.rm.app.ResourceManager.startResourceManager;

public class FlinkNoOpMetricReporter implements MetricReporter, Scheduled {
    private static final AtomicReference<HTTPServer> METRIC_SERVER = new AtomicReference<>();

    @Override
    public void open(final MetricConfig config) {
        if (METRIC_SERVER.get() == null) {
            synchronized (FlinkMetricReporter.class) {
                startResourceManager();
            }
        }
    }

    @Override
    public void close() {
        HTTPServer server = METRIC_SERVER.get();
        if (server == null) {
            return;
        }
        try {
            HTTPServer.class.getMethod("close").invoke(server);
        } catch (Exception e) {

        }
    }

    @Override
    public void notifyOfAddedMetric(final Metric metric, final String metricName, final MetricGroup group) {

    }

    @Override
    public void notifyOfRemovedMetric(final Metric metric, final String metricName, final MetricGroup group) {

    }

    @Override
    public void report() {

    }
}
